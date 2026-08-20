#include "src/server/chunk_server/chunk_server_file_service_impl.h"

#include <future>
#include <utility>
#include <vector>

#include "grpcpp/grpcpp.h"
#include "src/common/protocol_client/chunk_server_service_server_client.h"
#include "src/common/protocol_client/grpc_client_utils.h"
#include "src/common/protos_common.h"
#include "src/common/system_logger.h"
#include "src/common/utils.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"
#include "src/server/chunk_server/chunk_data_cache_manager.h"
#include "src/server/chunk_server/file_chunk_manager.h"

using gfs::common::utils::ConvertProtobufStatusToGrpcStatus;
using gfs::server::ChunkDataCacheManager;
using gfs::service::ChunkServerServiceChunkServerClient;
using absl::IsAlreadyExists;
using absl::IsDataLoss;
using absl::IsNotFound;
using absl::IsOutOfRange;
using absl::Status;
using absl::StatusOr;
using grpc::ServerContext;
using protos::grpc::AdvanceFileChunkVersionReply;
using protos::grpc::AdvanceFileChunkVersionRequest;
using protos::grpc::ApplyMutationsReply;
using protos::grpc::ApplyMutationsRequest;
using protos::grpc::CloneFileChunkReply;
using protos::grpc::CloneFileChunkRequest;
using protos::grpc::FileChunkMutationStatus;
using protos::grpc::InitFileChunkReply;
using protos::grpc::InitFileChunkRequest;
using protos::grpc::ReadFileChunkReply;
using protos::grpc::ReadFileChunkRequest;
using protos::grpc::SendChunkDataReply;
using protos::grpc::SendChunkDataRequest;
using protos::grpc::WriteFileChunkReply;
using protos::grpc::WriteFileChunkRequest;

using namespace gfs::common::utils;

namespace gfs {
namespace service {

grpc::Status ChunkServerFileServiceImpl::InitFileChunk(
    ServerContext* context, const InitFileChunkRequest* request,
    InitFileChunkReply* reply) {
  LOG(INFO) << "Received InitFileChunkRequest:" << (*request).DebugString();
  *reply->mutable_request() = *request;

  LOG(INFO) << "Preparing redo/undo logs for InitFileChunk of file handle: "
            << request->chunk_handle();
  // TODO: may not be necessary depending on how crash-resistent we want to be
  LOG(INFO) << "InitFileChunk redo/undo logs prepared: "
            << request->chunk_handle();

  LOG(INFO) << "Trying to create the file chunk with initial version of 1: "
            << request->chunk_handle();
  Status status = file_manager_->CreateChunk(request->chunk_handle(), 1);
  if (status.ok()) {
    // SUCCESS
    LOG(INFO) << "Initial empty file chunk successfully created: "
              << request->chunk_handle();
    reply->set_status(InitFileChunkReply::CREATED);
    return grpc::Status::OK;
  } else if (IsAlreadyExists(status)) {
    // ALREADY EXISTS
    LOG(ERROR) << "Cannot initialize file chunk because it already exits: "
               << request->chunk_handle();
    reply->set_status(InitFileChunkReply::ALREADY_EXISTS);
    return grpc::Status::OK;
  } else {
    // INTERNAL ERROR
    LOG(ERROR) << "Unexpected error when initializing file chunk: " << status;
    return ConvertProtobufStatusToGrpcStatus(status);
  }
}

grpc::Status ChunkServerFileServiceImpl::ReadFileChunk(
    ServerContext* context, const ReadFileChunkRequest* request,
    ReadFileChunkReply* reply) {
  LOG(INFO) << "Received ReadFileChunkRequest:" << (*request).DebugString();
  *reply->mutable_request() = *request;

  // No redo/undo logs needed for reads, since there are no state changes

  LOG(INFO) << "Trying to read the file chunk " << request->chunk_handle()
            << " of version " << request->chunk_version() << " from offset "
            << request->offset_start() << " for " << request->length()
            << " bytes";

  // Read file chunk
  StatusOr<std::string> data_or = file_manager_->ReadFromChunk(
      request->chunk_handle(), request->chunk_version(),
      request->offset_start(), request->length());

  if (data_or.ok()) {
    // SUCCESS
    reply->set_status(ReadFileChunkReply::OK);
    reply->set_data(data_or.value());
    reply->set_bytes_read(reply->data().length());
    LOG(INFO) << "Successfully read " << reply->bytes_read() << " bytes of "
              << request->chunk_handle();
    return grpc::Status::OK;
  } else if (IsDataLoss(data_or.status())) {
    // DATA CORRUPTION: the stored data failed checksum verification; the
    // requester should read from another replica (GFS paper section 5.2).
    // Delete the corrupt replica so it stops being served: our next report
    // will no longer list it, the master drops this location, and the
    // repair scan clones a fresh replica from a valid one (the paper's
    // "the master ... instructs the chunkserver that reported the mismatch
    // to delete its replica")
    LOG(ERROR) << "Cannot read file chunk " << request->chunk_handle()
               << " because the stored data is corrupted: "
               << data_or.status().ToString();
    DeleteCorruptReplica(request->chunk_handle());
    reply->set_status(ReadFileChunkReply::FAILED_DATA_CORRUPTED);
    return grpc::Status::OK;
  } else if (IsOutOfRange(data_or.status())) {
    // OUT OF RANGE
    LOG(ERROR) << "Cannot read file chunk because the requested offset is out"
               << " of range for " << request->chunk_handle() << ": "
               << data_or.status().ToString();
    reply->set_status(ReadFileChunkReply::FAILED_OUT_OF_RANGE);
    return grpc::Status::OK;
  } else if (IsNotFound(data_or.status())) {
    StatusOr<uint32_t> version_or =
        file_manager_->GetChunkVersion(request->chunk_handle());
    if (!version_or.ok()) {
      // NOT FOUND: file handle
      if (IsNotFound(version_or.status())) {
        LOG(ERROR) << "Cannot read file chunk because it is not found: "
                   << request->chunk_handle();
        reply->set_status(ReadFileChunkReply::FAILED_NOT_FOUND);
        return grpc::Status::OK;
      } else {
        // INTERNAL ERROR
        LOG(ERROR) << "Unexpected error when reading the file chunk "
                   << request->chunk_handle()
                   << " of version: " << version_or.status();
        return ConvertProtobufStatusToGrpcStatus(version_or.status());
      }
    } else {
      // NOT FOUND: stale version
      LOG(ERROR) << "Cannot read file chunk because it is stale: "
                 << request->chunk_handle();
      LOG(ERROR) << "Chunk server has version " << version_or.value()
                 << " but a different version is requested: "
                 << request->chunk_version();
      reply->set_status(ReadFileChunkReply::FAILED_VERSION_OUT_OF_SYNC);
      return grpc::Status::OK;
    }
  } else {
    // INTERNAL ERROR
    LOG(ERROR) << "Unexpected error when initializing file chunk: "
               << data_or.status();
    return ConvertProtobufStatusToGrpcStatus(data_or.status());
  }
}

grpc::Status ChunkServerFileServiceImpl::WriteFileChunk(
    ServerContext* context, const WriteFileChunkRequest* request,
    WriteFileChunkReply* reply) {
  LOG(INFO) << "Received WriteFileChunkRequest: " << (*request).DebugString();

  *reply->mutable_request() = *request;

  grpc::Status grpc_status = grpc::Status::OK;

  auto& request_header = request->header();

  LOG(INFO) << "Checking if we have lease on chunk handle: "
            << request_header.chunk_handle();

  if (!chunk_server_impl_->HasWriteLease(request_header.chunk_handle())) {
    // Doesn't have write lease
    LOG(ERROR) << "Don't have write lease for chunk handle: "
               << request_header.chunk_handle();
    reply->set_status(FileChunkMutationStatus::FAILED_NOT_LEASE_HOLDER);
    return grpc_status;
  }

  LOG(INFO) << "There is a valid lease on chunk handle: "
            << request_header.chunk_handle();

  // We currently do one write at a time and send apply mutation for just this
  // write. The WriteFileChunkReply status is set as FileChunkMutationStatus::OK
  // if this internal write succeeds. Because it can fail at other replicas, we
  // add the status of each replica apply mutation to the replica_status.

  // The primary replica defines the mutation order for the chunk (GFS paper
  // section 3.1). We serialize concurrent mutations to the same chunk by
  // holding the chunk's mutation lock across both the local apply and the
  // forwarding to the secondary replicas: the order in which concurrent
  // writers acquire this lock *is* the serial order, and every secondary
  // receives the mutations one at a time in that same order.
  absl::MutexLock chunk_mutation_lock_guard(
      chunk_server_impl_->GetChunkMutationLock(request_header.chunk_handle()));

  // Get data from cache and try to make the local write
  grpc_status = WriteFileChunkInternal(request_header, reply);

  if (reply->status() != FileChunkMutationStatus::OK) {
    // Write failed
    return grpc_status;
  }

  // Write successful
  // Send to other replicas to apply this write. The client may include this
  // (primary) server in the replica list; skip ourselves since the mutation
  // has already been applied locally.
  const protos::ChunkServerLocation self_location =
      chunk_server_impl_->GetSelfLocation();
  std::vector<protos::ChunkServerLocation> secondary_locations;
  for (const auto& replica_location : request->replica_locations()) {
    if (replica_location == self_location) {
      continue;
    }
    secondary_locations.push_back(replica_location);
  }

  LOG(INFO) << "Now sending apply mutation requests to "
            << secondary_locations.size()
            << " secondary replica(s) in parallel for file chunk: "
            << request_header.chunk_handle();

  // Send the requests in parallel
  std::vector<
      std::future<std::pair<std::string, StatusOr<ApplyMutationsReply>>>>
      apply_mutation_results;

  for (size_t replica = 0; replica < secondary_locations.size(); ++replica) {
    apply_mutation_results.push_back(
        std::async(std::launch::async, [&, replica]() {
          auto& replica_location = secondary_locations[replica];

          std::string hostname = replica_location.server_hostname();
          if (chunk_server_impl_->ResolveHostname()) {
            hostname = chunk_server_impl_->GetConfigManager()->ResolveHostname(
                hostname);
          }

          auto server_address =
              absl::StrCat(hostname, ":", replica_location.server_port());

          auto client =
              chunk_server_impl_->GetChunkServerProtocolClient(server_address);

          ApplyMutationsRequest apply_mutation_request;
          *apply_mutation_request.add_headers() = request_header;

          LOG(INFO)
              << "Sending apply mutation request to replica chunk server: "
              << server_address
              << " for file chunk: " << request_header.chunk_handle();

          // Bound the wait: the chunk's mutation lock is held while
          // forwarding, so a hung secondary must not stall the chunk forever
          grpc::ClientContext client_context;
          common::SetClientContextDeadline(
              client_context, chunk_server_impl_->GetConfigManager());
          auto apply_mutation_reply =
              client->SendRequest(apply_mutation_request, client_context);

          return std::pair<std::string, StatusOr<ApplyMutationsReply>>(
              server_address, apply_mutation_reply);
        }));
  }

  // Wait for the apply mutation replies
  for (size_t replica = 0; replica < secondary_locations.size(); ++replica) {
    LOG(INFO) << "Waiting for apply mutation reply for replica " << replica
              << " for file chunk: " << request_header.chunk_handle();

    auto apply_mutation_result = apply_mutation_results[replica].get();
    auto server_address = apply_mutation_result.first;
    auto apply_mutation_reply = apply_mutation_result.second;

    FileChunkMutationStatus apply_mutation_status;
    if (apply_mutation_reply.ok()) {
      apply_mutation_status = apply_mutation_reply.value().status();

      LOG(INFO) << "Received apply mutation status: " << apply_mutation_status
                << " for file chunk: " << request_header.chunk_handle()
                << " from chunk server: " << server_address;
    } else {
      LOG(ERROR) << "Apply mutation request to chunk server: " << server_address
                 << " for file chunk: " << request_header.chunk_handle()
                 << " failed. Status: " << apply_mutation_reply.status();

      apply_mutation_status = FileChunkMutationStatus::UNKNOWN;
    }

    // Add the result of this replica mutation to reply, so the client can
    // detect secondary failures and retry (GFS paper section 3.1 step 7)
    auto replica_status = reply->add_replica_status();
    *replica_status->mutable_chunk_server_location() =
        secondary_locations[replica];
    replica_status->set_status(apply_mutation_status);
  }

  return grpc_status;
}

grpc::Status ChunkServerFileServiceImpl::AdvanceFileChunkVersion(
    ServerContext* context, const AdvanceFileChunkVersionRequest* request,
    AdvanceFileChunkVersionReply* reply) {
  LOG(INFO) << "Received AdvanceFileChunkVersion:" << (*request).DebugString();
  *reply->mutable_request() = *request;

  // Advancing the version rewrites the whole chunk record, so it must be
  // serialized with in-flight mutations to the same chunk — otherwise an
  // interleaved read-modify-write could resurrect pre-mutation data under
  // the new version, silently losing an acknowledged write
  absl::MutexLock chunk_mutation_lock_guard(
      chunk_server_impl_->GetChunkMutationLock(request->chunk_handle()));

  // Advancing to a version we already have is a success, not an error: it
  // happens when the master crashed after instructing replicas to advance
  // but before recording the new version, and retries the advance after
  // recovery (GFS paper section 4.5)
  StatusOr<uint32_t> current_version_or =
      file_manager_->GetChunkVersion(request->chunk_handle());
  if (current_version_or.ok() &&
      current_version_or.value() == request->new_chunk_version()) {
    LOG(INFO) << "File chunk " << request->chunk_handle()
              << " is already at version " << request->new_chunk_version();
    reply->set_status(AdvanceFileChunkVersionReply::OK);
    reply->set_chunk_version(request->new_chunk_version());
    return grpc::Status::OK;
  }

  // Per AdvanceFileVersion gRPC definition, we only advance version by one
  const uint32_t from_version = request->new_chunk_version() - 1;
  LOG(INFO) << "Trying to advance the version of file chunk "
            << request->chunk_handle() << " from version " << from_version
            << " to version " << request->new_chunk_version();
  Status status = file_manager_->UpdateChunkVersion(
      request->chunk_handle(), from_version, request->new_chunk_version());

  if (status.ok()) {
    // SUCCESS
    LOG(INFO) << "Successfully updated file chunk " << request->chunk_handle()
              << " to version " << request->new_chunk_version();
    reply->set_status(AdvanceFileChunkVersionReply::OK);
    reply->set_chunk_version(request->new_chunk_version());
    return grpc::Status::OK;
  } else if (IsNotFound(status)) {
    StatusOr<uint32_t> version_or =
        file_manager_->GetChunkVersion(request->chunk_handle());
    if (!version_or.ok()) {
      // NOT FOUND: file handle
      if (IsNotFound(version_or.status())) {
        LOG(ERROR)
            << "Cannot advance file chunk version because it is not found: "
            << request->chunk_handle();
        reply->set_status(AdvanceFileChunkVersionReply::FAILED_NOT_FOUND);
        return grpc::Status::OK;
      } else {
        // INTERNAL ERROR
        LOG(ERROR) << "Unexpected error when reading the file chunk "
                   << request->chunk_handle()
                   << " of version: " << version_or.status();
        return ConvertProtobufStatusToGrpcStatus(version_or.status());
      }
    } else {
      // NOT FOUND: stale version
      LOG(ERROR) << "Cannot advance file chunk, because version is out of sync "
                 << request->chunk_handle();
      LOG(ERROR) << "Chunk server has version " << version_or.value()
                 << " but the request tries to update version from "
                 << from_version << " to " << request->new_chunk_version();
      reply->set_status(
          AdvanceFileChunkVersionReply::FAILED_VERSION_OUT_OF_SYNC);
      // Report our actual version (per the proto contract), so the master
      // can tell a genuinely stale replica from other mismatches
      reply->set_chunk_version(version_or.value());
      return grpc::Status::OK;
    }
  } else {
    // INTERNAL ERROR
    LOG(ERROR) << "Unexpected error when advancing chunk version: " << status;
    return ConvertProtobufStatusToGrpcStatus(status);
  }
}

grpc::Status ChunkServerFileServiceImpl::ApplyMutations(
    grpc::ServerContext* context,
    const protos::grpc::ApplyMutationsRequest* request,
    protos::grpc::ApplyMutationsReply* reply) {
  LOG(INFO) << "Received ApplyMutationsRequest: " << (*request).DebugString();

  *reply->mutable_request() = *request;

  // Ideally we'll use the serialized order of mutations.
  // But we are currently just sending one apply mutation request.
  auto& request_header = request->headers(0);

  // Apply mutations to the same chunk one at a time. The primary forwards
  // mutations while holding its own mutation lock, so mutations arrive here
  // (and are applied) in the primary's serial order.
  absl::MutexLock chunk_mutation_lock_guard(
      chunk_server_impl_->GetChunkMutationLock(request_header.chunk_handle()));

  // Get data from cache and try to make the local write
  WriteFileChunkReply write_reply;
  auto status = WriteFileChunkInternal(request_header, &write_reply);

  reply->set_status(write_reply.status());
  return status;
}

grpc::Status ChunkServerFileServiceImpl::CloneFileChunk(
    grpc::ServerContext* context,
    const protos::grpc::CloneFileChunkRequest* request,
    protos::grpc::CloneFileChunkReply* reply) {
  LOG(INFO) << "Received CloneFileChunkRequest: " << (*request).DebugString();
  *reply->mutable_request() = *request;

  const std::string& chunk_handle = request->chunk_handle();
  const uint32_t chunk_version = request->chunk_version();

  // If we already hold this chunk at (or beyond) the requested version,
  // there is nothing to do
  StatusOr<uint32_t> local_version_or =
      file_manager_->GetChunkVersion(chunk_handle);
  if (local_version_or.ok() && local_version_or.value() >= chunk_version) {
    LOG(INFO) << "Chunk " << chunk_handle << " already stored at version "
              << local_version_or.value() << "; no clone needed";
    reply->set_status(CloneFileChunkReply::ALREADY_UP_TO_DATE);
    return grpc::Status::OK;
  }

  // Fetch the full chunk data from the source replica
  std::string source_hostname = request->source_location().server_hostname();
  if (chunk_server_impl_->ResolveHostname()) {
    source_hostname =
        chunk_server_impl_->GetConfigManager()->ResolveHostname(
            source_hostname);
  }
  const std::string source_address = absl::StrCat(
      source_hostname, ":", request->source_location().server_port());

  LOG(INFO) << "Cloning chunk " << chunk_handle << " of version "
            << chunk_version << " from replica at " << source_address;

  ReadFileChunkRequest read_request;
  read_request.set_chunk_handle(chunk_handle);
  read_request.set_chunk_version(chunk_version);
  read_request.set_offset_start(0);
  read_request.set_length(file_manager_->GetMaxChunkSizeBytes());

  // Bound the fetch: a hung source replica must not pin this gRPC worker
  // forever (the master retries the clone on a later scan)
  grpc::ClientContext read_context;
  common::SetClientContextDeadline(read_context,
                                   chunk_server_impl_->GetConfigManager());
  auto source_client =
      chunk_server_impl_->GetChunkServerProtocolClient(source_address);
  StatusOr<ReadFileChunkReply> read_reply_or =
      source_client->SendRequest(read_request, read_context);

  if (!read_reply_or.ok() ||
      read_reply_or.value().status() != ReadFileChunkReply::OK) {
    LOG(ERROR) << "Failed to fetch chunk " << chunk_handle
               << " from source replica " << source_address << " for cloning";
    reply->set_status(CloneFileChunkReply::FAILED_SOURCE_UNAVAILABLE);
    return grpc::Status::OK;
  }

  // Store the cloned replica locally, under the chunk's mutation lock so a
  // concurrent mutation cannot interleave with the store
  absl::MutexLock chunk_mutation_lock_guard(
      chunk_server_impl_->GetChunkMutationLock(chunk_handle));
  // Re-check under the lock: while this clone was fetching, the chunk may
  // have been created or advanced here (e.g. a duplicate clone from an
  // earlier scan, or this server re-registered and received writes) — a
  // stale store would roll back committed mutations
  local_version_or = file_manager_->GetChunkVersion(chunk_handle);
  if (local_version_or.ok() && local_version_or.value() >= chunk_version) {
    LOG(INFO) << "Chunk " << chunk_handle << " reached version "
              << local_version_or.value()
              << " while cloning; discarding the fetched copy";
    reply->set_status(CloneFileChunkReply::ALREADY_UP_TO_DATE);
    return grpc::Status::OK;
  }
  auto store_status = file_manager_->StoreChunkData(
      chunk_handle, chunk_version, read_reply_or.value().data());
  if (!store_status.ok()) {
    LOG(ERROR) << "Failed to store cloned chunk " << chunk_handle << ": "
               << store_status.ToString();
    reply->set_status(CloneFileChunkReply::FAILED_STORE_ERROR);
    return grpc::Status::OK;
  }

  LOG(INFO) << "Successfully cloned chunk " << chunk_handle << " of version "
            << chunk_version << " (" << read_reply_or.value().bytes_read()
            << " bytes) from " << source_address;
  reply->set_status(CloneFileChunkReply::OK);
  return grpc::Status::OK;
}

void ChunkServerFileServiceImpl::DeleteCorruptReplica(
    const std::string& chunk_handle) {
  absl::MutexLock chunk_mutation_lock_guard(
      chunk_server_impl_->GetChunkMutationLock(chunk_handle));
  auto delete_status = file_manager_->DeleteChunk(chunk_handle);
  LOG(ERROR) << "Deleted corrupt replica of " << chunk_handle
             << " for later re-clone: " << delete_status.ToString();
}

grpc::Status ChunkServerFileServiceImpl::WriteFileChunkInternal(
    const protos::grpc::WriteFileChunkRequestHeader& request_header,
    protos::grpc::WriteFileChunkReply* const reply) {
  LOG(INFO) << "Checking data in cache for checksum: "
            << request_header.data_checksum()
            << ", Chunk handle: " << request_header.chunk_handle();

  auto cache_mgr = ChunkDataCacheManager::GetInstance();
  auto data_or = cache_mgr->GetValue(request_header.data_checksum());
  if (!data_or.ok()) {
    // Data not found, probably hasn't been sent
    LOG(ERROR) << "Data not found in cache for checksum: "
               << request_header.data_checksum()
               << ". Chunk handle: " << request_header.chunk_handle();
    reply->set_status(FileChunkMutationStatus::FAILED_DATA_NOT_FOUND);
    return grpc::Status::OK;
  }

  LOG(INFO) << "Data found in cache for checksum: "
            << request_header.data_checksum()
            << ". Chunk handle: " << request_header.chunk_handle()
            << ". Now writing data to file chunk.";

  // Do the actual disk write
  auto write_result = file_manager_->WriteToChunk(
      request_header.chunk_handle(), request_header.chunk_version(),
      request_header.offset_start(), request_header.length(), data_or.value());

  grpc::Status return_status = grpc::Status::OK;

  if (write_result.ok()) {
    // Write successful
    auto num_bytes_written = write_result.value();
    LOG(INFO) << "Write successful for file chunk: "
              << request_header.chunk_handle()
              << " Bytes written: " << num_bytes_written;

    reply->set_bytes_written(num_bytes_written);
    reply->set_status(FileChunkMutationStatus::OK);
  } else {
    // Write failed, lets see why it failed
    auto status = write_result.status();

    LOG(ERROR) << "Write failed for file chunk: "
               << request_header.chunk_handle()
               << ", Error code: " << status;

    if (IsNotFound(status)) {
      // See why it wasn't found
      // Get the current chunk version
      StatusOr<uint32_t> version_result =
          file_manager_->GetChunkVersion(request_header.chunk_handle());

      if (version_result.ok()) {
        // NOT FOUND: stale version
        LOG(ERROR) << "Cannot write file chunk "
                   << request_header.chunk_handle()
                   << " because the requested version: "
                   << request_header.chunk_version()
                   << " is stale. Current version: " << version_result.value();
        reply->set_status(FileChunkMutationStatus::FAILED_STALE_VERSION);
      } else {
        // Get version failed, maybe chunk doesn't exist.
        // NOT FOUND: file handle
        if (IsNotFound(version_result.status())) {
          LOG(ERROR) << "Cannot write to file chunk because it is not found: "
                     << request_header.chunk_handle();
          reply->set_status(FileChunkMutationStatus::FAILED_DATA_NOT_FOUND);
        } else {
          // INTERNAL ERROR
          LOG(ERROR)
              << "Unexpected error when checking the current write version for "
                 "file chunk: "
              << request_header.chunk_handle()
              << ". Status: " << version_result.status();
          return_status =
              ConvertProtobufStatusToGrpcStatus(version_result.status());
        }
      }
    } else if (IsOutOfRange(status)) {
      LOG(ERROR) << "Failed to write file chunk because the write offset "
                 << request_header.offset_start()
                 << " is out of the allowed range. Status: "
                 << write_result.status();
      reply->set_status(FileChunkMutationStatus::FAILED_OUT_OF_RANGE);
    } else if (IsDataLoss(status)) {
      // Callers of WriteFileChunkInternal hold this chunk's mutation lock,
      // so the corrupt replica can be deleted directly; see ReadFileChunk's
      // corruption branch for how deletion leads to repair
      LOG(ERROR) << "Failed to write file chunk because existing data "
                 << "around the written range is corrupted. Status: "
                 << write_result.status();
      auto delete_status =
          file_manager_->DeleteChunk(request_header.chunk_handle());
      LOG(ERROR) << "Deleted corrupt replica of "
                 << request_header.chunk_handle() << " for later re-clone: "
                 << delete_status.ToString();
      reply->set_status(FileChunkMutationStatus::FAILED_DATA_CORRUPTED);
    } else {
      // INTERNAL ERROR
      LOG(ERROR) << "Unexpected error while writing file chunk: "
                 << request_header.chunk_handle()
                 << ". Status: " << write_result.status();
      reply->set_status(FileChunkMutationStatus::UNKNOWN);
      return_status = ConvertProtobufStatusToGrpcStatus(write_result.status());
    }
  }

  // Remove the data from cache
  if (clear_cached_data_after_write_) {
    cache_mgr->RemoveValue(request_header.data_checksum());
  }

  return return_status;
}

grpc::Status ChunkServerFileServiceImpl::SendChunkData(
    grpc::ServerContext* context,
    const protos::grpc::SendChunkDataRequest* request,
    protos::grpc::SendChunkDataReply* reply) {
  *reply->mutable_request() = *request;

  // Is the data size greater than the allowed chunk size
  if (request->data().size() >
      chunk_server_impl_->GetConfigManager()->GetFileChunkBlockSize() *
          gfs::common::bytesPerMb) {
    LOG(ERROR) << "Received chunk data with checksum " << request->checksum()
               << " and size "
               << request->data().size() / gfs::common::bytesPerMb
               << "MB is bigger than the max allowed size "
               << chunk_server_impl_->GetConfigManager()
                      ->GetFileChunkBlockSize()
               << "MB";

    reply->set_status(SendChunkDataReply::DATA_TOO_BIG);
    return grpc::Status::OK;
  }

  // Calculate checksum and compare with what was sent to make sure data is
  // still intact
  auto checksum = calc_checksum(request->data());

  if (checksum != request->checksum()) {
    LOG(ERROR) << "Received bad chunk data. Received checksum: "
               << request->checksum() << ", calculated checksum: " << checksum;

    reply->set_status(SendChunkDataReply::BAD_DATA);
    return grpc::Status::OK;
  }

  // Store the data temporarily in the cache
  ChunkDataCacheManager::GetInstance()->SetValue(request->checksum(),
                                                 request->data());

  LOG(INFO) << "Received chunk data with checksum " << request->checksum()
            << " and size " << request->data().size() / gfs::common::bytesPerMb
            << "MB has been temporarily stored in the cache";

  reply->set_status(SendChunkDataReply::OK);

  return grpc::Status::OK;
}

}  // namespace service
}  // namespace gfs
