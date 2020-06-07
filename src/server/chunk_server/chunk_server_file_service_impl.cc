#include "src/server/chunk_server/chunk_server_file_service_impl.h"

#include <future>
#include <utility>
#include <vector>

#include "grpcpp/grpcpp.h"
#include "src/common/protocol_client/chunk_server_service_server_client.h"
#include "src/common/system_logger.h"
#include "src/common/utils.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"
#include "src/server/chunk_server/chunk_data_cache_manager.h"
#include "src/server/chunk_server/file_chunk_manager.h"

using gfs::common::utils::ConvertProtobufStatusToGrpcStatus;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using StatusCode = google::protobuf::util::error::Code;
using gfs::server::ChunkDataCacheManager;
using gfs::service::ChunkServerServiceChunkServerClient;
using grpc::ServerContext;
using protos::grpc::AdvanceFileChunkVersionReply;
using protos::grpc::AdvanceFileChunkVersionRequest;
using protos::grpc::ApplyMutationsReply;
using protos::grpc::ApplyMutationsRequest;
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
  } else if (status.error_code() == StatusCode::ALREADY_EXISTS) {
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
    reply->set_data(data_or.ValueOrDie());
    reply->set_bytes_read(reply->data().length());
    LOG(INFO) << "Successfully read " << reply->bytes_read() << " bytes of "
              << request->chunk_handle();
    return grpc::Status::OK;
  } else if (data_or.status().error_code() == StatusCode::OUT_OF_RANGE) {
    // OUT OF RANGE
    LOG(ERROR) << "Cannot read file chunk because the requested offset is out"
               << " of range for " << request->chunk_handle() << ": "
               << data_or.status().ToString();
    reply->set_status(ReadFileChunkReply::FAILED_OUT_OF_RANGE);
    return grpc::Status::OK;
  } else if (data_or.status().error_code() == StatusCode::NOT_FOUND) {
    StatusOr<uint32_t> version_or =
        file_manager_->GetChunkVersion(request->chunk_handle());
    if (!version_or.ok()) {
      // NOT FOUND: file handle
      if (version_or.status().error_code() == StatusCode::NOT_FOUND) {
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
      LOG(ERROR) << "Chunk server has version " << version_or.ValueOrDie()
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

  // Get data from cache and try to make the local write
  grpc_status = WriteFileChunkInternal(request_header, reply);

  if (reply->status() != FileChunkMutationStatus::OK) {
    // Write failed
    return grpc_status;
  }

  // Write successful
  // Send to other replicas to apply this write
  LOG(INFO) << "Now sending apply mutation requests to "
            << request->replica_locations_size()
            << " replica(s) in parallel for file chunk: "
            << request_header.chunk_handle();

  // Send the requests in parallel
  std::vector<
      std::future<std::pair<std::string, StatusOr<ApplyMutationsReply>>>>
      apply_mutation_results;

  for (int replica = 0; replica < request->replica_locations_size();
       ++replica) {
    apply_mutation_results.push_back(
        std::async(std::launch::async, [&, replica]() {
          auto& replica_location = request->replica_locations(replica);

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

          auto apply_mutation_reply =
              client->SendRequest(apply_mutation_request);

          return std::pair<std::string, StatusOr<ApplyMutationsReply>>(
              server_address, apply_mutation_reply);
        }));
  }

  // Wait for the apply mutation replies
  for (int replica = 0; replica < request->replica_locations_size();
       ++replica) {
    LOG(INFO) << "Waiting for apply mutation reply for replica " << replica
              << " for file chunk: " << request_header.chunk_handle();

    auto apply_mutation_result = apply_mutation_results[replica].get();
    auto server_address = apply_mutation_result.first;
    auto apply_mutation_reply = apply_mutation_result.second;

    FileChunkMutationStatus apply_mutation_status;
    if (apply_mutation_reply.ok()) {
      apply_mutation_status = apply_mutation_reply.ValueOrDie().status();

      LOG(INFO) << "Received apply mutation status: " << apply_mutation_status
                << " for file chunk: " << request_header.chunk_handle()
                << " from chunk server: " << server_address;
    } else {
      LOG(ERROR) << "Apply mutation request to chunk server: " << server_address
                 << " for file chunk: " << request_header.chunk_handle()
                 << " failed. Status: " << apply_mutation_reply.status();

      apply_mutation_status = FileChunkMutationStatus::UNKNOWN;
    }

    // Add the result of this replica mutation to reply
    auto replica_status = reply->add_replica_status();
    *replica_status->mutable_chunk_server_location() =
        request->replica_locations(replica);
    replica_status->set_status(apply_mutation_status);
  }

  return grpc_status;
}

grpc::Status ChunkServerFileServiceImpl::AdvanceFileChunkVersion(
    ServerContext* context, const AdvanceFileChunkVersionRequest* request,
    AdvanceFileChunkVersionReply* reply) {
  LOG(INFO) << "Received AdvanceFileChunkVersion:" << (*request).DebugString();
  *reply->mutable_request() = *request;

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
  } else if (status.error_code() == StatusCode::NOT_FOUND) {
    StatusOr<uint32_t> version_or =
        file_manager_->GetChunkVersion(request->chunk_handle());
    if (!version_or.ok()) {
      // NOT FOUND: file handle
      if (version_or.status().error_code() == StatusCode::NOT_FOUND) {
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
      LOG(ERROR) << "Chunk server has version " << version_or.ValueOrDie()
                 << " but the request tries to update version from "
                 << from_version << " to " << request->new_chunk_version();
      reply->set_status(
          AdvanceFileChunkVersionReply::FAILED_VERSION_OUT_OF_SYNC);
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

  // Get data from cache and try to make the local write
  WriteFileChunkReply write_reply;
  auto status = WriteFileChunkInternal(request_header, &write_reply);

  reply->set_status(write_reply.status());
  return status;
}

grpc::Status ChunkServerFileServiceImpl::WriteFileChunkInternal(
    const protos::grpc::WriteFileChunkRequestHeader& request_header,
    protos::grpc::WriteFileChunkReply* const reply) {
  LOG(INFO) << "Checking data in cache for checksum: "
            << request_header.data_checksum().c_str()
            << ", Chunk handle: " << request_header.chunk_handle();

  auto cache_mgr = ChunkDataCacheManager::GetInstance();
  auto data_or = cache_mgr->GetValue(request_header.data_checksum());
  if (!data_or.ok()) {
    // Data not found, probably hasn't been sent
    LOG(ERROR) << "Data not found in cache for checksum: "
               << request_header.data_checksum().c_str()
               << ". Chunk handle: " << request_header.chunk_handle();
    reply->set_status(FileChunkMutationStatus::FAILED_DATA_NOT_FOUND);
    return grpc::Status::OK;
  }

  LOG(INFO) << "Data found in cache for checksum: "
            << request_header.data_checksum().c_str()
            << ". Chunk handle: " << request_header.chunk_handle()
            << ". Now writing data to file chunk.";

  // Do the actual disk write
  auto write_result = file_manager_->WriteToChunk(
      request_header.chunk_handle(), request_header.chunk_version(),
      request_header.offset_start(), request_header.length(),
      data_or.ValueOrDie());

  grpc::Status return_status = grpc::Status::OK;

  if (write_result.ok()) {
    // Write successful
    auto num_bytes_written = write_result.ValueOrDie();
    LOG(INFO) << "Write successful for file chunk: "
              << request_header.chunk_handle()
              << " Bytes written: " << num_bytes_written;

    reply->set_bytes_written(num_bytes_written);
    reply->set_status(FileChunkMutationStatus::OK);
  } else {
    // Write failed, lets see why it failed
    auto error_code = write_result.status().error_code();

    LOG(ERROR) << "Write failed for file chunk: "
               << request_header.chunk_handle()
               << ", Error code: " << error_code;

    if (error_code == StatusCode::NOT_FOUND) {
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
                   << " is stale. Current version: "
                   << version_result.ValueOrDie();
        reply->set_status(FileChunkMutationStatus::FAILED_STALE_VERSION);
      } else {
        // Get version failed, maybe chunk doesn't exist.
        // NOT FOUND: file handle
        if (version_result.status().error_code() == StatusCode::NOT_FOUND) {
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
    } else if (error_code == StatusCode::OUT_OF_RANGE) {
      LOG(ERROR) << "Failed to write file chunk because the write offset "
                 << request_header.offset_start()
                 << " is out of the allowed range. Status: "
                 << write_result.status();
      reply->set_status(FileChunkMutationStatus::FAILED_OUT_OF_RANGE);
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
    LOG(ERROR) << "Received chunk data with checksum "
               << request->checksum().c_str() << " and size "
               << request->data().size() / gfs::common::bytesPerMb
               << "MB is bigger than the max allowed size "
               << 64 /*fix*/ << "MB";

    reply->set_status(SendChunkDataReply::DATA_TOO_BIG);
    return grpc::Status::OK;
  }

  // Calculate checksum and compare with what was sent to make sure data is
  // still intact
  auto checksum = calc_checksum(request->data());

  if (checksum != request->checksum()) {
    LOG(ERROR) << "Received bad chunk data. Received checksum: "
               << request->checksum().c_str()
               << ", calculated checksum: " << checksum.c_str();

    reply->set_status(SendChunkDataReply::BAD_DATA);
    return grpc::Status::OK;
  }

  // Store the data temporarily in the cache
  ChunkDataCacheManager::GetInstance()->SetValue(request->checksum(),
                                                 request->data());

  LOG(INFO) << "Received chunk data with checksum "
            << request->checksum().c_str() << " and size "
            << request->data().size() / gfs::common::bytesPerMb
            << "MB has been temporarily stored in the cache";

  reply->set_status(SendChunkDataReply::OK);

  return grpc::Status::OK;
}

}  // namespace service
}  // namespace gfs
