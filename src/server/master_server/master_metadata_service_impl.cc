#include "src/server/master_server/master_metadata_service_impl.h"

#include "absl/time/time.h"
#include "grpcpp/grpcpp.h"
#include "src/common/protocol_client/grpc_client_utils.h"
#include "src/common/system_logger.h"
#include "src/common/utils.h"
#include "src/protos/chunk_server.pb.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"
#include "src/protos/metadata.pb.h"

using gfs::service::ChunkServerServiceMasterServerClient;
using google::protobuf::Empty;
using grpc::ServerContext;
using protos::ChunkServerLocation;
using protos::FileChunkMetadata;
using protos::grpc::AdvanceFileChunkVersionReply;
using protos::grpc::AdvanceFileChunkVersionRequest;
using protos::grpc::DeleteFileRequest;
using protos::grpc::GrantLeaseReply;
using protos::grpc::GrantLeaseRequest;
using protos::grpc::InitFileChunkReply;
using protos::grpc::InitFileChunkRequest;
using protos::grpc::OpenFileReply;
using protos::grpc::OpenFileRequest;

namespace gfs {
namespace service {

inline server::MetadataManager* MasterMetadataServiceImpl::metadata_manager() {
  return server::MetadataManager::GetInstance();
}

inline server::ChunkServerManager&
MasterMetadataServiceImpl ::chunk_server_manager() {
  return server::ChunkServerManager::GetInstance();
}

std::string MasterMetadataServiceImpl::ResolveServerAddress(
    const protos::ChunkServerLocation& location) {
  std::string server_name(location.server_hostname());
  if (resolve_hostname_) {
    server_name = config_manager_->ResolveHostname(server_name);
  }
  return server_name + ":" + std::to_string(location.server_port());
}

grpc::Status MasterMetadataServiceImpl::HandleFileChunkCreation(
    const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileReply* reply) {
  const std::string& filename(request->filename());
  const uint32_t chunk_index(request->chunk_index());
  if (!metadata_manager()->ExistFileMetadata(filename)) {
    LOG(ERROR) << "Cannot create file chunk index " << chunk_index << " for "
               << filename << " because the file doesn't exist";
    return grpc::Status(grpc::NOT_FOUND, "File doesn't exist");
  }

  // Step 1. Create the file chunk
  absl::StatusOr<std::string> chunk_handle_or(
      metadata_manager()->CreateChunkHandle(filename, chunk_index));
  if (!chunk_handle_or.ok()) {
    LOG(ERROR) << "Chunk handle creation failed: " << chunk_handle_or.status();
    return common::utils::ConvertProtobufStatusToGrpcStatus(
        chunk_handle_or.status());
  } else {
    LOG(INFO) << "Chunk handle created: " << chunk_handle_or.value()
              << " for file " << filename;
  }
  const std::string& chunk_handle(chunk_handle_or.value());

  // Step 2. Allocate chunk servers for this file chunk
  const ushort num_of_chunk_replica(config_manager_->GetReplicationFactor());
  auto chunk_server_locations(chunk_server_manager().AllocateChunkServer(
      chunk_handle, num_of_chunk_replica));

  // Record the chunk metadata; newly created chunks start at version 1
  FileChunkMetadata metadata;
  metadata.set_chunk_handle(chunk_handle);
  metadata.set_version(1);
  metadata_manager()->SetFileChunkMetadata(metadata);

  // Step 3. Coordinate with chunk servers to initialize the file chunk.
  // Following section 4.4 of the GFS paper, chunk creation is allowed to
  // succeed on only a subset of the allocated servers: replicas that could
  // not be initialized are simply not reported as locations, and any
  // partially created replicas are reconciled later through the regular
  // chunk report exchange (garbage collection), while re-replication
  // restores the replication goal in the background.
  for (auto chunk_server_location :
       chunk_server_manager().GetChunkLocations(chunk_handle)) {
    const std::string server_address(
        ResolveServerAddress(chunk_server_location));
    // Create and return this chunk server Rpc client if not exist
    auto chunk_server_service_client =
        GetOrCreateChunkServerProtocolClient(server_address);
    // Prepare InitFileChunk Request to send to chunk server
    InitFileChunkRequest init_chunk_request;
    init_chunk_request.set_chunk_handle(chunk_handle);
    grpc::ClientContext client_context;
    common::SetClientContextDeadline(client_context, config_manager_);

    // Issue InitFileChunk request and check status
    absl::StatusOr<InitFileChunkReply> init_chunk_or(
        chunk_server_service_client->SendRequest(init_chunk_request,
                                                 client_context));

    // If an InitFileChunk request failed, log it and move on to the other
    // replicas; the chunk stays usable as long as one replica initializes
    if (!init_chunk_or.ok()) {
      LOG(WARNING) << "InitFileChunkRequest for " << chunk_handle
                   << " sent to chunk server " << server_address
                   << " failed: " << init_chunk_or.status().message();
      continue;
    } else {
      LOG(INFO) << "InitFileChunkRequest for " << chunk_handle
                << " sent to chunk server " << server_address << " succeeded";
    }

    // For chunk creation, we don't need to select a primary location, since
    // we always require WRITES to talk to the master server again due to
    // chunk version advancement, so the client cache should (if done correctly)
    // always be refreshed and get primary location from the WRITE call

    // Record the location that successfully initialized the chunk
    *reply->mutable_metadata()->add_locations() = chunk_server_location;
  }

  reply->mutable_metadata()->set_chunk_handle(chunk_handle);
  reply->mutable_metadata()->set_version(1);

  if (reply->metadata().locations().empty()) {
    LOG(ERROR) << "No chunk server can initialize file chunk: " << chunk_handle;
    // Roll back the chunk handle so a retry of this creation can succeed
    metadata_manager()->DeleteChunkHandle(filename, chunk_index);
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "no chunk server is available");
  }

  return grpc::Status::OK;
}

grpc::Status MasterMetadataServiceImpl::HandleFileCreation(
    const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileReply* reply) {
  // Step 1. Create file metadata
  const std::string& filename(request->filename());
  LOG(INFO) << "MasterMetadataService handling file creation: " << filename;

  if (metadata_manager()->ExistFileMetadata(filename)) {
    LOG(ERROR) << "Skipped creating file as it already exists: " << filename;
    return grpc::Status(grpc::ALREADY_EXISTS, "File already exists in server");
  }

  absl::Status status(metadata_manager()->CreateFileMetadata(filename));
  if (!status.ok()) {
    LOG(ERROR) << "File metadata creation failed: " << status.message();
    return common::utils::ConvertProtobufStatusToGrpcStatus(status);
  } else {
    LOG(INFO) << "File metadata created for " << filename;
  }

  // Step 2. Create the first file chunk for this file and allocate chunk
  // servers
  grpc::Status chunk_creation_status(HandleFileChunkCreation(request, reply));

  // If we did not create chunk successfully during file creation, we roll back
  // and remove the file metadata and chunk metadata that got created along
  // the way.
  if (!chunk_creation_status.ok()) {
    LOG(ERROR) << "Rolling back and deleting file metadata: " << filename;
    metadata_manager()->DeleteFileAndChunkMetadata(filename);
  }

  return chunk_creation_status;
}

grpc::Status MasterMetadataServiceImpl::HandleFileChunkRead(
    const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileReply* reply) {
  // Step 1. Access the chunk_handle
  const std::string& filename(request->filename());
  const uint32_t chunk_index(request->chunk_index());
  LOG(INFO) << "MasterMetadataService handling file read: " << filename
            << " at chunk index " << chunk_index;

  if (!metadata_manager()->ExistFileMetadata(filename)) {
    LOG(ERROR) << "Cannot read file because it doesn't exist: " << filename;
    return grpc::Status(grpc::NOT_FOUND, "File doesn't exists in server");
  }

  absl::StatusOr<std::string> chunk_handle_or(
      metadata_manager()->GetChunkHandle(filename, chunk_index));

  if (!chunk_handle_or.ok()) {
    LOG(ERROR) << "Error encountered when accessing chunk handle : "
               << chunk_handle_or.status().message();
    return common::utils::ConvertProtobufStatusToGrpcStatus(
        chunk_handle_or.status());
  }

  // Step 2. Access the file chunk metadata
  const std::string& chunk_handle(chunk_handle_or.value());
  absl::StatusOr<FileChunkMetadata> file_chunk_metadata_or(
      metadata_manager()->GetFileChunkMetadata(chunk_handle));

  if (!file_chunk_metadata_or.ok()) {
    LOG(ERROR) << "File chunk metadata not accessible for " << chunk_handle;
    return common::utils::ConvertProtobufStatusToGrpcStatus(
        file_chunk_metadata_or.status());
  }

  // Set the file chunk metadata in reply
  FileChunkMetadata metadata = file_chunk_metadata_or.value();
  reply->mutable_metadata()->set_chunk_handle(chunk_handle);
  reply->mutable_metadata()->set_version(metadata.version());
  for (auto chunk_server_location :
       chunk_server_manager().GetChunkLocations(chunk_handle)) {
    *reply->mutable_metadata()->add_locations() = chunk_server_location;
  }
  if (reply->metadata().locations().empty()) {
    return grpc::Status(grpc::UNAVAILABLE,
                        "No chunk servers available right now for file read");
  } else {
    return grpc::Status::OK;
  }
}

grpc::Status MasterMetadataServiceImpl::HandleFileChunkWrite(
    const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileReply* reply) {
  // Step 1. Access the chunk handle, if the chunk handle does not exist
  // then create one for this write request. Note that we currently only
  // support this mode, i.e. create_if_not_exists is assumed to be true
  // for write request
  const std::string& filename(request->filename());
  const uint32_t chunk_index(request->chunk_index());
  LOG(INFO) << "MasterMetadataService handling file write: " << filename
            << " at chunk index " << chunk_index;

  if (!metadata_manager()->ExistFileMetadata(filename) &&
      !request->create_if_not_exists()) {
    LOG(ERROR) << "Cannot write file because it doesn't exist: " << filename;
    return grpc::Status(grpc::NOT_FOUND, "File doesn't exists in server");
  }

  absl::StatusOr<std::string> chunk_handle_or(
      metadata_manager()->GetChunkHandle(filename, chunk_index));

  if (!chunk_handle_or.ok()) {
    if (!request->create_if_not_exists()) {
      LOG(ERROR) << "create_if_not_exists not set when writing to file "
                 << filename << " at chunk index " << chunk_index
                 << " but chunk does not exist";
      return grpc::Status(grpc::StatusCode::NOT_FOUND,
                          "chunk does not exist and create_if_not_exists "
                          "not set");
    }

    LOG(INFO) << "Creating a file chunk for " << filename << " at chunk index "
              << chunk_index;
    grpc::Status chunk_creation_status(HandleFileChunkCreation(request, reply));
    if (!chunk_creation_status.ok()) {
      return chunk_creation_status;
    }
    // Refetch the chunk handle after creation, and clear the locations the
    // creation handler put into the reply: the write path below re-populates
    // them (otherwise every location would appear twice)
    chunk_handle_or = metadata_manager()->GetChunkHandle(filename, chunk_index);
    if (!chunk_handle_or.ok()) {
      return common::utils::ConvertProtobufStatusToGrpcStatus(
          chunk_handle_or.status());
    }
    reply->mutable_metadata()->clear_locations();
  }

  const std::string chunk_handle(chunk_handle_or.value());

  // The whole version-advance / lease-grant sequence below must be serialized
  // per chunk: two concurrent write-opens for the same chunk must not both
  // advance the version or grant different leases. The lock also makes
  // concurrent write-opens idempotent: the second one simply observes the
  // lease granted by the first and returns the same primary and version.
  absl::MutexLock lease_lock_guard(
      metadata_manager()->GetChunkLeaseLock(chunk_handle));

  absl::StatusOr<FileChunkMetadata> file_chunk_metadata_or(
      metadata_manager()->GetFileChunkMetadata(chunk_handle));
  if (!file_chunk_metadata_or.ok()) {
    LOG(ERROR) << "File chunk metadata not accessible for " << chunk_handle;
    return common::utils::ConvertProtobufStatusToGrpcStatus(
        file_chunk_metadata_or.status());
  }

  const uint32_t chunk_version = file_chunk_metadata_or.value().version();

  // The chunk servers currently known to hold this chunk
  auto live_locations(chunk_server_manager().GetChunkLocations(chunk_handle));

  // Step 2. If a still-valid lease exists and its holder is still a live
  // replica, reuse it: the version does not advance, and the client is told
  // the existing primary. Per section 3.1 of the GFS paper the master grants
  // a new lease only "if no one has a lease", and per section 4.5 the
  // version number advances only when a *new* lease is granted.
  auto lease_metadata(
      metadata_manager()->GetPrimaryLeaseMetadata(chunk_handle));
  if (lease_metadata.second) {
    const ChunkServerLocation lease_holder(lease_metadata.first.first);
    const uint64_t expiration_unix_sec(lease_metadata.first.second);
    const bool lease_valid =
        absl::FromUnixSeconds(expiration_unix_sec) > absl::Now();
    const bool holder_is_live = live_locations.contains(lease_holder);

    if (lease_valid && holder_is_live) {
      LOG(INFO) << "Reusing existing lease for " << chunk_handle << ", held by "
                << lease_holder.server_hostname() << " at version "
                << chunk_version;
      reply->mutable_metadata()->set_chunk_handle(chunk_handle);
      reply->mutable_metadata()->set_version(chunk_version);
      *reply->mutable_metadata()->mutable_primary_location() = lease_holder;
      reply->mutable_metadata()->clear_locations();
      for (auto location : live_locations) {
        *reply->mutable_metadata()->add_locations() = location;
      }
      return grpc::Status::OK;
    }

    // The lease expired, or its holder went away; discard it and grant a
    // fresh one below
    LOG(INFO) << "Existing lease for " << chunk_handle << " held by "
              << lease_holder.server_hostname()
              << (lease_valid ? " is no longer a live replica" : " has expired")
              << "; granting a new lease";
    metadata_manager()->RemovePrimaryLeaseMetadata(chunk_handle);
  }

  // Step 3. Grant a new lease. Per section 4.5 of the GFS paper, the master
  // first increases the chunk version number and informs the up-to-date
  // replicas, and both record the new version persistently before any client
  // is notified. Replicas that fail to advance (stale or unreachable) are
  // excluded from the locations returned to the client.
  const uint32_t new_chunk_version = chunk_version + 1;
  LOG(INFO) << "Advancing chunk version for chunk handle " << chunk_handle
            << " from " << chunk_version << " to " << new_chunk_version;

  std::vector<protos::ChunkServerLocation> advanced_locations;
  for (auto chunk_server_location : live_locations) {
    const std::string server_address(
        ResolveServerAddress(chunk_server_location));
    LOG(INFO) << "Issuing AdvanceFileChunkVersion request to " << server_address
              << " for chunk handle " << chunk_handle;
    auto chunk_server_service_client =
        GetOrCreateChunkServerProtocolClient(server_address);
    AdvanceFileChunkVersionRequest advance_version_request;
    advance_version_request.set_chunk_handle(chunk_handle);
    advance_version_request.set_new_chunk_version(new_chunk_version);
    grpc::ClientContext client_context;
    common::SetClientContextDeadline(client_context, config_manager_);

    absl::StatusOr<AdvanceFileChunkVersionReply> advance_version_reply_or(
        chunk_server_service_client->SendRequest(advance_version_request,
                                                 client_context));
    if (!advance_version_reply_or.ok()) {
      LOG(ERROR) << "Failed to advance chunk version for chunk " << chunk_handle
                 << " on chunk server " << server_address << " due to "
                 << advance_version_reply_or.status()
                 << "; excluding it from this write";
      continue;
    }

    const AdvanceFileChunkVersionReply& advance_reply(
        advance_version_reply_or.value());
    if (advance_reply.status() == AdvanceFileChunkVersionReply::OK) {
      LOG(INFO) << "Advanced chunk version for chunk " << chunk_handle
                << " on chunk server " << server_address;
      advanced_locations.push_back(chunk_server_location);
    } else if (advance_reply.status() ==
                   AdvanceFileChunkVersionReply::FAILED_VERSION_OUT_OF_SYNC &&
               advance_reply.chunk_version() == new_chunk_version) {
      // The replica is already at the target version. This happens when a
      // previous lease grant advanced the replicas but the master failed
      // before recording it; the replica is up to date (GFS paper
      // section 4.5)
      LOG(INFO) << "Chunk server " << server_address << " already has chunk "
                << chunk_handle << " at version " << new_chunk_version;
      advanced_locations.push_back(chunk_server_location);
    } else {
      LOG(ERROR) << "Chunk server " << server_address
                 << " could not advance chunk " << chunk_handle
                 << " to version " << new_chunk_version
                 << " (stale or missing replica); excluding it from this "
                 << "write. Reported version: "
                 << advance_reply.chunk_version();
    }
  }

  if (advanced_locations.empty()) {
    LOG(ERROR) << "No chunk servers were able to advance version for "
               << chunk_handle << " from " << chunk_version << " to "
               << new_chunk_version;
    return grpc::Status(grpc::UNAVAILABLE,
                        "Cannot advance versions on any chunk server, so no "
                        "write operations can be done. Abort.");
  }

  // Record the new version on the master (persistently, when the metadata
  // store is enabled) before notifying the client
  auto version_advance_status(
      metadata_manager()->AdvanceChunkVersion(chunk_handle));
  if (!version_advance_status.ok()) {
    LOG(ERROR) << "Failed to advance chunk version for chunk " << chunk_handle
               << " on the master due to " << version_advance_status;
    return common::utils::ConvertProtobufStatusToGrpcStatus(
        version_advance_status);
  }

  // Grant the lease to one of the up-to-date replicas; the first one that
  // accepts becomes the primary
  bool lease_granted = false;
  protos::ChunkServerLocation primary_location;
  for (auto& location : advanced_locations) {
    const std::string primary_server_address(ResolveServerAddress(location));
    auto lease_service_client(
        GetOrCreateChunkServerProtocolClient(primary_server_address));

    LOG(INFO) << "MasterMetadataService trying to grant write lease to server "
              << primary_server_address;
    GrantLeaseRequest grant_lease_request;
    grant_lease_request.set_chunk_handle(chunk_handle);
    grant_lease_request.set_chunk_version(new_chunk_version);
    const uint64_t expiration_unix_sec = absl::ToUnixSeconds(
        absl::Now() + config_manager_->GetWriteLeaseTimeout());
    grant_lease_request.mutable_lease_expiration_time()->set_seconds(
        expiration_unix_sec);
    grpc::ClientContext client_context;
    common::SetClientContextDeadline(client_context, config_manager_);

    absl::StatusOr<GrantLeaseReply> grant_lease_reply_or(
        lease_service_client->SendRequest(grant_lease_request, client_context));

    if (!grant_lease_reply_or.ok()) {
      LOG(ERROR) << "Grant lease request for chunk " << chunk_handle << " at "
                 << primary_server_address << " failed due to "
                 << grant_lease_reply_or.status();
      continue;
    }
    if (grant_lease_reply_or.value().status() != GrantLeaseReply::ACCEPTED) {
      // The chunk server refused the lease (e.g. it considers its replica
      // stale); do not treat it as the primary
      LOG(ERROR) << "Grant lease request for chunk " << chunk_handle << " at "
                 << primary_server_address
                 << " was not accepted: " << grant_lease_reply_or.value().status();
      continue;
    }

    LOG(INFO) << "Grant lease request for chunk " << chunk_handle << " at "
              << primary_server_address << " accepted";
    lease_granted = true;
    primary_location = location;
    metadata_manager()->SetPrimaryLeaseMetadata(chunk_handle, location,
                                                expiration_unix_sec);
    break;
  }

  if (!lease_granted) {
    return grpc::Status(grpc::UNAVAILABLE,
                        "No chunk server is available to accept the write "
                        "lease and act as the primary. Abort");
  }

  reply->mutable_metadata()->set_chunk_handle(chunk_handle);
  reply->mutable_metadata()->set_version(new_chunk_version);
  *reply->mutable_metadata()->mutable_primary_location() = primary_location;
  reply->mutable_metadata()->clear_locations();
  for (auto& location : advanced_locations) {
    *reply->mutable_metadata()->add_locations() = location;
  }

  return grpc::Status::OK;
}

std::shared_ptr<ChunkServerServiceMasterServerClient>
MasterMetadataServiceImpl::GetOrCreateChunkServerProtocolClient(
    const std::string& server_address) {
  if (chunk_server_service_clients_.contains(server_address)) {
    return chunk_server_service_clients_[server_address];
  } else {
    LOG(INFO) << "Estabalishing new connection to chunk server at: "
              << server_address;
    chunk_server_service_clients_[server_address] =
        std::make_shared<ChunkServerServiceMasterServerClient>(
            grpc::CreateChannel(server_address,
                                grpc::InsecureChannelCredentials()));
    return chunk_server_service_clients_[server_address];
  }
}

grpc::Status MasterMetadataServiceImpl::OpenFile(ServerContext* context,
                                                 const OpenFileRequest* request,
                                                 OpenFileReply* reply) {
  // Dispatch different mode to different handle function
  switch (request->mode()) {
    case OpenFileRequest::CREATE:
      return HandleFileCreation(request, reply);
    case OpenFileRequest::READ:
      return HandleFileChunkRead(request, reply);
    case OpenFileRequest::WRITE:
      return HandleFileChunkWrite(request, reply);
    default:
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid mode");
  }
}

// Handle a DeleteFileRequest request sent by the client.
grpc::Status MasterMetadataServiceImpl::DeleteFile(
    ServerContext* context, const DeleteFileRequest* request,
    google::protobuf::Empty* reply) {
  // Delete the file metadata, and all chunk metadata associated with a file
  // when processing a delete file request. Note that this action only deletes
  // the metadata; the garbage collection of the actual chunks happens through
  // the regular chunk report exchange between master and chunk servers (GFS
  // paper section 4.4).
  const std::string& filename(request->filename());
  LOG(INFO) << "Trying to delete file and chunk metadata associated with "
            << filename;
  metadata_manager()->DeleteFileAndChunkMetadata(filename);
  return grpc::Status::OK;
}

}  // namespace service
}  // namespace gfs
