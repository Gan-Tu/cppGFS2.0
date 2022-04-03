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
  // TODO(Xi): undo the file handle creation if any step of the remaining fails,
  // otherwise future create will fail but no file allocations exist
  google::protobuf::util::StatusOr<std::string> chunk_handle_or(
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
  const ushort num_of_chunk_replica(3);
  // TODO(someone): make this number configurable via config.yml. Probably not
  // a priority right now
  auto chunk_server_locations(chunk_server_manager().AllocateChunkServer(
      chunk_handle, num_of_chunk_replica));

  // Prepare the OpenFileReply
  FileChunkMetadata metadata;
  metadata.set_chunk_handle(chunk_handle);
  metadata.set_version(1);  // 1 is the initialized version
  metadata_manager()->SetFileChunkMetadata(metadata);

  reply->mutable_metadata()->set_chunk_handle(chunk_handle);
  reply->mutable_metadata()->set_version(1);

  // Step 3. Coordinate with chunk servers to initialize the file chunk
  for (auto chunk_server_location :
       chunk_server_manager().GetChunkLocations(chunk_handle)) {
    std::string server_name(chunk_server_location.server_hostname());
    if (resolve_hostname_) {
      server_name = config_manager_->ResolveHostname(server_name);
    }
    const std::string server_address(
        server_name + ":" +
        std::to_string(chunk_server_location.server_port()));
    // Create and return this chunk server Rpc client if not exist
    auto chunk_server_service_client =
        GetOrCreateChunkServerProtocolClient(server_address);
    // Prepare InitFileChunk Request to send to chunk server
    InitFileChunkRequest init_chunk_request;
    init_chunk_request.set_chunk_handle(chunk_handle);
    grpc::ClientContext client_context;
    common::SetClientContextDeadline(client_context, config_manager_);

    // Issue InitFileChunk request and check status
    google::protobuf::util::StatusOr<InitFileChunkReply> init_chunk_or(
        chunk_server_service_client->SendRequest(init_chunk_request,
                                                 client_context));

    // If an InitFileChunk request failed, we log it with a warning and
    // keep going
    if (!init_chunk_or.ok()) {
      LOG(WARNING) << "InitFileChunkRequest for " << chunk_handle
                   << " sent to chunk server " << server_address
                   << " failed: " << init_chunk_or.status().message();
      // TODO(xi): should undo file creation and file allocations
      return common::utils::ConvertProtobufStatusToGrpcStatus(
          init_chunk_or.status());
    } else {
      LOG(INFO) << "InitFileChunkRequest for " << chunk_handle
                << " sent to chunk server " << server_address << " succeeded";
    }

    // For chunk creation, we don't need to select a primary location, since
    // we always require WRITES to talk to the master server again due to
    // chunk version advancement, so the client cache should (if done correctly)
    // always be refreshed and get primary location from the WRITE call

    // Prepare the InitFileChunk reply with the chunk metadata
    *reply->mutable_metadata()->add_locations() = chunk_server_location;
  }

  if (reply->metadata().locations().empty()) {
    LOG(ERROR) << "No chunk servers are available for allocation.";
    LOG(ERROR) << "No file chunk can be initialized for: " << chunk_handle;
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

  google::protobuf::util::Status status(
      metadata_manager()->CreateFileMetadata(filename));
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
    LOG(ERROR) << "Cannot read file because it doesn't exist" << filename;
    return grpc::Status(grpc::NOT_FOUND, "File doesn't exists in server");
  }

  google::protobuf::util::StatusOr<std::string> chunk_handle_or(
      metadata_manager()->GetChunkHandle(filename, chunk_index));

  if (!chunk_handle_or.ok()) {
    LOG(ERROR) << "Error encountered when accessing chunk handle : "
               << chunk_handle_or.status().message();
    return common::utils::ConvertProtobufStatusToGrpcStatus(
        chunk_handle_or.status());
  }

  // Step 2. Access the file chunk metadata
  const std::string& chunk_handle(chunk_handle_or.value());
  google::protobuf::util::StatusOr<FileChunkMetadata> file_chunk_metadata_or(
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
    LOG(ERROR) << "Cannot read file because it doesn't exist" << filename;
    return grpc::Status(grpc::NOT_FOUND, "File doesn't exists in server");
  }

  google::protobuf::util::StatusOr<std::string> chunk_handle_or(
      metadata_manager()->GetChunkHandle(filename, chunk_index));

  if (!chunk_handle_or.ok()) {
    if (!request->create_if_not_exists()) {
      LOG(ERROR) << "create_if_not_exists not set when writing to file "
                 << filename << " at chunk index " << chunk_index
                 << " but chunk does not exist";
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "create_if_not_exists not set");
    } else if (chunk_index != 0) {
      // TODO(xi): technically, it should fail otherwise we will have holes
      // but Xi implemented this to allow writes beyond the file using this
      // logic for simplicity, so fine.
    }

    LOG(INFO) << "Creating a file chunk for " << filename << " at chunk index "
              << chunk_index;
    grpc::Status chunk_creation_status(HandleFileChunkCreation(request, reply));
    if (!chunk_creation_status.ok()) {
      return chunk_creation_status;
    }
    // Refetch the chunk handle after creation
    chunk_handle_or = metadata_manager()->GetChunkHandle(filename, chunk_index);
  }

  google::protobuf::util::StatusOr<FileChunkMetadata> file_chunk_metadata_or(
      metadata_manager()->GetFileChunkMetadata(chunk_handle_or.value()));
  if (!file_chunk_metadata_or.ok()) {
    LOG(ERROR) << "File chunk metadata not accessible for "
               << chunk_handle_or.value();
    return common::utils::ConvertProtobufStatusToGrpcStatus(
        file_chunk_metadata_or.status());
  }

  FileChunkMetadata metadata = file_chunk_metadata_or.value();
  const std::string& chunk_handle(metadata.chunk_handle());
  uint32_t chunk_version = metadata.version();
  uint32_t new_chunk_version = chunk_version + 1;

  LOG(INFO) << "Advancing chunk version for chunk handle " << chunk_handle
            << " from " << chunk_version << " to " << new_chunk_version;

  // Now that all chunk server locations are stored in reply, the master first
  // advances the chunk version of this chunk and then send a GrantLeaseRequest
  std::vector<protos::ChunkServerLocation> advanced_locations = {};
  for (auto chunk_server_location :
       chunk_server_manager().GetChunkLocations(chunk_handle)) {
    std::string chunk_server_hostname = chunk_server_location.server_hostname();
    if (resolve_hostname_) {
      chunk_server_hostname =
          config_manager_->ResolveHostname(chunk_server_hostname);
    }
    const std::string server_address(
        chunk_server_hostname + ":" +
        std::to_string(chunk_server_location.server_port()));
    LOG(INFO) << "Issuing AdvanceFileChunkVersion request to " << server_address
              << " for chunk handle " << chunk_handle;
    // Create and return this chunk server Rpc client if not exist
    auto chunk_server_service_client =
        GetOrCreateChunkServerProtocolClient(server_address);
    // Issue AdvanceFileChunkVersion request
    AdvanceFileChunkVersionRequest advance_version_request;
    advance_version_request.set_chunk_handle(chunk_handle);
    // Advance the chunk version by 1
    advance_version_request.set_new_chunk_version(new_chunk_version);
    grpc::ClientContext client_context;
    common::SetClientContextDeadline(client_context, config_manager_);

    // Issue AdvanceFileChunkVersion and check status
    google::protobuf::util::StatusOr<AdvanceFileChunkVersionReply>
        advance_version_reply_or(chunk_server_service_client->SendRequest(
            advance_version_request, client_context));
    if (!advance_version_reply_or.ok()) {
      LOG(ERROR) << "Failed to advance chunk version for chunk " << chunk_handle
                 << " on chunk server " << server_address << "due to "
                 << advance_version_reply_or.status();
      LOG(INFO) << "Skipping chunk server "
                << chunk_server_location.server_hostname()
                << " for write operation " << chunk_handle;
      // TODO(Xi): handle if all version advancement fails
      // TODO(tugan): the client shouldn't write to this chunk server anymore
      // TODO(tugan): should issue request to bring the replica up to date
    } else {
      LOG(INFO) << "Advanced chunk version for chunk " << chunk_handle
                << " on chunk server " << server_address;
      advanced_locations.push_back(chunk_server_location);
    }
  }

  if (advanced_locations.empty()) {
    LOG(ERROR) << "No chunk servers were able to advance version for "
               << chunk_handle << " from " << chunk_version << " to "
               << new_chunk_version;
    return grpc::Status(grpc::UNAVAILABLE,
                        "Cannot advance versions on all chunk servers, so no "
                        "write operations can be done. Abort.");
  }

  // This should happen after at least one chunk server is advanced so master
  // doesn't become out of sync with all others
  auto version_advance_status(
      metadata_manager()->AdvanceChunkVersion(chunk_handle));
  if (!version_advance_status.ok()) {
    LOG(ERROR) << "Failed to advance chunk version for chunk " << chunk_handle
               << " on the master due to " << version_advance_status;
    return common::utils::ConvertProtobufStatusToGrpcStatus(
        version_advance_status);
  } else {
    LOG(INFO) << "Advanced chunk version for chunk " << chunk_handle
              << " on the master server ";
  }

  // After advancing the chunk version, obtain a lease
  // We do so by trying to grant lease to one server at a time, and stop
  // at the first one that accepted the lease
  bool lease_granted = false;
  protos::ChunkServerLocation primary_location;

  LOG(INFO) << "Checking if we have existing lease for " << chunk_handle;
  auto result_or = metadata_manager()->GetPrimaryLeaseMetadata(chunk_handle);
  if (result_or.second) {  // has value
    auto lease_location_expiration_time = result_or.first;
    protos::ChunkServerLocation prev_lease_holder_location =
        lease_location_expiration_time.first;
    // make sure the old lease helder is still valid
    bool lease_location_is_still_valid = false;
    for (auto& valid_location : advanced_locations) {
      if (valid_location.server_hostname() ==
              prev_lease_holder_location.server_hostname() ||
          valid_location.server_port() ==
              prev_lease_holder_location.server_port()) {
        lease_location_is_still_valid = true;
      }
    }
    if (!lease_location_is_still_valid) {
      LOG(ERROR) << "Original lease server for " << chunk_handle
                 << " is no longer available; will choose new lease holder"
                 << primary_location.server_hostname();
    } else {
      // lease not expired
      if (absl::FromUnixSeconds(lease_location_expiration_time.second) >
          absl::Now()) {
        LOG(INFO) << "Reuse existing lease for " << chunk_handle << ", held by "
                  << prev_lease_holder_location.server_hostname();
        lease_granted = true;
        primary_location = prev_lease_holder_location;
      } else {
        // clean up
        LOG(INFO) << "Original lease is expired for " << chunk_handle
                  << " held by " << primary_location.server_hostname();
        metadata_manager()->RemovePrimaryLeaseMetadata(chunk_handle);
      }
    }
  }

  if (!lease_granted) {
    for (auto& location : advanced_locations) {
      std::string primary_server_hostname = location.server_hostname();
      if (resolve_hostname_) {
        primary_server_hostname =
            config_manager_->ResolveHostname(primary_server_hostname);
      }
      const std::string& primary_server_address(
          primary_server_hostname + ":" +
          std::to_string(location.server_port()));
      auto lease_service_client(
          GetOrCreateChunkServerProtocolClient(primary_server_address));

      LOG(INFO)
          << "MasterMetadataService trying to grant write lease to server "
          << primary_server_address;
      // Prepare GrantLeaseRequest to send to chunk server
      GrantLeaseRequest grant_lease_request;
      grant_lease_request.set_chunk_handle(chunk_handle);
      grant_lease_request.set_chunk_version(chunk_version + 1);
      uint64_t expiration_unix_sec = absl::ToUnixSeconds(
          absl::Now() + config_manager_->GetWriteLeaseTimeout());
      grant_lease_request.mutable_lease_expiration_time()->set_seconds(
          expiration_unix_sec);
      grpc::ClientContext client_context;
      common::SetClientContextDeadline(client_context, config_manager_);

      // Issue GrantLeaseRequest request and check status
      google::protobuf::util::StatusOr<GrantLeaseReply> grant_lease_reply_or(
          lease_service_client->SendRequest(grant_lease_request,
                                            client_context));

      // Handle error, and logging
      if (!grant_lease_reply_or.ok()) {
        LOG(ERROR) << "Grant lease request for chunk " << chunk_handle << " at "
                   << primary_server_address << " failed due to "
                   << grant_lease_reply_or.status();
        continue;
      } else {
        LOG(INFO) << "Grant lease request for chunk " << chunk_handle << " at "
                  << primary_server_address << " succeeded";
        lease_granted = true;
        primary_location = location;
        metadata_manager()->SetPrimaryLeaseMetadata(chunk_handle, location,
                                                    expiration_unix_sec);
        break;
      }
    }
  }

  if (!lease_granted) {
    return grpc::Status(grpc::UNAVAILABLE,
                        "No chunk server is available to accept the write "
                        "lease and act as the primary. Abort");
  }

  reply->mutable_metadata()->set_chunk_handle(chunk_handle);
  reply->mutable_metadata()->set_version(new_chunk_version);
  *reply->mutable_metadata()->mutable_primary_location() = primary_location;
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
  // the metadata and the garbage collection of actual chunk is achieved by 
  // the heartbeat mechanism between master and chunk servers. 
  const std::string& filename(request->filename());
  LOG(INFO) << "Trying to delete file and chunk metadata associated with "
            << filename;
  metadata_manager()->DeleteFileAndChunkMetadata(filename);
  return grpc::Status::OK;
}

}  // namespace service
}  // namespace gfs
