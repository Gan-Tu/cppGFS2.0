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
    LOG(INFO) << "Chunk handle created: " << chunk_handle_or.ValueOrDie()
              << " for file " << filename;
  }
  const std::string& chunk_handle(chunk_handle_or.ValueOrDie());

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

  // Step 3. Coordinate with chunk servers to initialize the file chunk
  for (auto chunk_server_location : chunk_server_locations) {
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
                   << " failed: " << init_chunk_or.status().error_message();
      return common::utils::ConvertProtobufStatusToGrpcStatus(
          init_chunk_or.status());
    } else {
      LOG(INFO) << "InitFileChunkRequest for " << chunk_handle
                << " sent to chunk server " << server_address << " succeeded";
    }

    // Pick a primary chunk server. Just select the first one
    if (metadata.primary_location().server_hostname().empty()) {
      auto primary_location(chunk_server_location);
      *(metadata.mutable_primary_location()) = primary_location;
      LOG(INFO) << "Assign " << primary_location.DebugString()
                << " as the primary chunk location for " << chunk_handle;
      metadata_manager()->SetPrimaryChunkServerLocation(chunk_handle,
                                                      primary_location);
    }

    // Prepare the InitFileChunk reply with the chunk metadata
    *metadata.add_locations() = chunk_server_location;
  }

  if (metadata.locations().empty()) {
    LOG(ERROR) << "No chunk servers are available for allocation.";
    LOG(ERROR) << "No file chunk can be initialized for: " << chunk_handle;
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "no chunk server is available");
  }

  metadata_manager()->SetFileChunkMetadata(metadata);
  *reply->mutable_metadata() = metadata;

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
    LOG(ERROR) << "File metadata creation failed: " << status.error_message();
    return common::utils::ConvertProtobufStatusToGrpcStatus(status);
  } else {
    LOG(INFO) << "File metadata created for " << filename;
  }

  // Step 2. Create the first file chunk for this file and allocate chunk
  // servers
  return HandleFileChunkCreation(request, reply);
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
               << chunk_handle_or.status().error_message();
    return common::utils::ConvertProtobufStatusToGrpcStatus(
        chunk_handle_or.status());
  }

  // Step 2. Access the file chunk metadata
  const std::string& chunk_handle(chunk_handle_or.ValueOrDie());
  google::protobuf::util::StatusOr<FileChunkMetadata> file_chunk_metadata_or(
      metadata_manager()->GetFileChunkMetadata(chunk_handle));

  if (!file_chunk_metadata_or.ok()) {
    LOG(ERROR) << "File chunk metadata not accessible for " << chunk_handle;
    return common::utils::ConvertProtobufStatusToGrpcStatus(
        file_chunk_metadata_or.status());
  }

  // Set the file chunk metadata in reply
  *reply->mutable_metadata() = file_chunk_metadata_or.ValueOrDie();
  return grpc::Status::OK;
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
  }

  google::protobuf::util::StatusOr<FileChunkMetadata> file_chunk_metadata_or(
      metadata_manager()->GetFileChunkMetadata(chunk_handle_or.ValueOrDie()));
  if (!file_chunk_metadata_or.ok()) {
    LOG(ERROR) << "File chunk metadata not accessible for "
               << chunk_handle_or.ValueOrDie();
    return common::utils::ConvertProtobufStatusToGrpcStatus(
        file_chunk_metadata_or.status());
  }

  FileChunkMetadata metadata = file_chunk_metadata_or.ValueOrDie();
  *reply->mutable_metadata() = metadata;

  const std::string& chunk_handle(metadata.chunk_handle());
  uint32_t chunk_version(metadata.version());
  uint32_t new_chunk_version = chunk_version + 1;

  LOG(INFO) << "Advancing chunk version for chunk handle " << chunk_handle
            << " from " << chunk_version << " to " << new_chunk_version;

  // Now that all chunk server locations are stored in reply, the master first
  // advances the chunk version of this chunk and then send a GrantLeaseRequest
  for (auto chunk_server_location : metadata.locations()) {
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
      LOG(INFO) << "Removing chunk server "
                << chunk_server_location.server_hostname()
                << " from write locations for " << chunk_handle;
      // TODO(Xi): handle if all version advancement fails
      // TODO(tugan): the client shouldn't write to this chunk server anymore
    } else {
      LOG(INFO) << "Advanced chunk version for chunk " << chunk_handle
                << " on chunk server " << server_address;
    }
  }

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
  std::string primary_server_hostname =
      reply->metadata().primary_location().server_hostname();
  if (resolve_hostname_) {
    primary_server_hostname =
        config_manager_->ResolveHostname(primary_server_hostname);
  }
  const std::string& primary_server_address(
      primary_server_hostname + ":" +
      std::to_string(reply->metadata().primary_location().server_port()));
  auto lease_service_client(
      GetOrCreateChunkServerProtocolClient(primary_server_address));

  LOG(INFO) << "MasterMetadataService granting write lease to primary server "
            << primary_server_address;
  // Prepare GrantLeaseRequest to send to chunk server
  GrantLeaseRequest grant_lease_request;
  grant_lease_request.set_chunk_handle(chunk_handle);
  grant_lease_request.set_chunk_version(chunk_version + 1);
  grant_lease_request.mutable_lease_expiration_time()->set_seconds(
      absl::ToUnixSeconds(absl::Now() +
                          config_manager_->GetWriteLeaseTimeout()));
  grpc::ClientContext client_context;
  common::SetClientContextDeadline(client_context, config_manager_);

  // Issue GrantLeaseRequest request and check status
  google::protobuf::util::StatusOr<GrantLeaseReply> grant_lease_reply_or(
      lease_service_client->SendRequest(grant_lease_request, client_context));

  // Handle error, and logging
  if (!grant_lease_reply_or.ok()) {
    LOG(ERROR) << "Grant lease request for chunk " << chunk_handle << " at "
               << primary_server_address << " failed due to "
               << grant_lease_reply_or.status();
    // TODO(Xi): technically, we can retry on another server if the random
    // server cannot be a primary
    return common::utils::ConvertProtobufStatusToGrpcStatus(
        grant_lease_reply_or.status());
  } else {
    LOG(INFO) << "Grant lease request for chunk " << chunk_handle << " at "
              << primary_server_address << " succeeded";
  }

  // Get the Metadata back to reply
  auto updated_metadata_or(
      metadata_manager()->GetFileChunkMetadata(chunk_handle));
  if (!updated_metadata_or.ok()) {
    return common::utils::ConvertProtobufStatusToGrpcStatus(
        updated_metadata_or.status());
  }
  *reply->mutable_metadata() = updated_metadata_or.ValueOrDie();

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
  // TODO(everyone): implement the GFS master server logic here
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "needs implementation");
}

}  // namespace service
}  // namespace gfs
