#include "src/server/master_server/master_metadata_service_impl.h"

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
using protos::grpc::DeleteFileRequest;
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

  // Step 1. Create the file chunk
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

  // Prepare the InitChunkFileReply
  *reply->mutable_metadata() = FileChunkMetadata();
  reply->mutable_metadata()->set_chunk_handle(chunk_handle);
  reply->mutable_metadata()->set_version(0);

  // Step 3. Coordinate with chunk servers to initialize the file chunk
  for (auto chunk_server_location : chunk_server_locations) {
    const std::string server_address(
        chunk_server_location.server_hostname() + ":" +
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
    if (reply->metadata().primary_location().server_hostname().empty()) {
      auto primary_location(chunk_server_location);
      *(reply->mutable_metadata()->mutable_primary_location()) =
          primary_location;
    }

    // Prepare the InitFileChunk reply with the chunk metadata
    reply->mutable_metadata()->mutable_locations()->Add(
        std::move(chunk_server_location));
  }

  // If all InitFileChunkRequest failed and no locations are reported,
  // we fail this RPC as client may not be able to write anything unless
  // retrying
  if (reply->metadata().locations().empty()) {
    LOG(ERROR) << "No file chunk requested for chunk handle " << chunk_handle;
    // TODO(Xi): uncomment the following lines once the file chunk services
    // are implemented on the chunk server's side
    // return grpc::Status(grpc::StatusCode::UNAVAILABLE,
    //                    "no chunk server is available");
  }

  return grpc::Status::OK;
}

grpc::Status MasterMetadataServiceImpl::HandleFileCreation(
    const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileReply* reply) {
  // Step 1. Create file metadata
  const std::string& filename(request->filename());
  LOG(INFO) << "MasterMetadataService handling file creation: " << filename;

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

  google::protobuf::util::StatusOr<std::string> chunk_handle_or(
      metadata_manager()->GetChunkHandle(filename, chunk_index));

  if (!chunk_handle_or.ok()) {
  }
 
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "needs implementation");
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
