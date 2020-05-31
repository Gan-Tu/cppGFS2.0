#include "src/server/master_server/master_metadata_service_impl.h"

#include "grpcpp/grpcpp.h"
#include "src/common/protocol_client/grpc_client_utils.h"
#include "src/common/utils.h"
#include "src/protos/chunk_server.pb.h"
#include "src/protos/metadata.pb.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"

using gfs::service::ChunkServerServiceMasterServerClient;
using google::protobuf::Empty;
using grpc::ServerContext;
using protos::ChunkServerLocation;
using protos::FileChunkMetadata;
using protos::grpc::DeleteFileRequest;
using protos::grpc::InitFileChunkRequest;
using protos::grpc::InitFileChunkReply;
using protos::grpc::OpenFileReply;
using protos::grpc::OpenFileRequest;

namespace gfs {
namespace service {

inline server::MetadataManager* MasterMetadataServiceImpl::metadata_manager() {
  return server::MetadataManager::GetInstance();
}

inline server::ChunkServerManager& MasterMetadataServiceImpl
    ::chunk_server_manager() {
  return server::ChunkServerManager::GetInstance();
}

grpc::Status MasterMetadataServiceImpl::HandleFileCreation(
    const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileReply* reply) {
  // Step 1. Create file metadata
  const std::string& filename(request->filename());
  google::protobuf::util::Status status(
      metadata_manager()->CreateFileMetadata(filename));
  if (!status.ok()) {
    return common::utils::ConvertProtobufStatusToGrpcStatus(status);
  }

  // Step 2. Create the first file chunk for this file
  google::protobuf::util::StatusOr<std::string> chunk_or(
      metadata_manager()->CreateChunkHandle(filename,0));
  if (!chunk_or.ok()) {
    return common::utils::ConvertProtobufStatusToGrpcStatus(status);
  }
  const std::string& chunk(chunk_or.ValueOrDie());

  // Step 3. Allocate chunk servers
  const ushort num_of_chunk_replica(3);
  // TODO(someone): make this number configurable via config.yml. Probably not
  // a priority right now
  auto chunk_server_locations(
      chunk_server_manager().AllocateChunkServer(chunk, num_of_chunk_replica));

  // Prepare the InitChunkFileReply
  *reply->mutable_metadata() = FileChunkMetadata();
  reply->mutable_metadata()->set_chunk_handle(chunk);
  reply->mutable_metadata()->set_version(0);

  // Step 4. Coordinate with chunk servers to initialize the file chunk 
  for(auto chunk_server_location : chunk_server_locations) {
    const std::string server_address(chunk_server_location.server_hostname() +
        std::to_string(chunk_server_location.server_port()));
    auto chunk_server_channel(grpc::CreateChannel(server_address,
        grpc::InsecureChannelCredentials()));
    // Register this chunk server Rpc client if not existed
    RegisterChunkServerRpcClient(server_address, chunk_server_channel);
    auto chunk_server_service_client(
        chunk_server_service_clients_.TryGetValue(server_address).first);
    // Prepare InitFileChunk Request to send to chunk server
    InitFileChunkRequest init_chunk_request;
    init_chunk_request.set_chunk_handle(chunk);
    grpc::ClientContext client_context;
    common::SetClientContextDeadline(client_context, config_manager_);
  
    // Issue InitFileChunk request and check status
    google::protobuf::util::StatusOr<InitFileChunkReply> init_chunk_or(
        chunk_server_service_client->SendRequest(init_chunk_request, 
            client_context));

    // TODO(Xi): we probably don't have to fail this request just because one
    // init chunk request is not successful. Will come back and refine this 
    // once the chunk server grpc impl is in shape
    if (!init_chunk_or.ok()) {
      common::utils::ConvertProtobufStatusToGrpcStatus(status);
    }

    // Pick a primary chunk server. Just select the first one
    if (reply->metadata().primary_location().server_hostname().empty()) {
      auto primary_location(chunk_server_location);
      *(reply->mutable_metadata()->mutable_primary_location()) 
          = primary_location; 
    }

    // Prepare the InitFileChunk reply with the chunk metadata
    reply->mutable_metadata()->mutable_locations()->Add(
        std::move(chunk_server_location));
  }

  return grpc::Status::OK;
}

grpc::Status MasterMetadataServiceImpl::HandleFileChunkRead(
    const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileReply* reply) {
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "needs implementation");
}

grpc::Status MasterMetadataServiceImpl::HandleFileChunkWrite(
    const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileReply* reply) {
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "needs implementation");
}

bool MasterMetadataServiceImpl::RegisterChunkServerRpcClient(
    std::string server_name, std::shared_ptr<grpc::Channel> channel) {
  return chunk_server_service_clients_.TryInsert(server_name,
             std::make_shared<ChunkServerServiceMasterServerClient>(channel));
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
