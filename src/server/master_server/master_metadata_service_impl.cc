#include "src/server/master_server/master_metadata_service_impl.h"

#include "grpcpp/grpcpp.h"
#include "src/common/utils.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"


using gfs::service::ChunkServerServiceMasterServerClient;
using google::protobuf::Empty;
using grpc::ServerContext;
using protos::grpc::DeleteFileRequest;
using protos::grpc::OpenFileReply;
using protos::grpc::OpenFileRequest;

namespace gfs {
namespace service {

inline server::MetadataManager* MasterMetadataServiceImpl::metadata_manager() {
  return server::MetadataManager::GetInstance();
}

grpc::Status MasterMetadataServiceImpl::HandleFileCreation(
    const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileReply* reply) {
  const std::string& filename(request->filename());
  google::protobuf::util::Status status(
      metadata_manager()->CreateFileMetadata(filename));
  return common::utils::ConvertProtobufStatusToGrpcStatus(status);
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
  auto iter_and_inserted = chunk_server_service_clients_.insert(
      {server_name,
       std::make_shared<ChunkServerServiceMasterServerClient>(channel)});
  return iter_and_inserted.second;
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
