#include "src/server/master_server/master_metadata_service_impl.h"

#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"

using google::protobuf::Empty;
using grpc::ServerContext;
using protos::grpc::DeleteFileRequest;
using protos::grpc::MasterMetadataService;
using protos::grpc::OpenFileReply;
using protos::grpc::OpenFileRequest;

namespace gfs {
namespace protocols {
namespace master {

grpc::Status MasterMetadataServiceImpl::OpenFile(ServerContext* context,
                                                 const OpenFileRequest* request,
                                                 OpenFileReply* reply) {
  // TODO(everyone): implement the GFS master server logic here
  *reply->mutable_request() = *request;
  reply->set_chunk_version(1);
  return grpc::Status::OK;
}

// Handle a DeleteFileRequest request sent by the client.
grpc::Status MasterMetadataServiceImpl::DeleteFile(
    ServerContext* context, const DeleteFileRequest* request,
    google::protobuf::Empty* reply) {
  // TODO(everyone): implement the GFS master server logic here
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "needs implementation");
}

}  // namespace master
}  // namespace protocols
}  // namespace gfs
