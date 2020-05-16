#include "src/server/chunk_server/chunk_server_lease_service_impl.h"

#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/chunk_server_lease_service.grpc.pb.h"

using grpc::ServerContext;
using protos::grpc::GrantLeaseReply;
using protos::grpc::GrantLeaseRequest;
using protos::grpc::RevokeLeaseReply;
using protos::grpc::RevokeLeaseRequest;

namespace gfs {
namespace service {

grpc::Status ChunkServerLeaseServiceImpl::GrantLease(
    ServerContext* context, const GrantLeaseRequest* request,
    GrantLeaseReply* reply) {
  // TODO(someone): implement the GFS chunk server logic here
  *reply->mutable_request() = *request;
  reply->set_status(GrantLeaseReply::ACCEPTED);
  return grpc::Status::OK;
}

grpc::Status ChunkServerLeaseServiceImpl::RevokeLease(
    ServerContext* context, const RevokeLeaseRequest* request,
    RevokeLeaseReply* reply) {
  // TODO(someone): implement the GFS chunk server logic here
  return grpc::Status(grpc::StatusCode::NOT_FOUND,
                      "chunk handle doesn't exist");
}

}  // namespace service
}  // namespace gfs
