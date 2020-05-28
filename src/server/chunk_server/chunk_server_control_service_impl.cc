#include "src/server/chunk_server/chunk_server_control_service_impl.h"

#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/chunk_server_control_service.grpc.pb.h"

using grpc::ServerContext;
using protos::grpc::CheckHeartBeatReply;
using protos::grpc::CheckHeartBeatRequest;
using protos::grpc::TriggerReportChunkServerReply;
using protos::grpc::TriggerReportChunkServerRequest;

namespace gfs {
namespace service {

// Handle a CheckHeartBeat request sent by the master.
grpc::Status ChunkServerControlServiceImpl::CheckHeartBeat(
    ServerContext* context, const CheckHeartBeatRequest* request,
    CheckHeartBeatReply* reply) {
  // Just to check if server is alive, reply to notify its alive
  *reply->mutable_request() = *request;
  return grpc::Status::OK;
}

// Handle a TriggerReportChunkServer request by master.
grpc::Status ChunkServerControlServiceImpl::TriggerReportChunkServer(
    ServerContext* context, const TriggerReportChunkServerRequest* request,
    TriggerReportChunkServerReply* reply) {
  // TODO(bmokutub): Make the report chunkserver client call to the master
  // chunkserver mgr service.
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "needs implementation");
}

}  // namespace service
}  // namespace gfs
