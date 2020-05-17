#include "src/server/master_server/master_chunk_server_manager_service_impl.h"

#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/master_chunk_server_manager_service.grpc.pb.h"

using grpc::ServerContext;
using protos::grpc::ReportChunkServerReply;
using protos::grpc::ReportChunkServerRequest;

namespace gfs {
namespace service {

grpc::Status MasterChunkServerManagerServiceImpl::ReportChunkServer(
    ServerContext* context, const ReportChunkServerRequest* request,
    ReportChunkServerReply* reply) {
  // TODO(bmokutub): handle the request by using the ChunkServerManager for
  // updating chunk/chunkserver map and metadata manager to check chunk latest
  // version.
  return grpc::Status::OK;
}
}  // namespace service
}  // namespace gfs
