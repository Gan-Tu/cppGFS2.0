#ifndef GFS_SERVER_MASTER_SERVER_MASTER_CHUNK_SERVER_MANAGER_SERVICE_IMPL_H_
#define GFS_SERVER_MASTER_SERVER_MASTER_CHUNK_SERVER_MANAGER_SERVICE_IMPL_H_

#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/master_chunk_server_manager_service.grpc.pb.h"

namespace gfs {
namespace service {

// The synchronous implementation for handling MasterChunkServerManagerService
// requests.
class MasterChunkServerManagerServiceImpl final
    : public protos::grpc::MasterChunkServerManagerService::Service {
  // Handles a ReportChunkServerRequest sent by a chunkserver.
  grpc::Status ReportChunkServer(
      grpc::ServerContext* context,
      const protos::grpc::ReportChunkServerRequest* request,
      protos::grpc::ReportChunkServerReply* reply) override;
};

// The asynchronous implementation for handling MasterChunkServerManagerService
// requests
// TODO(bmokutub): support handling client requests asynchronously
class MasterChunkServerManagerServiceAsyncImpl final
    : public protos::grpc::MasterChunkServerManagerService::Service {};

}  // namespace service
}  // namespace gfs

#endif  // GFS_SERVER_MASTER_SERVER_MASTER_CHUNK_SERVER_MANAGER_SERVICE_IMPL_H_
