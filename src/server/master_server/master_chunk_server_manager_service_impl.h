#ifndef GFS_SERVER_MASTER_SERVER_MASTER_CHUNK_SERVER_MANAGER_SERVICE_IMPL_H_
#define GFS_SERVER_MASTER_SERVER_MASTER_CHUNK_SERVER_MANAGER_SERVICE_IMPL_H_

#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/master_chunk_server_manager_service.grpc.pb.h"

namespace gfs {
namespace service {

// The synchronous implementation for handling MasterChunkServerManagerService
// requests. This is a chunk server manager service running on the master used
// to to get chunkserver information from the chunkservers and register or
// update the chunkserver manager if needed. We use this to keep the chunkserver
// manager in sync with the chunkservers.
class MasterChunkServerManagerServiceImpl final
    : public protos::grpc::MasterChunkServerManagerService::Service {
  // Handles a ReportChunkServerRequest sent by a chunkserver.
  // The first time a chunkserver reports itself, we register the chunkserver.
  // And subsequently, it sends a periodic report, and we update the chunkserver
  // info on the chunkserver manager based on this report. This allows us to
  // know when a chunkserver doesn't have a chunk we have allocated to it. This
  // could be due to disk corruption or failure during the chunk write. We will
  // also use this to inform a chunkserver if the chunkserver has a stale
  // version of a chunk and ask it to delete it. And we will update the
  // chunkservermanager to not report that chunkserver as one of the locations
  // for the chunk.
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
