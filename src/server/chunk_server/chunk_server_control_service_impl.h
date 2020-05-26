#ifndef GFS_SERVER_CHUNK_SERVER_CHUNK_SERVER_CONTROL_SERVICE_IMPL_H_
#define GFS_SERVER_CHUNK_SERVER_CHUNK_SERVER_CONTROL_SERVICE_IMPL_H_

#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/chunk_server_control_service.grpc.pb.h"

namespace gfs {
namespace service {

// The synchronous implementation for handling ChunkServerControlService
// requests. This service running on the chunkserver is used by the master to
// send control information to it. Such as checking the chunkserver heartbeat.
class ChunkServerControlServiceImpl final
    : public protos::grpc::ChunkServerControlService::Service {
  // Handle a CheckHeartBeat request sent by the master.
  // Used to check if the chunkserver is alive.
  grpc::Status CheckHeartBeat(
      grpc::ServerContext* context,
      const protos::grpc::CheckHeartBeatRequest* request,
      protos::grpc::CheckHeartBeatReply* reply) override;

  // Handle a TriggerReportChunkServer request by master. Usually the
  // chunkserver reports itself to the master, but there are instances where the
  // master needs to trigger/ask for the report itself. e.g. if the master
  // crashed and restarting.
  grpc::Status TriggerReportChunkServer(
      grpc::ServerContext* context,
      const protos::grpc::TriggerReportChunkServerRequest* request,
      protos::grpc::TriggerReportChunkServerReply* reply) override;
};

// The asynchronous implementation for handling ChunkServerControlService
// requests
// TODO(bmokutub): support handling requests asynchronously
class ChunkServerControlServiceAsyncImpl final
    : public protos::grpc::ChunkServerControlService::Service {};

}  // namespace service
}  // namespace gfs

#endif  // GFS_SERVER_CHUNK_SERVER_CHUNK_SERVER_CONTROL_SERVICE_IMPL_H_
