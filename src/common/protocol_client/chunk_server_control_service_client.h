#ifndef GFS_COMMON_PROTOCOL_CLIENT_CHUNK_SERVER_CONTROL_SERVICE_CLIENT_H_
#define GFS_COMMON_PROTOCOL_CLIENT_CHUNK_SERVER_CONTROL_SERVICE_CLIENT_H_

#include <memory>

#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/chunk_server_control_service.grpc.pb.h"

namespace gfs {
namespace service {

// Client used by master to send control information to chunkserver. Such as
// checking the chunkserver heartbeat.
//
// If you need to customize the gRPC behavior, such as sending extra metadata
// or set a timeout for the gRPC call, pass an extra gRPC |context| object.
// Otherwise, default client context is used. For more, see:
// https://github.com/grpc/grpc/blob/master/include/grpcpp/impl/codegen/client_context_impl.h
//
// Note that you should NOT reuse client context, as they are call-specific.
//
// TODO(bmokutub): support sending asynchronous client requests
class ChunkServerControlServiceClient {
 public:
  // Initialize a protocol manager for talking to a gRPC server listening on
  // the specified gRPC |channel|, which handles gRPCs defined in
  // ChunkServerControlService.
  ChunkServerControlServiceClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(protos::grpc::ChunkServerControlService::NewStub(channel)) {}

  // Used to check if the chunkserver is alive. The chunkserver sends an ok,
  // and a reply. A reply shows that the server is alive. This returns chunk
  // server's corresponding reply if successful; otherwise a Status with error
  // message. This method is synchronous and will block until it hears from the
  // chunk server. This gRPC is only sent by master to the chunk server.
  google::protobuf::util::StatusOr<protos::grpc::CheckHeartBeatReply>
  SendRequest(const protos::grpc::CheckHeartBeatRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::CheckHeartBeatReply>
  SendRequest(const protos::grpc::CheckHeartBeatRequest& request,
              grpc::ClientContext& context);

  // Usually the chunkserver reports itself to the master, but there are
  // instances where the master needs to trigger/ask for the report itself. e.g.
  // if the master crashed and restarting. This returns chunk server's
  // corresponding reply if successful; otherwise a Status with error message.
  // This method is synchronous and will block until it hears from the chunk
  // server.
  google::protobuf::util::StatusOr<protos::grpc::TriggerReportChunkServerReply>
  SendRequest(const protos::grpc::TriggerReportChunkServerRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::TriggerReportChunkServerReply>
  SendRequest(const protos::grpc::TriggerReportChunkServerRequest& request,
              grpc::ClientContext& context);

 private:
  // The gRPC client for managing protocols defined in
  // ChunkServerControlService.
  std::unique_ptr<protos::grpc::ChunkServerControlService::Stub> stub_;
};

}  // namespace service
}  // namespace gfs

#endif  // GFS_COMMON_PROTOCOL_CLIENT_CHUNK_SERVER_CONTROL_SERVICE_CLIENT_H_
