#ifndef GFS_COMMON_PROTOCOL_CLIENT_MASTER_CHUNK_SERVER_MANAGER_SERVICE_CLIENT_H_
#define GFS_COMMON_PROTOCOL_CLIENT_MASTER_CHUNK_SERVER_MANAGER_SERVICE_CLIENT_H_

#include <memory>

#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/master_chunk_server_manager_service.grpc.pb.h"

namespace gfs {
namespace service {

// MasterChunkServerManagerServiceClient for sending gRPC requests to the master
// server to report the chunkserver chunks to the master.
//
// Example:
//
//  grpc::Channel master_channel =
//     grpc::CreateChannel("0.0.0.0:50051", grpc::InsecureChannelCredentials());
//  MasterChunkServerManagerServiceClient client(master_channel);
//
//  ReportChunkServerRequest request;
//  // Populate your requests here ...
//
//  grpc::ClientContext client_context;
//
//  // Add optional data to client context, if applicable
//  client_context.set_deadline(std::chrono::system_clock::now() +
//                  std::chrono::milliseconds(2000)); // optional deadline
//  client_context.AddMetadata("foo", "bar"); // optional metadata
//
//  // Send the request and get the reply
//  StatusOr<ReportChunkServerReply> status_or =
//    client.SendRequest(request, client_context);
//  if (status_or.ok()) {
//    LOG(INFO) << status_or.ValueOrDie().DebugString() << std::endl;
//  }
class MasterChunkServerManagerServiceClient {
 public:
  // Initialize a protocol manager for talking to a gRPC server listening on
  // the specified gRPC |channel|, which handles gRPCs defined in the
  // MasterChunkServerManagerService.
  MasterChunkServerManagerServiceClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(protos::grpc::MasterChunkServerManagerService::NewStub(channel)) {
  }

  // Send a ReportChunkServerRequest gRPC |request| to the master server, and
  // return master's corresponding reply if successful; otherwise a Status with
  // error message. This method is synchronous and will block until it hears
  // from the master
  // This chunkserver report call should be less frequent compared to the
  // heartbeat call (which is used to see if the chunkserver is still running),
  // due to the potential size of the messages. Since there can be a very large
  // number of chunks on each chunkserver. Should be done on chunkserver
  // startup/restart and then periodically after that.
  //
  // If you need to customize the gRPC behavior, such as sending extra metadata
  // or set a timeout for the gRPC call, pass an extra gRPC |context| object.
  // Otherwise, default client context is used. For more, see:
  // https://github.com/grpc/grpc/blob/master/include/grpcpp/impl/codegen/client_context_impl.h
  //
  // Note that you should NOT reuse client context, as they are call-specific.
  //
  // TODO(bmokutub): support sending asynchronous client requests
  google::protobuf::util::StatusOr<protos::grpc::ReportChunkServerReply>
  SendRequest(const protos::grpc::ReportChunkServerRequest& request);

  google::protobuf::util::StatusOr<protos::grpc::ReportChunkServerReply>
  SendRequest(const protos::grpc::ReportChunkServerRequest& request,
              grpc::ClientContext& context);

 private:
  // The gRPC client for managing protocols defined in
  // MasterChunkServerManagerService.
  std::unique_ptr<protos::grpc::MasterChunkServerManagerService::Stub> stub_;
};

}  // namespace service
}  // namespace gfs

#endif  // GFS_COMMON_PROTOCOL_CLIENT_MASTER_CHUNK_SERVER_MANAGER_SERVICE_CLIENT_H_
