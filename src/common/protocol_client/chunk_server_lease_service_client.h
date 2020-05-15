#ifndef GFS_COMMON_PROTOCOL_CLIENT_CHUNK_SERVER_LEASE_SERVICE_CLIENT_H_
#define GFS_COMMON_PROTOCOL_CLIENT_CHUNK_SERVER_LEASE_SERVICE_CLIENT_H_

#include <memory>

#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/chunk_server_lease_service.grpc.pb.h"

namespace gfs {
namespace service {

class ChunkServerLeaseServiceClient {
 public:
  // Initialize a protocol manager for talking to a gRPC server listening on
  // the specified gRPC |channel|, which handles gRPCs defined in the
  // ChunkServerLeaseService.
  ChunkServerLeaseServiceClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(protos::grpc::ChunkServerLeaseService::NewStub(channel)) {}

  // Send an GrantLease gRPC |request| to the chunk server, and return chunk
  // server's corresponding reply if successful; otherwise a Status with error
  // message. This method is synchronous and will block until it hears from the
  // chunk server. This gRPC is only sent by master to the chunk server.
  //
  // If you need to customize the gRPC behavior, such as sending extra metadata
  // or set a timeout for the gRPC call, pass an extra gRPC |context| object.
  // Otherwise, default client context is used. For more, see:
  // https://github.com/grpc/grpc/blob/master/include/grpcpp/impl/codegen/client_context_impl.h
  //
  // Note that you should NOT reuse client context, as they are call-specific.
  //
  // TODO(tugan): support sending asynchronous client requests
  google::protobuf::util::StatusOr<protos::grpc::GrantLeaseReply> SendRequest(
      const protos::grpc::GrantLeaseRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::GrantLeaseReply> SendRequest(
      const protos::grpc::GrantLeaseRequest& request,
      grpc::ClientContext& context);

  // Send an RevokeLease gRPC |request| to the chunk server, and return chunk
  // server's corresponding reply if successful; otherwise a Status with error
  // message. This method is synchronous and will block until it hears from the
  // chunk server.
  //
  // If you need to customize the gRPC behavior, such as sending extra metadata
  // or set a timeout for the gRPC call, pass an extra gRPC |context| object.
  // Otherwise, default client context is used. For more, see:
  // https://github.com/grpc/grpc/blob/master/include/grpcpp/impl/codegen/client_context_impl.h
  //
  // Note that you should NOT reuse client context, as they are call-specific.
  //
  // TODO(tugan): support sending asynchronous client requests
  google::protobuf::util::StatusOr<protos::grpc::RevokeLeaseReply> SendRequest(
      const protos::grpc::RevokeLeaseRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::RevokeLeaseReply> SendRequest(
      const protos::grpc::RevokeLeaseRequest& request,
      grpc::ClientContext& context);

 private:
  // The gRPC client for managing protocols defined in ChunkServerLeaseService
  std::unique_ptr<protos::grpc::ChunkServerLeaseService::Stub> stub_;
};

}  // namespace service
}  // namespace gfs

#endif  // GFS_COMMON_PROTOCOL_CLIENT_CHUNK_SERVER_LEASE_SERVICE_CLIENT_H_