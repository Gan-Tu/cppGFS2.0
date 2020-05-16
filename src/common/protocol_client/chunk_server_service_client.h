#ifndef GFS_COMMON_PROTOCOL_CLIENT_CHUNK_SERVER_SERVICE_CLIENT_H_
#define GFS_COMMON_PROTOCOL_CLIENT_CHUNK_SERVER_SERVICE_CLIENT_H_

#include <memory>

#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"
#include "src/protos/grpc/chunk_server_lease_service.grpc.pb.h"

namespace gfs {
namespace service {

// Communication manager for sending gRPC requests to the chunk server
// to perform file operations and lease operations.
//
// If you need to customize the gRPC behavior, such as sending extra metadata
// or set a timeout for the gRPC call, pass an extra gRPC |context| object.
// Otherwise, default client context is used. For more, see:
// https://github.com/grpc/grpc/blob/master/include/grpcpp/impl/codegen/client_context_impl.h
//
// Note that you should NOT reuse client context, as they are call-specific.
//
// TODO(tugan): support sending asynchronous client requests
class ChunkServerServiceClient {
 public:
  // Initialize a protocol manager for talking to a gRPC server listening on
  // the specified gRPC |channel|, which handles gRPCs defined in both the
  // ChunkServerLeaseService and ChunkServerFileService.
  ChunkServerServiceClient(std::shared_ptr<grpc::Channel> channel)
      : lease_stub_(protos::grpc::ChunkServerLeaseService::NewStub(channel)),
        file_stub_(protos::grpc::ChunkServerFileService::NewStub(channel)) {}

  // Send an GrantLease gRPC |request| to the chunk server, and return chunk
  // server's corresponding reply if successful; otherwise a Status with error
  // message. This method is synchronous and will block until it hears from the
  // chunk server. This gRPC is only sent by master to the chunk server.
  google::protobuf::util::StatusOr<protos::grpc::GrantLeaseReply> SendRequest(
      const protos::grpc::GrantLeaseRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::GrantLeaseReply> SendRequest(
      const protos::grpc::GrantLeaseRequest& request,
      grpc::ClientContext& context);

  // Send an RevokeLease gRPC |request| to the chunk server, and return chunk
  // server's corresponding reply if successful; otherwise a Status with error
  // message. This method is synchronous and will block until it hears from the
  // chunk server.
  google::protobuf::util::StatusOr<protos::grpc::RevokeLeaseReply> SendRequest(
      const protos::grpc::RevokeLeaseRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::RevokeLeaseReply> SendRequest(
      const protos::grpc::RevokeLeaseRequest& request,
      grpc::ClientContext& context);

  // Send an InitFileChunk gRPC |request| to the chunk server, and return chunk
  // server's corresponding reply if successful; otherwise a Status with error
  // message. This method is synchronous and will block until it hears from the
  // chunk server. This gRPC is only sent by master to the chunk server.
  google::protobuf::util::StatusOr<protos::grpc::InitFileChunkReply>
  SendRequest(const protos::grpc::InitFileChunkRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::InitFileChunkReply>
  SendRequest(const protos::grpc::InitFileChunkRequest& request,
              grpc::ClientContext& context);

  // Send an ReadFileChunk gRPC |request| to the chunk server, and return chunk
  // server's corresponding reply if successful; otherwise a Status with error
  // message. This method is synchronous and will block until it hears from the
  // chunk server.
  google::protobuf::util::StatusOr<protos::grpc::ReadFileChunkReply>
  SendRequest(const protos::grpc::ReadFileChunkRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::ReadFileChunkReply>
  SendRequest(const protos::grpc::ReadFileChunkRequest& request,
              grpc::ClientContext& context);

  // Send an AdvanceFileChunkVersion gRPC |request| to the chunk server, and
  // return chunk server's corresponding reply if successful; otherwise a Status
  // with error message. This method is synchronous and will block until it
  // hears from the chunk server. This gRPC is only sent by master to the chunk
  // server.
  google::protobuf::util::StatusOr<protos::grpc::AdvanceFileChunkVersionReply>
  SendRequest(const protos::grpc::AdvanceFileChunkVersionRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::AdvanceFileChunkVersionReply>
  SendRequest(const protos::grpc::AdvanceFileChunkVersionRequest& request,
              grpc::ClientContext& context);

  // Send an WriteFileChunkRequest gRPC |request| to the chunk server, and
  // return chunk server's corresponding reply if successful; otherwise a Status
  // with error message. This method is synchronous and will block until it
  // hears from the chunk server.
  google::protobuf::util::StatusOr<protos::grpc::WriteFileChunkReply>
  SendRequest(const protos::grpc::WriteFileChunkRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::WriteFileChunkReply>
  SendRequest(const protos::grpc::WriteFileChunkRequest& request,
              grpc::ClientContext& context);

 private:
  // The gRPC client for managing protocols defined in ChunkServerLeaseService
  std::unique_ptr<protos::grpc::ChunkServerLeaseService::Stub> lease_stub_;

  // The gRPC client for managing protocols defined in ChunkServerFileService
  std::unique_ptr<protos::grpc::ChunkServerFileService::Stub> file_stub_;
};

}  // namespace service
}  // namespace gfs

#endif  // GFS_COMMON_PROTOCOL_CLIENT_CHUNK_SERVER_SERVICE_CLIENT_H_