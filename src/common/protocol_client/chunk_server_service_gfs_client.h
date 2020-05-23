#ifndef GFS_COMMON_PROTOCOL_CLIENT_CHUNK_SERVER_SERVICE_GFS_CLIENT_H_
#define GFS_COMMON_PROTOCOL_CLIENT_CHUNK_SERVER_SERVICE_GFS_CLIENT_H_

#include <memory>

#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"

namespace gfs {
namespace service {

// Communication manager for a GFS client to send gRPC requests to the chunk
// server to perform file operations.
//
// If you need to customize the gRPC behavior, such as sending extra metadata
// or set a timeout for the gRPC call, pass an extra gRPC |context| object.
// Otherwise, default client context is used. For more, see:
// https://github.com/grpc/grpc/blob/master/include/grpcpp/impl/codegen/client_context_impl.h
//
// Note that you should NOT reuse client context, as they are call-specific.
//
// TODO(tugan): support sending asynchronous client requests
class ChunkServerServiceGfsClient {
 public:
  // Initialize a protocol manager for talking to a gRPC server listening on
  // the specified gRPC |channel|, which handles gRPCs defined in both the
  // ChunkServerLeaseService and ChunkServerFileService.
  ChunkServerServiceGfsClient(std::shared_ptr<grpc::Channel> channel)
      : file_stub_(protos::grpc::ChunkServerFileService::NewStub(channel)) {}

  // Send an ReadFileChunk gRPC |request| to the chunk server, and return chunk
  // server's corresponding reply if successful; otherwise a Status with error
  // message. This method is synchronous and will block until it hears from the
  // chunk server.
  google::protobuf::util::StatusOr<protos::grpc::ReadFileChunkReply>
  SendRequest(const protos::grpc::ReadFileChunkRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::ReadFileChunkReply>
  SendRequest(const protos::grpc::ReadFileChunkRequest& request,
              grpc::ClientContext& context);

  // Send an SendChunkDataRequest gRPC |request| to the chunk server, and
  // return chunk server's corresponding reply if successful; otherwise a Status
  // with error message. This method is synchronous and will block until it
  // hears from the chunk server.
  google::protobuf::util::StatusOr<protos::grpc::SendChunkDataReply>
  SendRequest(const protos::grpc::SendChunkDataRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::SendChunkDataReply>
  SendRequest(const protos::grpc::SendChunkDataRequest& request,
              grpc::ClientContext& context);

  // Send an WriteFileChunkRequest gRPC |request| to the chunk server, and
  // return chunk server's corresponding reply if successful; otherwise a Status
  // with error message, as well as all other statuses from secondary replica.
  // This method is synchronous and will block until it hears from the chunk
  // server.
  google::protobuf::util::StatusOr<protos::grpc::WriteFileChunkReply>
  SendRequest(const protos::grpc::WriteFileChunkRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::WriteFileChunkReply>
  SendRequest(const protos::grpc::WriteFileChunkRequest& request,
              grpc::ClientContext& context);

 private:
  // The gRPC client for managing protocols defined in ChunkServerFileService
  std::unique_ptr<protos::grpc::ChunkServerFileService::Stub> file_stub_;
};

}  // namespace service
}  // namespace gfs

#endif  // GFS_COMMON_PROTOCOL_CLIENT_CHUNK_SERVER_SERVICE_GFS_CLIENT_H_
