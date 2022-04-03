#ifndef GFS_COMMON_PROTOCOL_CLIENT_MASTER_SERVICE_CLIENT_H_
#define GFS_COMMON_PROTOCOL_CLIENT_MASTER_SERVICE_CLIENT_H_

#include <memory>

#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"

namespace gfs {
namespace service {

// Communication manager for sending gRPC requests to the master server
// to perform metadata related operations.
//
// Example:
//
//  grpc::Channel master_channel =
//     grpc::CreateChannel("0.0.0.0:50051", grpc::InsecureChannelCredentials());
//  MasterMetadataServiceClient metadata_client(master_channel);
//
//  OpenFileRequest request;
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
//  StatusOr<OpenFileReply> status_or =
//    metadata_client.SendRequest(request, client_context);
//  if (status_or.ok()) {
//    std::cout << status_or.value().DebugString() << std::endl;
//  }
class MasterMetadataServiceClient {
 public:
  // Initialize a protocol manager for talking to a gRPC server listening on
  // the specified gRPC |channel|, which handles gRPCs defined in the
  // MasterMetadataService.
  MasterMetadataServiceClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(protos::grpc::MasterMetadataService::NewStub(channel)) {}

  // Send an OpenFile gRPC |request| to the master server, and return master's
  // corresponding reply if successful; otherwise a Status with error message.
  // This method is synchronous and will block until it hears from the master
  //
  // If you need to customize the gRPC behavior, such as sending extra metadata
  // or set a timeout for the gRPC call, pass an extra gRPC |context| object.
  // Otherwise, default client context is used. For more, see:
  // https://github.com/grpc/grpc/blob/master/include/grpcpp/impl/codegen/client_context_impl.h
  //
  // Note that you should NOT reuse client context, as they are call-specific.
  //
  // TODO(tugan): support sending asynchronous client requests
  google::protobuf::util::StatusOr<protos::grpc::OpenFileReply> SendRequest(
      const protos::grpc::OpenFileRequest& request);
  google::protobuf::util::StatusOr<protos::grpc::OpenFileReply> SendRequest(
      const protos::grpc::OpenFileRequest& request,
      grpc::ClientContext& context);

  // Send an DeleteFile gRPC |request| to the master server. Return Status::OK
  // if the operation is successful; otherwise a Status with error message.
  // This method is synchronous and will block until it hears from the master
  //
  // If you need to customize the gRPC behavior, such as sending extra metadata
  // or set a timeout for the gRPC call, pass an extra gRPC |context| object.
  // Otherwise, default client context is used. For more, see:
  // https://github.com/grpc/grpc/blob/master/include/grpcpp/impl/codegen/client_context_impl.h
  //
  // Note that you should NOT reuse client context, as they are call-specific.
  //
  // TODO(tugan): support sending asynchronous client requests
  google::protobuf::util::Status SendRequest(
      const protos::grpc::DeleteFileRequest& request);
  google::protobuf::util::Status SendRequest(
      const protos::grpc::DeleteFileRequest& request,
      grpc::ClientContext& context);

 private:
  // The gRPC client for managing protocols defined in MasterMetadataService
  std::unique_ptr<protos::grpc::MasterMetadataService::Stub> stub_;
};

}  // namespace service
}  // namespace gfs

#endif  // GFS_COMMON_PROTOCOL_CLIENT_MASTER_SERVICE_CLIENT_H_