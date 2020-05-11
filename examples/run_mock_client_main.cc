#include <iostream>
#include <memory>
#include <string>

#include "grpcpp/grpcpp.h"
#include "src/common/protocols/protocol_manager.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"

using gfs::protocols::MasterMetadataServiceRpcProtocols;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using protos::grpc::DeleteFileRequest;
using protos::grpc::OpenFileReply;
using protos::grpc::OpenFileRequest;

int main(int argc, char** argv) {
  // Initialize an instance of communication manager
  // TODO(tugan): add support to listen on host:port based on configuration
  std::string server_address("0.0.0.0:50051");
  auto credentials = grpc::InsecureChannelCredentials();
  auto master_channel = grpc::CreateChannel(server_address, credentials);
  MasterMetadataServiceRpcProtocols metadata_client(master_channel);

  // Prepare a mock gRPC request and client context
  OpenFileRequest open_request;
  grpc::ClientContext client_context1;
  open_request.set_filename("/tmp/test_open_file");
  open_request.set_chunk_index(0);
  open_request.set_mode(OpenFileRequest::WRITE);
  open_request.set_create_if_not_exists(true);

  // Send the request and print the response to screen
  StatusOr<OpenFileReply> status_or =
      metadata_client.SendRequest(open_request, client_context1);
  std::cout << "Request sent: \n" << open_request.DebugString() << std::endl;

  if (status_or.ok()) {
    std::cout << "Response received: \n"
              << status_or.ValueOrDie().DebugString() << std::endl;
  } else {
    std::cout << "Request failed: \n"
              << status_or.status().ToString() << std::endl;
  }

  DeleteFileRequest delete_request;
  delete_request.set_filename("/tmp/test_delete_file");
  grpc::ClientContext client_context2;
  Status status = metadata_client.SendRequest(delete_request, client_context2);
  std::cout << "Request sent: \n" << delete_request.DebugString() << std::endl;
  std::cout << "Request status: \n" << status.ToString() << std::endl;

  return 0;
}