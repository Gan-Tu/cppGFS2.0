#include <iostream>
#include <memory>
#include <string>

#include "google/protobuf/timestamp.pb.h"
#include "grpcpp/grpcpp.h"
#include "src/common/protocol_client/chunk_server_lease_service_client.h"
#include "src/common/protocol_client/master_metadata_service_client.h"
#include "src/protos/grpc/chunk_server_lease_service.grpc.pb.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"

using gfs::service::ChunkServerLeaseServiceClient;
using gfs::service::MasterMetadataServiceClient;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using protos::grpc::DeleteFileRequest;
using protos::grpc::GrantLeaseReply;
using protos::grpc::GrantLeaseRequest;
using protos::grpc::OpenFileReply;
using protos::grpc::OpenFileRequest;
using protos::grpc::RevokeLeaseReply;
using protos::grpc::RevokeLeaseRequest;

int main(int argc, char** argv) {
  // Initialize an instance of communication manager
  // TODO(tugan): add support to listen on host:port based on configuration
  std::string metadata_service_address("0.0.0.0:50051");
  std::string lease_service_address("0.0.0.0:50052");
  auto credentials = grpc::InsecureChannelCredentials();
  auto master_channel =
      grpc::CreateChannel(metadata_service_address, credentials);
  auto chunk_server_lease_channel =
      grpc::CreateChannel(lease_service_address, credentials);
  MasterMetadataServiceClient metadata_client(master_channel);
  ChunkServerLeaseServiceClient lease_client(chunk_server_lease_channel);

  // Prepare a mock gRPC request and client context
  OpenFileRequest open_request;
  grpc::ClientContext client_context1;
  open_request.set_filename("/tmp/test_open_file");
  open_request.set_chunk_index(0);
  open_request.set_mode(OpenFileRequest::WRITE);
  open_request.set_create_if_not_exists(true);

  // Send the request and print the response to screen
  StatusOr<OpenFileReply> metadata_or =
      metadata_client.SendRequest(open_request, client_context1);
  std::cout << "Request sent: \n" << open_request.DebugString() << std::endl;

  if (metadata_or.ok()) {
    std::cout << "Response received: \n"
              << metadata_or.ValueOrDie().DebugString() << std::endl;
  } else {
    std::cout << "Request failed: \n"
              << metadata_or.status().ToString() << std::endl;
  }

  DeleteFileRequest delete_request;
  delete_request.set_filename("/tmp/test_delete_file");
  grpc::ClientContext client_context2;
  Status status = metadata_client.SendRequest(delete_request, client_context2);
  std::cout << "Request sent: \n" << delete_request.DebugString() << std::endl;
  std::cout << "Request status: \n" << status.ToString() << std::endl;

  google::protobuf::Timestamp lease_expiration_time;
  lease_expiration_time.set_seconds(1000);

  GrantLeaseRequest grant_lease_request;
  grant_lease_request.set_chunk_handle("/tmp/test_grant_lease");
  grant_lease_request.set_chunk_version(1);
  *grant_lease_request.mutable_lease_expiration_time() = lease_expiration_time;
  grpc::ClientContext client_context3;
  StatusOr<GrantLeaseReply> grant_lease_or =
      lease_client.SendRequest(grant_lease_request, client_context3);
  std::cout << "Request sent: \n"
            << grant_lease_request.DebugString() << std::endl;

  if (grant_lease_or.ok()) {
    std::cout << "Response received: \n"
              << grant_lease_or.ValueOrDie().DebugString() << std::endl;
  } else {
    std::cout << "Request failed: \n"
              << grant_lease_or.status().ToString() << std::endl;
  }

  return 0;
}