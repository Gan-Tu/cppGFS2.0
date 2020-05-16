#include <iostream>
#include <memory>
#include <string>

#include "google/protobuf/timestamp.pb.h"
#include "grpcpp/grpcpp.h"
#include "src/common/protocol_client/chunk_server_service_client.h"
#include "src/common/protocol_client/master_metadata_service_client.h"
#include "src/common/system_logger.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"
#include "src/protos/grpc/chunk_server_lease_service.grpc.pb.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"

using gfs::service::ChunkServerServiceClient;
using gfs::service::MasterMetadataServiceClient;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using protos::grpc::DeleteFileRequest;
using protos::grpc::GrantLeaseReply;
using protos::grpc::GrantLeaseRequest;
using protos::grpc::InitFileChunkReply;
using protos::grpc::InitFileChunkRequest;
using protos::grpc::OpenFileReply;
using protos::grpc::OpenFileRequest;
using protos::grpc::RevokeLeaseReply;
using protos::grpc::RevokeLeaseRequest;

int main(int argc, char** argv) {
  gfs::common::SystemLogger::GetInstance().Initialize(/*program_name=*/argv[0]);

  // Initialize an instance of communication manager
  // TODO(tugan): add support to listen on host:port based on configuration
  std::string master_address("0.0.0.0:50051");
  std::string chunk_server_address("0.0.0.0:50052");
  auto credentials = grpc::InsecureChannelCredentials();
  auto master_channel = grpc::CreateChannel(master_address, credentials);
  auto chunk_server_lease_channel =
      grpc::CreateChannel(chunk_server_address, credentials);
  MasterMetadataServiceClient metadata_client(master_channel);
  ChunkServerServiceClient chunk_server_client(chunk_server_lease_channel);

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
  LOG(INFO) << "Request #1 sent: " << open_request.DebugString() << std::endl;

  if (metadata_or.ok()) {
    LOG(INFO) << "Response #1 received: "
              << metadata_or.ValueOrDie().DebugString() << std::endl;
  } else {
    LOG(INFO) << "Request #1 failed: " << metadata_or.status().ToString()
              << std::endl;
  }

  DeleteFileRequest delete_request;
  delete_request.set_filename("/tmp/test_delete_file");
  grpc::ClientContext client_context2;
  Status status = metadata_client.SendRequest(delete_request, client_context2);
  LOG(INFO) << "Request #2 sent: " << delete_request.DebugString() << std::endl;
  LOG(INFO) << "Request #2 status: " << status.ToString() << std::endl;

  google::protobuf::Timestamp lease_expiration_time;
  lease_expiration_time.set_seconds(1000);
  GrantLeaseRequest grant_lease_request;
  grant_lease_request.set_chunk_handle("/tmp/test_grant_lease");
  grant_lease_request.set_chunk_version(1);
  *grant_lease_request.mutable_lease_expiration_time() = lease_expiration_time;
  grpc::ClientContext client_context3;
  StatusOr<GrantLeaseReply> grant_lease_or =
      chunk_server_client.SendRequest(grant_lease_request, client_context3);
  LOG(INFO) << "Request #3 sent: " << grant_lease_request.DebugString()
            << std::endl;

  if (grant_lease_or.ok()) {
    LOG(INFO) << "Response #3 received: "
              << grant_lease_or.ValueOrDie().DebugString() << std::endl;
  } else {
    LOG(INFO) << "Request #3 failed: " << grant_lease_or.status().ToString()
              << std::endl;
  }

  RevokeLeaseRequest revoke_lease_request;
  revoke_lease_request.set_chunk_handle("/tmp/test_revoke_lease");
  *revoke_lease_request.mutable_original_lease_expiration_time() =
      lease_expiration_time;
  grpc::ClientContext client_context4;
  StatusOr<RevokeLeaseReply> revoke_lease_or =
      chunk_server_client.SendRequest(revoke_lease_request, client_context4);
  LOG(INFO) << "Request #4 sent: " << revoke_lease_request.DebugString()
            << std::endl;

  if (revoke_lease_or.ok()) {
    LOG(INFO) << "Response #4 received: "
              << revoke_lease_or.ValueOrDie().DebugString() << std::endl;
  } else {
    LOG(INFO) << "Request #4 failed: " << revoke_lease_or.status().ToString()
              << std::endl;
  }

  InitFileChunkRequest init_file_request;
  init_file_request.set_chunk_handle("/tmp/test_init_file");
  grpc::ClientContext client_context5;
  StatusOr<InitFileChunkReply> init_file_or =
      chunk_server_client.SendRequest(init_file_request, client_context5);
  LOG(INFO) << "Request #4 sent: " << init_file_request.DebugString()
            << std::endl;

  if (init_file_or.ok()) {
    LOG(INFO) << "Response #4 received: "
              << init_file_or.ValueOrDie().DebugString() << std::endl;
  } else {
    LOG(INFO) << "Request #4 failed: " << init_file_or.status().ToString()
              << std::endl;
  }

  return 0;
}