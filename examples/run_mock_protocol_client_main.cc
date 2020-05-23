#include <iostream>
#include <memory>
#include <string>

#include "google/protobuf/timestamp.pb.h"
#include "grpcpp/grpcpp.h"
#include "src/common/protocol_client/chunk_server_service_gfs_client.h"
#include "src/common/protocol_client/chunk_server_service_master_server_client.h"
#include "src/common/protocol_client/master_metadata_service_client.h"
#include "src/common/system_logger.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"
#include "src/protos/grpc/chunk_server_lease_service.grpc.pb.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"

using gfs::service::ChunkServerServiceGfsClient;
using gfs::service::ChunkServerServiceMasterServerClient;
using gfs::service::MasterMetadataServiceClient;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using protos::grpc::AdvanceFileChunkVersionRequest;
using protos::grpc::DeleteFileRequest;
using protos::grpc::GrantLeaseRequest;
using protos::grpc::InitFileChunkRequest;
using protos::grpc::OpenFileRequest;
using protos::grpc::ReadFileChunkRequest;
using protos::grpc::RevokeLeaseRequest;
using protos::grpc::SendFileChunkRequest;
using protos::grpc::WriteFileChunkRequest;
using protos::grpc::WriteFileChunkRequestHeader;


template <typename T, typename U>
void LogRequestAndResponse(T request, StatusOr<U> reply_or) {
  LOG(INFO) << "Request sent: \n" << request.DebugString();
  if (reply_or.ok()) {
    LOG(INFO) << "Response received: \n" << reply_or.ValueOrDie().DebugString();
  } else {
    LOG(ERROR) << "Request failed: \n" << reply_or.status().ToString();
  }
}

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
  auto chunk_server_file_channel = 
      grpc::CreateChannel(chunk_server_address, credentials);
  MasterMetadataServiceClient metadata_client(master_channel);
  // Master-side client wrapper to issue requests to chunk server
  ChunkServerServiceMasterServerClient chunk_server_master_client(
      chunk_server_lease_channel);
  // Client / Chunk server-side client wrapper to issue requests to chunk server
  ChunkServerServiceGfsClient chunk_server_client_client(
      chunk_server_file_channel);

  // Prepare a mock gRPC: OpenFile
  OpenFileRequest open_request;
  open_request.set_filename("/tmp/test_open_file");
  open_request.set_chunk_index(0);
  open_request.set_mode(OpenFileRequest::WRITE);
  open_request.set_create_if_not_exists(true);

  grpc::ClientContext client_context;
  LogRequestAndResponse(
      open_request, metadata_client.SendRequest(open_request, client_context));

  // Prepare a mock gRPC: DeleteFile
  DeleteFileRequest delete_request;
  delete_request.set_filename("/tmp/test_delete_file");

  grpc::ClientContext client_context2;
  Status status = metadata_client.SendRequest(delete_request, client_context2);
  LOG(INFO) << "Request sent: " << delete_request.DebugString();
  LOG(INFO) << "Request status: " << status.ToString();

  // Prepare a mock gRPC: GrantLease
  GrantLeaseRequest grant_lease_request;
  grant_lease_request.set_chunk_handle("9d2a2342-97f9-11ea");
  grant_lease_request.set_chunk_version(1);
  grant_lease_request.mutable_lease_expiration_time()->set_seconds(1000);

  grpc::ClientContext client_context3;
  LogRequestAndResponse(
      grant_lease_request,
      chunk_server_master_client.SendRequest(grant_lease_request, 
                                             client_context3));

  // Prepare a mock gRPC: RevokeLease
  RevokeLeaseRequest revoke_lease_request;
  revoke_lease_request.set_chunk_handle("9d2a2342-97f9-11ea");
  revoke_lease_request.mutable_original_lease_expiration_time()->set_seconds(
      1000);

  grpc::ClientContext client_context4;
  LogRequestAndResponse(
      revoke_lease_request,
      chunk_server_master_client.SendRequest(revoke_lease_request, 
                                             client_context4));

  // Prepare a mock gRPC: InitFileChunk
  InitFileChunkRequest init_file_request;
  init_file_request.set_chunk_handle("9d2a2342-97f9-11ea");

  grpc::ClientContext client_context5;
  LogRequestAndResponse(
      init_file_request,
      chunk_server_master_client.SendRequest(init_file_request, 
                                             client_context5));

  // Prepare a mock gRPC: ReadFileChunk
  ReadFileChunkRequest read_file_request;
  read_file_request.set_chunk_handle("9d2a2342-97f9-11ea");

  grpc::ClientContext client_context6;
  LogRequestAndResponse(
      read_file_request,
      chunk_server_client_client.SendRequest(read_file_request, 
                                             client_context6));

  // Prepare a mock gRPC: WriteFileChunk
  WriteFileChunkRequestHeader write_file_request_header;
  SendFileChunkRequest send_file_request;
  write_file_request_header.set_chunk_handle("9d2a2342-97f9-11ea");
  write_file_request_header.set_chunk_version(10);
  write_file_request_header.set_offset_start(100);
  write_file_request_header.set_length(50);
  std::string payload = "Hello World";
  *send_file_request.mutable_header() = write_file_request_header;
  send_file_request.set_data(payload.c_str());

  grpc::ClientContext client_context8;
  LogRequestAndResponse(
      send_file_request,
      chunk_server_client_client.SendRequest(send_file_request, 
                                             client_context8));

  // Prepare a mock gRPC: AdvanceFileChunkVersion
  AdvanceFileChunkVersionRequest advance_version_request;
  advance_version_request.set_chunk_handle("9d2a2342-97f9-11ea");

  grpc::ClientContext client_context9;
  LogRequestAndResponse(advance_version_request,
                        chunk_server_master_client.SendRequest(
                            advance_version_request,
                            client_context9));

  return 0;
}
