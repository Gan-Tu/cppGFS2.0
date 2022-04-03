#include <iostream>
#include <memory>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "google/protobuf/timestamp.pb.h"
#include "grpcpp/grpcpp.h"
#include "src/common/config_manager.h"
#include "src/common/protocol_client/chunk_server_service_gfs_client.h"
#include "src/common/protocol_client/chunk_server_service_server_client.h"
#include "src/common/protocol_client/master_metadata_service_client.h"
#include "src/common/system_logger.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"
#include "src/protos/grpc/chunk_server_lease_service.grpc.pb.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"

using gfs::common::ConfigManager;
using gfs::service::ChunkServerServiceChunkServerClient;
using gfs::service::ChunkServerServiceGfsClient;
using gfs::service::ChunkServerServiceMasterServerClient;
using gfs::service::MasterMetadataServiceClient;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using protos::grpc::AdvanceFileChunkVersionRequest;
using protos::grpc::ApplyMutationsRequest;
using protos::grpc::DeleteFileRequest;
using protos::grpc::GrantLeaseRequest;
using protos::grpc::InitFileChunkRequest;
using protos::grpc::OpenFileRequest;
using protos::grpc::ReadFileChunkRequest;
using protos::grpc::RevokeLeaseRequest;
using protos::grpc::SendChunkDataRequest;
using protos::grpc::WriteFileChunkRequest;
using protos::grpc::WriteFileChunkRequestHeader;

ABSL_FLAG(std::string, config_path, "data/config.yml", "/path/to/config.yml");
ABSL_FLAG(std::string, master_name, "master_server_01",
          "connect to the given master server, as defined in the config");
ABSL_FLAG(std::string, chunk_server_name, "chunk_server_01",
          "connect to the given chunk server, as defined in the config");
ABSL_FLAG(bool, use_docker_dns_server, false, "use docker's DNS server");

template <typename T, typename U>
void LogRequestAndResponse(T request, StatusOr<U> reply_or) {
  LOG(INFO) << "Request sent: \n" << request.DebugString();
  if (reply_or.ok()) {
    LOG(INFO) << "Response received: \n" << reply_or.value().DebugString();
  } else {
    LOG(ERROR) << "Request failed: \n" << reply_or.status().ToString();
  }
}

int main(int argc, char** argv) {
  gfs::common::SystemLogger::GetInstance().Initialize(/*program_name=*/argv[0]);

  // Parse command line arguments
  absl::ParseCommandLine(argc, argv);
  const std::string config_path = absl::GetFlag(FLAGS_config_path);
  const std::string master_name = absl::GetFlag(FLAGS_master_name);
  const std::string chunk_server_name = absl::GetFlag(FLAGS_chunk_server_name);
  const bool resolve_hostname = !absl::GetFlag(FLAGS_use_docker_dns_server);

  // Initialize configurations
  LOG(INFO) << "Reading GFS configuration: " << config_path;
  ConfigManager* config = ConfigManager::GetConfig(config_path).value();
  if (!config->HasMasterServer(master_name)) {
    LOG(ERROR) << "No master server found in config: " << master_name;
    return 1;
  } else if (!config->HasChunkServer(chunk_server_name)) {
    LOG(ERROR) << "No chunk server found in config: " << chunk_server_name;
    return 1;
  }

  // Initialize an instance of communication manager
  std::string master_address(
      config->GetServerAddress(master_name, resolve_hostname));
  std::string chunk_server_address(
      config->GetServerAddress(chunk_server_name, resolve_hostname));

  auto credentials = grpc::InsecureChannelCredentials();

  LOG(INFO) << "Connecting to master server at " << master_address;
  auto master_channel = grpc::CreateChannel(master_address, credentials);

  LOG(INFO) << "Connecting to chunk server at " << chunk_server_address;
  auto chunk_server_channel =
      grpc::CreateChannel(chunk_server_address, credentials);

  MasterMetadataServiceClient metadata_client(master_channel);
  // Master-side client wrapper to issue requests to chunk server
  ChunkServerServiceMasterServerClient chunk_server_master_client(
      chunk_server_channel);
  // Chunk server side client wrapper to issue requests to other chunk server
  ChunkServerServiceChunkServerClient chunk_server_intercom_client(
      chunk_server_channel);
  // Client / Chunk server-side client wrapper to issue requests to chunk server
  ChunkServerServiceGfsClient chunk_server_gfs_client(chunk_server_channel);

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
  grant_lease_request.mutable_lease_expiration_time()->set_seconds(1690735960);

  grpc::ClientContext client_context3;
  LogRequestAndResponse(grant_lease_request,
                        chunk_server_master_client.SendRequest(
                            grant_lease_request, client_context3));

  // Prepare a mock gRPC: RevokeLease
  RevokeLeaseRequest revoke_lease_request;
  revoke_lease_request.set_chunk_handle("9d2a2342-97f9-11ea");
  revoke_lease_request.mutable_original_lease_expiration_time()->set_seconds(
      1690735960);

  grpc::ClientContext client_context4;
  LogRequestAndResponse(revoke_lease_request,
                        chunk_server_master_client.SendRequest(
                            revoke_lease_request, client_context4));

  // Prepare a mock gRPC: InitFileChunk
  InitFileChunkRequest init_file_request;
  init_file_request.set_chunk_handle("9d2a2342-97f9-11ea");

  grpc::ClientContext client_context5;
  LogRequestAndResponse(init_file_request,
                        chunk_server_master_client.SendRequest(
                            init_file_request, client_context5));

  // Prepare a mock gRPC: ReadFileChunk
  ReadFileChunkRequest read_file_request;
  read_file_request.set_chunk_handle("9d2a2342-97f9-11ea");
  read_file_request.set_chunk_version(1);
  read_file_request.set_offset_start(0);
  read_file_request.set_length(10);

  grpc::ClientContext client_context6;
  LogRequestAndResponse(
      read_file_request,
      chunk_server_gfs_client.SendRequest(read_file_request, client_context6));

  // Prepare a mock gRPC: SendChunkData
  SendChunkDataRequest send_data_request;
  std::string payload = "Hello World";
  send_data_request.set_data(payload.c_str());
  send_data_request.set_checksum("XA0F2FJ824132LI2");

  grpc::ClientContext client_context8;
  LogRequestAndResponse(
      send_data_request,
      chunk_server_gfs_client.SendRequest(send_data_request, client_context8));

  // Prepare a mock gRPC: WriteFileChunk
  WriteFileChunkRequestHeader write_file_request_header;
  write_file_request_header.set_chunk_handle("9d2a2342-97f9-11ea");
  write_file_request_header.set_chunk_version(10);
  write_file_request_header.set_offset_start(100);
  write_file_request_header.set_length(50);
  write_file_request_header.set_data_checksum("XA0F2FJ824132LI2");
  WriteFileChunkRequest write_file_request;
  *write_file_request.mutable_header() = write_file_request_header;

  grpc::ClientContext client_context9;
  LogRequestAndResponse(
      write_file_request,
      chunk_server_gfs_client.SendRequest(write_file_request, client_context9));

  // Prepare a mock gRPC: AdvanceFileChunkVersion
  AdvanceFileChunkVersionRequest advance_version_request;
  advance_version_request.set_chunk_handle("9d2a2342-97f9-11ea");
  advance_version_request.set_new_chunk_version(2);

  grpc::ClientContext client_context10;
  LogRequestAndResponse(advance_version_request,
                        chunk_server_master_client.SendRequest(
                            advance_version_request, client_context10));

  // Prepare a mock gRPC: ApplyMutations
  ApplyMutationsRequest apply_mutations_request;
  *apply_mutations_request.add_headers() = write_file_request_header;

  grpc::ClientContext client_context11;
  LogRequestAndResponse(apply_mutations_request,
                        chunk_server_intercom_client.SendRequest(
                            apply_mutations_request, client_context11));

  return 0;
}
