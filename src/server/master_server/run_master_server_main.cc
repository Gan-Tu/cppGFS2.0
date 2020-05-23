#include <memory>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "grpcpp/grpcpp.h"
#include "src/common/config_manager.h"
#include "src/common/system_logger.h"
#include "src/server/master_server/master_chunk_server_manager_service_impl.h"
#include "src/server/master_server/master_metadata_service_impl.h"

using gfs::common::ConfigManager;
using gfs::service::MasterChunkServerManagerServiceImpl;
using gfs::service::MasterMetadataServiceImpl;
using grpc::Server;
using grpc::ServerBuilder;

ABSL_FLAG(std::string, config_path, "data/config.yml", "/path/to/config.yml");
ABSL_FLAG(std::string, master_name, "master_server_01",
          "run as the given master, as defined in the config");
ABSL_FLAG(bool, use_docker_dns_server, false, "use docker's DNS server");

int main(int argc, char** argv) {
  gfs::common::SystemLogger::GetInstance().Initialize(/*program_name=*/argv[0]);

  // Parse command line arguments
  absl::ParseCommandLine(argc, argv);
  const std::string config_path = absl::GetFlag(FLAGS_config_path);
  const std::string master_name = absl::GetFlag(FLAGS_master_name);
  const bool use_docker_dns_server = absl::GetFlag(FLAGS_use_docker_dns_server);

  // Initialize configurations
  LOG(INFO) << "Reading GFS configuration: " << config_path;
  ConfigManager* config = ConfigManager::GetConfig(config_path).ValueOrDie();
  if (!config->HasMasterServer(master_name)) {
    LOG(ERROR) << "No master server found in config: " << master_name;
    return 1;
  }

  LOG(INFO) << "Running as master server: " << master_name;
  LOG(INFO) << "Server starting...";
  ServerBuilder builder;

  // TODO(tugan): add support to listen on host:port based on configuration
  std::string server_address(config->GetServerAddress(
      master_name, /*resolve_hostname=*/!use_docker_dns_server));

  // Listen on the given address without any authentication mechanism for now.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

  // Register a synchronous service for handling clients' metadata requests
  MasterMetadataServiceImpl metadata_service;
  builder.RegisterService(&metadata_service);

  // Register a synchronous service for coordinating with chunkservers
  MasterChunkServerManagerServiceImpl chunk_server_mgr_service;
  builder.RegisterService(&chunk_server_mgr_service);

  // Assemble and start the server
  std::unique_ptr<Server> server(builder.BuildAndStart());

  LOG(INFO) << "Server listening on " << server_address;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();

  return 0;
}