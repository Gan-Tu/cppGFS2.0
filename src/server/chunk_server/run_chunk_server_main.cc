#include <memory>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "grpcpp/grpcpp.h"
#include "src/common/config_manager.h"
#include "src/common/system_logger.h"
#include "src/server/chunk_server/chunk_server_control_service_impl.h"
#include "src/server/chunk_server/chunk_server_file_service_impl.h"
#include "src/server/chunk_server/chunk_server_impl.h"
#include "src/server/chunk_server/chunk_server_lease_service_impl.h"
#include "src/server/chunk_server/file_chunk_manager.h"

using gfs::common::ConfigManager;
using gfs::server::ChunkServerImpl;
using gfs::server::FileChunkManager;
using gfs::service::ChunkServerControlServiceImpl;
using gfs::service::ChunkServerFileServiceImpl;
using gfs::service::ChunkServerLeaseServiceImpl;
using google::protobuf::util::StatusOr;
using grpc::Server;
using grpc::ServerBuilder;

ABSL_FLAG(std::string, config_path, "data/config.yml", "/path/to/config.yml");
ABSL_FLAG(std::string, chunk_server_name, "chunk_server_01",
          "run as the given chunk server, as defined in the config");
ABSL_FLAG(bool, use_docker_dns_server, false, "use docker's DNS server");

int main(int argc, char** argv) {
  gfs::common::SystemLogger::GetInstance().Initialize(/*program_name=*/argv[0]);

  // Parse command line arguments
  absl::ParseCommandLine(argc, argv);
  const std::string config_path = absl::GetFlag(FLAGS_config_path);
  const std::string chunk_server_name = absl::GetFlag(FLAGS_chunk_server_name);
  const bool resolve_hostname = !absl::GetFlag(FLAGS_use_docker_dns_server);

  // Initialize configurations
  LOG(INFO) << "Reading GFS configuration: " << config_path;
  ConfigManager* config = ConfigManager::GetConfig(config_path).ValueOrDie();
  if (!config->HasChunkServer(chunk_server_name)) {
    LOG(ERROR) << "No chunk server found in config: " << chunk_server_name;
    return 1;
  }

  LOG(INFO) << "Running as chunk server: " << chunk_server_name;
  LOG(INFO) << "Server starting...";

  // Initialize the file chunk manager
  auto chunk_database_name = config->GetDatabaseName(chunk_server_name);
  auto max_chunk_size_bytes = config->GetFileChunkBlockSize() * 1024 * 1024;
  FileChunkManager::GetInstance()->Initialize(chunk_database_name,
                                              max_chunk_size_bytes);
  LOG(INFO) << "File chunk manager initialized with chunk database: "
            << chunk_database_name;

  ServerBuilder builder;

  std::string server_address(
      config->GetServerAddress(chunk_server_name, resolve_hostname));

  // Listen on the given address without any authentication mechanism for now.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

  // Chunkserver needs to accept large data and needs to set max message size
  // as the default is 4MB. We all an additional 1000 bytes as the message may
  // contain metadata on top of payload
  builder.SetMaxReceiveMessageSize(config->GetFileChunkBlockSize() * 
                                       gfs::common::bytesPerMb + 1000);

  // Chunk Server implementation
  LOG(INFO) << "Initializing main chunk server...";
  StatusOr<ChunkServerImpl*> chunk_server_impl_or =
      ChunkServerImpl::ConstructChunkServerImpl(config_path, chunk_server_name,
                                                resolve_hostname);

  if (!chunk_server_impl_or.ok()) {
    LOG(ERROR) << "Failed to create a chunk server: "
               << chunk_server_impl_or.status();
    return 1;
  }
  ChunkServerImpl* chunk_server_impl = chunk_server_impl_or.ValueOrDie();
  LOG(INFO) << "Chunk server initialized...";

  // Initialize gRPC protocol clients for talking to the master server(s) that
  // should be available upon GFS cluster's startup time, based on initial
  // config file.
  for (std::string& master_server_name : config->GetAllMasterServers()) {
    const std::string master_server_address =
        config->GetServerAddress(master_server_name, resolve_hostname);

    LOG(INFO) << "Initializing gRPC protocol client for talking to "
              << master_server_name << " at " << master_server_address;

    chunk_server_impl->RegisterMasterProtocolClient(master_server_address);
  }

  // Start report chunks to the master periodically, this chunkserver should 
  // report itself to the master server(s). This will enable the master be aware
  // of this chunkserver, and to start selecting it for chunk allocation. This 
  // also allows chunk servers to be dynamically added since they just need to 
  // report themselves to master.
  chunk_server_impl->StartReportToMaster();

  // Register synchronous services for handling clients' metadata requests
  // Note that gRPC only support providing services through via a single port.
  ChunkServerLeaseServiceImpl lease_service(chunk_server_impl);
  builder.RegisterService(&lease_service);

  ChunkServerFileServiceImpl file_service(chunk_server_impl);
  builder.RegisterService(&file_service);

  ChunkServerControlServiceImpl control_service(chunk_server_impl);
  builder.RegisterService(&control_service);

  // Assemble and start the server
  std::unique_ptr<Server> server(builder.BuildAndStart());
  LOG(INFO) << "Server listening on " << server_address;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();

  // Terminate the reporting service
  chunk_server_impl->TerminateReportToMaster();

  return 0;
}
