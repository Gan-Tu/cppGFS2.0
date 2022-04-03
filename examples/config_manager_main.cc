#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "src/common/config_manager.h"
#include "src/common/system_logger.h"

using gfs::common::ConfigManager;
using google::protobuf::util::StatusOr;

ABSL_FLAG(std::string, config_path, "data/config.yml",
          "/absolute/path/to/config.yml");

int main(int argc, char* argv[]) {
  absl::ParseCommandLine(argc, argv);
  gfs::common::SystemLogger::GetInstance().Initialize(/*program_name=*/argv[0]);

  ConfigManager* config =
      ConfigManager::GetConfig(absl::GetFlag(FLAGS_config_path)).value();

  for (std::string& server_name : config->GetAllMasterServers()) {
    LOG(INFO) << "Master server: " << server_name
              << ": (hostname=" << config->GetServerHostname(server_name)
              << ", port=" << config->GetServerPort(server_name)
              << ", server_address=" << config->GetServerAddress(server_name)
              << ", server_ip_address="
              << config->GetServerAddress(server_name,
                                          /*resolve_hostname=*/true)
              << ")";
  }

  for (std::string& server_name : config->GetAllChunkServers()) {
    LOG(INFO) << "Chunk server:  " << server_name
              << ": (hostname=" << config->GetServerHostname(server_name)
              << ", port=" << config->GetServerPort(server_name)
              << ", server_address=" << config->GetServerAddress(server_name)
              << ", server_ip_address="
              << config->GetServerAddress(server_name,
                                          /*resolve_hostname=*/true)
              << ")";
  }

  LOG(INFO) << "block size: " << config->GetFileChunkBlockSize() << "MB";
  LOG(INFO) << "min disk: " << config->GetRequiredDiskSpaceToMaintain() << "MB";
  LOG(INFO) << "grpc timeout: " << config->GetGrpcDeadline();
  LOG(INFO) << "lease timeout: " << config->GetWriteLeaseTimeout();
  LOG(INFO) << "heartbeat task periodic sleep duration: "
            << config->GetHeartBeatTaskSleepDuration();
  LOG(INFO) << "client cache timeout: " << config->GetClientCacheTimeout();

  return 0;
}