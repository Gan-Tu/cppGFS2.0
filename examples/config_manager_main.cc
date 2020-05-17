#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "src/common/config_manager.h"
#include "src/common/system_logger.h"

#include <iostream>

using gfs::common::ConfigManager;
using google::protobuf::util::StatusOr;

ABSL_FLAG(std::string, config_path, "./examples/config.yaml",
          "/absolute/path/to/config.yaml");
ABSL_FLAG(bool, use_docker_hostname, false,
          "If true, use docker_hostname instead of each server's IP in "
          "configuration file, for setting up network connections.");

int main(int argc, char* argv[]) {
  //absl::ParseCommandLine(argc, argv);
  //gfs::common::SystemLogger::GetInstance().Initialize(/*program_name=*/argv[0]);

  std::string config_filename = absl::GetFlag(FLAGS_config_path);

  // LOG(INFO) << "hello";
  // StatusOr<ConfigManager> config =
  //     ConfigManager::ParseConfigFile(config_filename);
  // LOG(INFO) << "Parse config file: " << config.ok();
  // if (config.ok()) {
  //   LOG(INFO) << config.ValueOrDie().GetMasterServerLocations();
  // }

  auto config = ConfigManager::ParseConfigFile(config_filename);
  // // LOG(INFO) << config->GetMasterServerLocations()[0];
  // for(gfs::common::ServerLocationMetadata& x: config->GetMasterServerLocations()) {
  //   std::cout << x.hostname;
  // }

  // std::string filename = absl::GetFlag(FLAGS_config_file);
  // LOG(INFO) << filename;

  // YAML::Node config;
  // try {
  //   config = YAML::LoadFile(filename);
  // } catch (YAML::ParserException e) {
  //   LOG(ERROR) << "Caught YAML::ParserException: " << e.msg;
  //   return 1;
  // } catch (YAML::BadFile e) {
  //   LOG(ERROR) << "Caught YAML::BadFile exception: " << e.msg;
  //   return 2;
  // }

  // LOG(INFO) << "master servers: " << config["network"]["master_servers"];
  // LOG(INFO) << "chunk servers: " << config["network"]["chunk_servers"];

  // LOG(INFO) << "disk: " << config["disk"];
  // LOG(INFO) << "timeout: " << config["timeout"];

  return 0;
}