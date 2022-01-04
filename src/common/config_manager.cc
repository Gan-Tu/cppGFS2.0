#include "src/common/config_manager.h"

#include <memory>

#include "absl/strings/str_cat.h"
#include "src/common/utils.h"

using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using StatusCode = google::protobuf::util::kCode;
using gfs::common::utils::ValidateConfigFile;

namespace gfs {
namespace common {

StatusOr<ConfigManager*> ConfigManager::GetConfig(
    const std::string& config_filename) {
  YAML::Node node;
  try {
    node = YAML::LoadFile(config_filename);
    Status validation_status = ValidateConfigFile(node);
    if (validation_status.ok()) {
      return new ConfigManager(node);
    } else {
      return validation_status;
    }
  } catch (YAML::ParserException& e) {
    return Status(StatusCode::INVALID_ARGUMENT, e.msg);
  } catch (YAML::BadFile& e) {
    return Status(StatusCode::INVALID_ARGUMENT, e.msg);
  }
}

ConfigManager::ConfigManager(const YAML::Node& node) {
  config_ = node;
  for (size_t i = 0; i < config_["servers"]["master_servers"].size(); i++) {
    master_servers_names_.insert(
        config_["servers"]["master_servers"][i].as<std::string>());
  }
  for (size_t i = 0; i < config_["servers"]["chunk_servers"].size(); i++) {
    chunk_servers_names_.insert(
        config_["servers"]["chunk_servers"][i].as<std::string>());
  }
}

std::vector<std::string> ConfigManager::GetAllMasterServers() {
  return std::vector<std::string>(master_servers_names_.begin(),
                                  master_servers_names_.end());
}

std::vector<std::string> ConfigManager::GetAllChunkServers() {
  return std::vector<std::string>(chunk_servers_names_.begin(),
                                  chunk_servers_names_.end());
}

bool ConfigManager::HasMasterServer(const std::string& server_name) {
  return master_servers_names_.contains(server_name);
}

bool ConfigManager::HasChunkServer(const std::string& server_name) {
  return chunk_servers_names_.contains(server_name);
}

std::string ConfigManager::GetServerHostname(const std::string& server_name) {
  return config_["network"][server_name]["hostname"].as<std::string>();
}

uint32_t ConfigManager::GetServerPort(const std::string& server_name) {
  return config_["network"][server_name]["port"].as<uint32_t>();
}

std::string ConfigManager::ResolveHostname(const std::string& hostname) {
  return config_["network"]["dns_lookup_table"][hostname].as<std::string>();
}

std::string ConfigManager::GetServerAddress(const std::string& server_name,
                                            const bool resolve_hostname) {
  std::string hostname = GetServerHostname(server_name);
  if (resolve_hostname) {
    return absl::StrCat(ResolveHostname(hostname), ":",
                        GetServerPort(server_name));
  } else {
    return absl::StrCat(hostname, ":", GetServerPort(server_name));
  }
}

std::string ConfigManager::GetDatabaseName(const std::string& server_name) {
  return config_["disk"]["leveldb"][server_name].as<std::string>();
}

uint64_t ConfigManager::GetFileChunkBlockSize() {
  return config_["disk"]["block_size_mb"].as<uint64_t>();
}

uint64_t ConfigManager::GetRequiredDiskSpaceToMaintain() {
  return config_["disk"]["min_free_disk_space_mb"].as<uint64_t>();
}

absl::Duration ConfigManager::GetGrpcDeadline() {
  absl::Duration deadline;
  absl::ParseDuration(config_["timeout"]["grpc"].as<std::string>(), &deadline);
  return deadline;
}

absl::Duration ConfigManager::GetWriteLeaseTimeout() {
  absl::Duration deadline;
  absl::ParseDuration(config_["timeout"]["lease"].as<std::string>(), &deadline);
  return deadline;
}

absl::Duration ConfigManager::GetHeartBeatTaskSleepDuration() {
  absl::Duration deadline;
  absl::ParseDuration(config_["timeout"]["heartbeat"].as<std::string>(),
                      &deadline);
  return deadline;
}

absl::Duration ConfigManager::GetClientCacheTimeout() {
  absl::Duration deadline;
  absl::ParseDuration(config_["timeout"]["client_cache"].as<std::string>(),
                      &deadline);
  return deadline;
}

}  // namespace common
}  // namespace gfs
