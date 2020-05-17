#ifndef GFS_COMMON_CONFIG_MANAGER_H_
#define GFS_COMMON_CONFIG_MANAGER_H_

#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "yaml-cpp/yaml.h"

namespace gfs {
namespace common {

struct ServerLocationMetadata {
  std::string hostname;
  std::string ip_address;
  uint32_t port;
};

class ConfigManager {
 public:
  ConfigManager() = default;

  // Private constructor callable only by factory method
  ConfigManager(YAML::Node& node) : config_(node) {}

  static ConfigManager* ParseConfigFile(const std::string& config_filename);

  std::vector<ServerLocationMetadata> GetMasterServerLocations();
  std::vector<ServerLocationMetadata> GetChunkServerLocations();

 private:
  // The parsed YAML configuration node
  YAML::Node config_;
};

}  // namespace common
}  // namespace gfs

#endif  // GFS_COMMON_CONFIG_MANAGER_H_
