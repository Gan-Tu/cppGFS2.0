#include "src/common/config_manager.h"

using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using google::protobuf::util::error::Code;

namespace gfs {
namespace common {

ConfigManager* ConfigManager::ParseConfigFile(
    const std::string& config_filename) {
  YAML::Node node;
  try {
    node = YAML::LoadFile(config_filename);
    return new ConfigManager(node);
  } catch (YAML::ParserException& e) {
    return NULL;
  } catch (YAML::BadFile& e) {
    return NULL;
  }
}

std::vector<ServerLocationMetadata> GetMasterServerLocations() {
  std::vector<ServerLocationMetadata> metadata;
  ServerLocationMetadata server1;
  server1.ip_address = "0.0.0.0";
  server1.hostname = "test";
  server1.port = 50021;
  metadata.push_back(server1);
  return metadata;
}

std::vector<ServerLocationMetadata> GetChunkServerLocations();

}  // namespace common
}  // namespace gfs
