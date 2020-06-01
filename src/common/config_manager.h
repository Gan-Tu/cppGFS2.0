#ifndef GFS_COMMON_CONFIG_MANAGER_H_
#define GFS_COMMON_CONFIG_MANAGER_H_

#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/time/time.h"
#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "yaml-cpp/yaml.h"

namespace gfs {
namespace common {

class ConfigManager {
 public:
  // Return an initialized configuration manager by reading the YAML
  // configuration |config_filename|; otherwise, return an error status
  // if the parsing failed, or any configuration file validation errors
  static google::protobuf::util::StatusOr<ConfigManager*> GetConfig(
      const std::string& config_filename);

  std::vector<std::string> GetAllMasterServers();
  std::vector<std::string> GetAllChunkServers();

  bool HasMasterServer(const std::string& server_name);
  bool HasChunkServer(const std::string& server_name);

  // Return the hostname for the given server
  std::string GetServerHostname(const std::string& server_name);

  // Return the port that the given server is listening on
  uint32_t GetServerPort(const std::string& server_name);

  // Return <hostname>:<port> if |resolve_hostname| is false; otherwise, resolve
  // the hostname to a given IP using config's DNS lookup table, and return
  // <ip_address>:<port> instead. Note that, if using Docker's container
  // networking module, which comes with default DNS resolver, you should NOT
  // set |resolve_hostname|: https://docs.docker.com/conf
  std::string GetServerAddress(const std::string& server_name,
                               const bool resolve_hostname = false);

  // Resovle a hostname to an IP, using provided DNS lookup table in config
  std::string ResolveHostname(const std::string& hostname);

  // Return the leveldb database name for the given chunk server
  std::string GetDatabaseName(const std::string& server_name);

  // Return the size of each file chunk/block in MB
  uint64_t GetFileChunkBlockSize();

  // Return the minimum disk space each chunk server needs to maintain in MB
  uint64_t GetRequiredDiskSpaceToMaintain();

  // Return the deadline, after when a grpc request should timeout
  absl::Duration GetGrpcDeadline();

  // Return the default timeout for each write lease granted
  absl::Duration GetWriteLeaseTimeout();

  // Return the max deadline allowed between heart beat messages, after when
  // the server will be considered dead
  absl::Duration GetHeartBeatDeadline();

  // Return the timeout, after when the client side file metadata cache expires
  absl::Duration GetClientCacheTimeout();

 private:
  ConfigManager() = default;
  ConfigManager(const YAML::Node& node);

  // The parsed YAML configuration node
  YAML::Node config_;

  // Available servers
  absl::flat_hash_set<std::string> master_servers_names_;
  absl::flat_hash_set<std::string> chunk_servers_names_;
};

}  // namespace common
}  // namespace gfs

#endif  // GFS_COMMON_CONFIG_MANAGER_H_
