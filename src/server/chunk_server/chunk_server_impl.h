#ifndef GFS_SERVER_CHUNK_SERVER_IMPL_H_
#define GFS_SERVER_CHUNK_SERVER_IMPL_H_

#include "absl/container/flat_hash_map.h"
#include "absl/time/time.h"
#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "src/common/config_manager.h"
#include "src/common/protocol_client/chunk_server_service_server_client.h"
#include "src/common/protocol_client/master_chunk_server_manager_service_client.h"
#include "src/common/utils.h"
#include "src/server/chunk_server/file_chunk_manager.h"

namespace gfs {
namespace server {

class ChunkServerImpl {
 public:
  static google::protobuf::util::StatusOr<ChunkServerImpl*>
  ConstructChunkServerImpl(const std::string& config_filename,
                           const std::string& chunk_server_name,
                           const bool resolve_hostname = false);

  // Add or update the write lease for |file_handle| to expire at UNIX seconds
  // timestamp of |expiration_unix_sec|.
  void AddOrUpdateLease(const std::string& file_handle,
                        const uint64_t expiration_unix_sec);
  void RemoveLease(const std::string& file_handle);
  bool HasWriteLease(const std::string& file_handle);
  // Return NOT_FOUND, if no lease exists for file handle
  google::protobuf::util::StatusOr<absl::Time> GetLeaseExpirationTime(
      const std::string& file_handle);

  // Returns the stored version of the specified chunk handle.
  // If the handle doesn't exist on the chunk server, return NOT_FOUND error.
  // If there are data corruption on the chunk server that it is not able to
  // determine the chunk version, return INTERNAL error.
  google::protobuf::util::StatusOr<uint32_t> GetChunkVersion(
      const std::string& file_handle);

  // Return the protocol client for talking to the master at |server_address|.
  // If the connection is already established, reuse the connection.
  // Otherwise, initialize and return a new protocol client connecting to
  // |server_address|.
  std::shared_ptr<gfs::service::MasterChunkServerManagerServiceClient>
  GetMasterProtocolClient(const std::string& server_address);

  // Register (create) a protocol client for talking to the master at
  // |server_address|. Overwrite any existing connection.
  void RegisterMasterProtocolClient(const std::string& server_address);

  // Similar to GetMasterProtocolClient, but for talking to chunk servers.
  std::shared_ptr<gfs::service::ChunkServerServiceChunkServerClient>
  GetChunkServerProtocolClient(const std::string& server_address);

  // Report this chunkserver to master server. This is useful during chunk
  // server startup to inform the master, that it is now available, report the
  // stored chunks and available disk. This enables the master to start issuing
  // chunk allocations to this server. This functionality enables chunkservers
  // to be dynamically added to the cluster, they just need to report themselves
  // and the master will become aware of them and start issuing chunk
  // allocations to them. Returns true if successful and false otherwise.
  bool ReportToMaster(const uint64_t initial_disk_space_mb);

  // Get the configuration manager used by the chunkserver
  gfs::common::ConfigManager* GetConfigManager() const;

  // Check if resolving hostname is enabled
  bool ResolveHostname() const;

 private:
  ChunkServerImpl() = default;

  ChunkServerImpl(gfs::common::ConfigManager* config_manager,
                  const std::string& chunk_server_name,
                  const bool resolve_hostname)
      : config_manager_(config_manager),
        chunk_server_name_(chunk_server_name),
        resolve_hostname_(resolve_hostname) {}

  gfs::common::ConfigManager* config_manager_ = nullptr;
  FileChunkManager* file_manager_ = FileChunkManager::GetInstance();
  std::string chunk_server_name_ = nullptr;
  bool resolve_hostname_ = false;

  ~ChunkServerImpl();

  // Server address and its corresponding GFS protocol client
  // A protocol client will be added the first time the connection is added, and
  // subsequent calls will simply reuse this protocol client and connection.
  // Currently we don't remove connections no longer in use, for simplicity.
  //
  // Note that this design makes it possible to dynamically add new connections
  // to new servers that may not be present during startup configuration.
  gfs::common::thread_safe_flat_hash_map<
      std::string,
      std::shared_ptr<gfs::service::MasterChunkServerManagerServiceClient>>
      master_server_clients_;
  gfs::common::thread_safe_flat_hash_map<
      std::string,
      std::shared_ptr<gfs::service::ChunkServerServiceChunkServerClient>>
      chunk_server_clients_;

  // File handles and its version
  // TODO(tugan,michael): use chunk file manager instead, when ready
  gfs::common::thread_safe_flat_hash_map<std::string, uint32_t> chunk_versions_;

  // Write leases that chunk server holds, and their respective expiration time
  gfs::common::thread_safe_flat_hash_map<std::string, uint64_t>
      lease_and_expiration_unix_sec_;
};

}  // namespace server
}  // namespace gfs

#endif  // GFS_SERVER_CHUNK_SERVER_IMPL_H_
