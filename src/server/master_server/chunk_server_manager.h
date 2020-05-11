#ifndef GFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H_
#define GFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H_

#include <functional>
#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "src/protos/chunk_server.pb.h"

// Define how we do equality check for the ChunkServerLocation and ChunkServer
// TODO(bmokutub): Can move to a separate header file e.g. chunk_server.h
namespace protos {
bool operator==(const protos::ChunkServerLocation& lhs,
                const protos::ChunkServerLocation& rhs) {
  return lhs.server_hostname() == rhs.server_hostname() &&
         lhs.server_port() == rhs.server_port();
}

bool operator==(const protos::ChunkServer& lhs,
                const protos::ChunkServer& rhs) {
  return lhs.location() == rhs.location();
}
}  // namespace protos

namespace gfs {

namespace server {

// For generating the hash value of a ChunkServerLocation.
// A simple implementation.
class ChunkServerLocationHash {
 public:
  std::size_t operator()(const protos::ChunkServerLocation& location) const
      noexcept {
    std::size_t h1 = std::hash<std::string>{}(location.server_hostname());
    std::size_t h2 = std::hash<uint32_t>{}(location.server_port());
    return h1 ^ (h2 << 1);
  }
};

// Singleton class for managing all the chunk servers. Selects the chunk server
// to store a chunk. Tries to balance the load on the servers.
class ChunkServerManager {
 public:
  static ChunkServerManager& GetInstance() {
    static ChunkServerManager instance;

    return instance;
  }

  // To prevent copying the ChunkServerManager
  ChunkServerManager(const ChunkServerManager&) = delete;

  void operator=(const ChunkServerManager&) = delete;

  // Allocates a ChunkServer to store a chunk.
  // Returns nullptr if there's no available ChunkServer.
  std::shared_ptr<protos::ChunkServer> AllocateChunkServer();

  // Register the ChunkServer with the manager.
  // Manager can now decide to select it for chunk storage.
  // Returns false if ChunkServer is already registered.
  bool RegisterChunkServer(
      const std::shared_ptr<protos::ChunkServer> chunk_server);

  // Unregister the ChunkServer with the manager.
  // Manager no longer knows about this server and won't be selected for chunk
  // storage.
  void UnRegisterChunkServer(
      const protos::ChunkServerLocation& server_location);

  // Unregister all the ChunkServers.
  // There would be no ChunkServer for chunk storage until they are registered.
  void UnRegisterAllChunkServers();

  // Returns the ChunkServer for the specified location. Returns nullptr if not
  // registered.
  std::shared_ptr<protos::ChunkServer> GetChunkServer(
      const protos::ChunkServerLocation& server_location);

  // TODO(bmokutub): Use the heartbeat to update ChunkServers info.
  void SyncChunkServers();

 private:
  // TODO(bmokutub): Consider a thread safe map or locking this.
  // May not need to use shared_ptr since proto manage the memory?
  absl::flat_hash_map<protos::ChunkServerLocation,
                      std::shared_ptr<protos::ChunkServer>,
                      ChunkServerLocationHash>
      chunk_servers_map_;

  ChunkServerManager() = default;
};
}  // namespace server
}  // namespace gfs

#endif  // GFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H_