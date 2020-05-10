#ifndef GFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H_
#define GFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H_

#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"

namespace gfs {

// The unique identifier for a chunk server
class ChunkServerKey {
  // To make ChunkServerKey hashable, for use in absl hash map.
  template <typename H>
  friend H AbslHashValue(H hash_state, const ChunkServerKey& key);

 public:
  ChunkServerKey(const std::string& host_name, const std::string& port)
      : host_name_(host_name), port_(port) {}

  bool operator==(const ChunkServerKey& other) const {
    return host_name_ == other.host_name_ && port_ == other.port_;
  }

  std::string GetHostName() const { return host_name_; }

  std::string GetPort() const { return port_; }

 private:
  const std::string host_name_;

  const std::string port_;
};

template <typename H>
H AbslHashValue(H hash_state, const ChunkServerKey& key) {
  return H::combine(std::move(hash_state), key.host_name_, key.port_);
}

class ChunkServer {
 public:
  ChunkServer(const ChunkServerKey& server_key)
      : server_key_(server_key), available_memory_mb_(0) {}

  const ChunkServerKey& GetKey() const { return server_key_; }

  uint32_t GetAvailableMemoryMb() const { return available_memory_mb_; }

  void SetAvailableMemoryMb(const uint32_t& memory_mb) {
    available_memory_mb_ = memory_mb;
  }

  bool operator==(const ChunkServer& other) const {
    return server_key_ == other.server_key_;
  }

 private:
  const ChunkServerKey server_key_;

  // Amount of memory in megabytes available on the server
  uint32_t available_memory_mb_;
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

  // Get a ChunkServer that is available to store a chunk.
  // Returns nullptr if there's no available ChunkServer.
  std::shared_ptr<ChunkServer> GetAvailableChunkServer();

  // Register the ChunkServer with the manager.
  // Manager can now decide to select it for chunk storage.
  // Returns false if ChunkServer is already registered.
  bool RegisterChunkServer(const std::shared_ptr<ChunkServer> chunk_server);

  // Unregister the ChunkServer with the manager.
  // Manager no longer knows about this server and won't be selected for chunk
  // storage.
  void UnRegisterChunkServer(const ChunkServerKey& server_key);

  // Unregister all the ChunkServers.
  // There would be no ChunkServer for chunk storage until they are registered.
  void UnRegisterAllChunkServers();

  // Returns the ChunkServer for the specified key. Returns nullptr if not
  // registered.
  std::shared_ptr<ChunkServer> GetChunkServer(const ChunkServerKey& server_key);

  // TODO(bmokutub): Use the heartbeat to update ChunkServers info.
  void SyncChunkServers();

 private:
  absl::flat_hash_map<ChunkServerKey, std::shared_ptr<ChunkServer>>
      chunk_servers_map_;

  ChunkServerManager() = default;
};
}  // namespace gfs

#endif  // GFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H_