#include "src/server/master_server/chunk_server_manager.h"

namespace gfs {

namespace server {

// Iterates through the chunk servers and selects the server with
// the max available memory.
// TODO(bmokutub): optimize this, maybe use a sorted map.
std::shared_ptr<protos::ChunkServer> ChunkServerManager::AllocateChunkServer() {
  auto servers_iterator = this->chunk_servers_map_.begin();

  std::shared_ptr<protos::ChunkServer> max_memory_server = nullptr;
  uint32_t max_memory = 0;

  while (servers_iterator != this->chunk_servers_map_.end()) {
    auto current_server = servers_iterator->second;
    uint32_t current_server_memory = current_server->available_memory_mb();

    if (current_server_memory > max_memory) {
      max_memory_server = current_server;
      max_memory = current_server_memory;
    }

    servers_iterator++;
  }

  // TODO(bmokutub): check if this memory is enough for a chunk
  // Or when updating server info, it can be set to zero if not enough?
  // Also consider temporarily reducing the available memory to prevent it from
  // being selected always. Will be solved once we start updating the available
  // memory from heartbeat info.
  return max_memory_server;
}

bool ChunkServerManager::RegisterChunkServer(
    const std::shared_ptr<protos::ChunkServer> chunk_server) {
  auto result =
      this->chunk_servers_map_.insert({chunk_server->location(), chunk_server});

  return result.second;
}

void ChunkServerManager::UnRegisterChunkServer(
    const protos::ChunkServerLocation& server_location) {
  this->chunk_servers_map_.erase(server_location);
}

void ChunkServerManager::UnRegisterAllChunkServers() {
  this->chunk_servers_map_.clear();
}

std::shared_ptr<protos::ChunkServer> ChunkServerManager::GetChunkServer(
    const protos::ChunkServerLocation& server_location) {
  std::shared_ptr<protos::ChunkServer> server = nullptr;

  auto result = this->chunk_servers_map_.find(server_location);

  // Check if it was found
  if (result != this->chunk_servers_map_.end()) {
    server = result->second;
  }

  return server;
}

void ChunkServerManager::SyncChunkServers() {}
}  // namespace server
}  // namespace gfs