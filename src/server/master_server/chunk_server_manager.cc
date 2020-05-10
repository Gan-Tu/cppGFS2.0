#include "chunk_server_manager.h"

namespace gfs {

// Iterates through the chunk servers and selects the server with
// the max available memory.
std::shared_ptr<ChunkServer> ChunkServerManager::GetAvailableChunkServer() {
  auto servers_iterator = this->chunk_servers_map_.begin();

  std::shared_ptr<ChunkServer> max_memory_server = nullptr;
  uint32_t max_memory = 0;

  while (servers_iterator != this->chunk_servers_map_.end()) {
    auto current_server = servers_iterator->second;
    uint32_t current_server_memory = current_server->GetAvailableMemoryMb();

    if (current_server_memory > max_memory) {
      max_memory_server = current_server;
      max_memory = current_server_memory;
    }

    servers_iterator++;
  }

  // TODO(bmokutub): check if this memory is enough for a chunk
  // Or when updating server info, it can be set to zero if not enough?
  return max_memory_server;
}

bool ChunkServerManager::RegisterChunkServer(
    const std::shared_ptr<ChunkServer> chunk_server) {
  auto result =
      this->chunk_servers_map_.insert({chunk_server->GetKey(), chunk_server});

  return result.second;
}

void ChunkServerManager::UnRegisterChunkServer(
    const ChunkServerKey& server_key) {
  this->chunk_servers_map_.erase(server_key);
}

void ChunkServerManager::UnRegisterAllChunkServers() {
  this->chunk_servers_map_.clear();
}

std::shared_ptr<ChunkServer> ChunkServerManager::GetChunkServer(
    const ChunkServerKey& server_key) {
  std::shared_ptr<ChunkServer> server = nullptr;

  auto result = this->chunk_servers_map_.find(server_key);

  // Check if it was found
  if (result != this->chunk_servers_map_.end()) {
    server = result->second;
  }

  return server;
}

void ChunkServerManager::SyncChunkServers() {}
}  // namespace gfs