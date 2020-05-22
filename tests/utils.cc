#include "tests/utils.h"

namespace tests {

void JoinAndClearThreads(std::vector<std::thread>& threads) {
  for (auto& t : threads) {
    t.join();
  }
  threads.clear();
}

std::string ComputeNestedFileName(const std::string& base, 
                                  const int nested_level) {
  std::string filename;
  for (int i = 0; i < nested_level; i++) {
    filename += ("/" + base + std::to_string(i));
  }
  return filename;
}

protos::ChunkServerLocation ChunkServerLocationBuilder(
    const std::string& hostname, const uint32_t port) {
  protos::ChunkServerLocation location;
  location.set_server_hostname(hostname);
  location.set_server_port(port);
  return location; 
}

void InitializeChunkMetadata(
         protos::FileChunkMetadata& chunk_metadata, 
         const std::string& chunk_handle, uint32_t version,
         const std::pair<std::string, uint32_t>& primary_location) {
  chunk_metadata.set_chunk_handle(chunk_handle);
  chunk_metadata.set_version(version);
  *chunk_metadata.mutable_primary_location() =
      ChunkServerLocationBuilder(primary_location.first, 
                                 primary_location.second);
}

} // namespace tests
