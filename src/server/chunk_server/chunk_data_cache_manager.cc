#include "src/server/chunk_server/chunk_data_cache_manager.h"

namespace gfs {
namespace server {

google::protobuf::util::StatusOr<std::string> ChunkDataCacheManager::GetValue(
    const std::string& key) {
  auto try_get_value(data_cache_.TryGetValue(key));
  if (!try_get_value.second) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::NOT_FOUND,
        "Value not found for key: " + key);
  }
  return try_get_value.first;
}

void ChunkDataCacheManager::SetValue(const std::string& key, 
    const std::string& value) {
  data_cache_.SetValue(key, value);
}

void ChunkDataCacheManager::RemoveValue(const std::string& key) {
  data_cache_.Erase(key);
}

ChunkDataCacheManager* ChunkDataCacheManager::GetInstance() {
  static ChunkDataCacheManager* chunk_data_cache_manager 
      = new ChunkDataCacheManager();
  return chunk_data_cache_manager;
}

} // namespace server
} // namespace gfs
