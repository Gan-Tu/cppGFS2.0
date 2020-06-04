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

google::protobuf::util::Status ChunkDataCacheManager::SetValue(
    const std::string& key, const std::string& value) {
  size_t data_size(value.size());

  // The very corner case that a huge value singly exceeds cache capcity do not
  // expect to have in this project
  if (data_size > cache_capacity_limit_) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::OUT_OF_RANGE,
        "Value exceeds cache capacity");
  }

  // The eviction case as the we reach capacity. Note that for simplicity we do
  // not consider all the crazy concurrent scheme that can happen here
  while (cache_capacity_.load() + data_size > cache_capacity_limit_) {
    // Lock the key_history_ set and evict the data that corresponds to the 
    // first key there
    absl::WriterMutexLock lock_guard(&key_history_lock_);
    // In this case the key_history_ cannot be empty so begin exists
    auto key_to_evict(*key_history_.begin());
    key_history_.erase(key_history_.begin());
    // We are under a single lock, so directly accessing / deleting is safe
    size_t value_to_evict_len(data_cache_[key_to_evict].size());
    data_cache_.erase(key_to_evict);
    cache_capacity_ -= value_to_evict_len;
  }
  
  // Set the value and update the cache capacity
  data_cache_.SetValue(key, value);
  return google::protobuf::util::Status::OK;
}

// Note that this function is only used by test, and is not thread-safe
// for simplicity
void ChunkDataCacheManager::SetCacheCapacityLimit(
    const unsigned long cache_capacity_limit) {
  cache_capacity_limit_ = cache_capacity_limit;
}

ChunkDataCacheManager* GetInstance() {
  static ChunkDataCacheManager* chunk_data_cache_manager 
      = new ChunkDataCacheManager();
  return chunk_data_cache_manager;
}

} // namespace server
} // namespace gfs
