#ifndef GFS_SERVER_CHUNK_SERVER_CHUNK_DATA_CACHE_MANAGER_H_
#define GFS_SERVER_CHUNK_SERVER_CHUNK_DATA_CACHE_MANAGER_H_

#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "src/common/utils.h"

// Singleton class responsible for managing the data cache and retrival for
// file chunk data that sent from client to chunk servers. It employs thread-
// safe hashmap to store the key-value pairs, where the key is typically the
// checksum of the data and the value is a string. In order to keep the cache
// under a size limit, we employ a LRU eviction policy when the cache is at
// capacity.

namespace gfs {
namespace server {

class ChunkDataCacheManager {
 public:
  // Static method to access the singleton object
  static ChunkDataCacheManager* GetInstance();   

  // Get value for a key, return error if key not found
  google::protobuf::util::StatusOr<std::string> GetValue(
      const std::string& key);

  // Set value for a key, return error if this operation is unsuccessful. This 
  // can happen if this value is bigger than the capacity of the whole cache 
  // capacity. Otherwise, we will always set the value for this key. We evict 
  // as much as possible data to make this insertion as this happens as the 
  // latest operation
  google::protobuf::util::Status SetValue(const std::string& key, 
  				          const std::string& value);
 
  // Set cache capacity in bytes. This is only useful for testing as the
  // default value is very large
  void SetCacheCapacityLimit(const unsigned long cache_capacity_limit);

 private:
  // A capacity cap for this cache unit. Default value 4G
  // TODO(Xi): this value needs to be configurable. 
  unsigned long cache_capacity_limit_ = 4294967296;

  // Cache capacity in bytes
  std::atomic<unsigned long> cache_capacity_{0};

  // A thread-safe hashmap to store the key-value mapping
  common::parallel_hash_map<std::string, std::string> data_cache_;

  // A hash-set to store the history of keys that got inserted. This 
  // will be useful when eviction is needed. For simplicity, we will just
  // remove the first key in this set, and the reason of not using a 
  // thread-safe data structure for this one is that iterator usage becomes
  // tricky, we use a single lock to serialize operations
  absl::flat_hash_set<std::string> key_history_;

  // A lock to synchronize key_history
  absl::Mutex key_history_lock_; 
};

} // namespace server
} // namespace gfs

#endif // GFS_SERVER_CHUNK_SERVER_CHUNK_DATA_CACHE_MANAGER_H_
