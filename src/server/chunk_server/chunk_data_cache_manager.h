#ifndef GFS_SERVER_CHUNK_SERVER_CHUNK_DATA_CACHE_MANAGER_H_
#define GFS_SERVER_CHUNK_SERVER_CHUNK_DATA_CACHE_MANAGER_H_

#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "src/common/utils.h"

// Singleton class responsible for managing the data cache and retrival for
// file chunk data that sent from client to chunk servers. It employs thread-
// safe hashmap to store the key-value pairs, where the key is typically the
// checksum of the data and the value is a string. For simplicity, we let the 
// caller of this module to remove items (this can happen typically after a 
// write operation is successfully executed). 
// 
// Example: use ChunkDataCacheManager::GetInstance()->GetValue(key) to get the
// value for a key. 
//
// Use ChunkDataCacheManager::GetInstance()->SetValue(key, value) to set the
// value for a key.
//
// Use ChunkDataCacheManager::GetInstance()->RemoveValue(key) to remove the 
// value for a key. 
//
// Remember to check error. 

namespace gfs {
namespace server {

class ChunkDataCacheManager {
 public:
  // Static method to access the singleton object
  static ChunkDataCacheManager* GetInstance();   

  // Get value for a key, return error if key not found
  google::protobuf::util::StatusOr<std::string> GetValue(
      const std::string& key);

  // Set value for a key
  void SetValue(const std::string& key, const std::string& value);

  // Remove value for a key, regardless of the existence of this key
  void RemoveValue(const std::string& key);

 private:
  // A thread-safe hashmap to store the key-value mapping
  common::parallel_hash_map<std::string, std::string> data_cache_;
};

} // namespace server
} // namespace gfs

#endif // GFS_SERVER_CHUNK_SERVER_CHUNK_DATA_CACHE_MANAGER_H_
