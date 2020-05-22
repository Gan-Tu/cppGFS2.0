#include "src/client/client_cache_manager.h"

using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;

namespace gfs {
namespace client {

google::protobuf::util::StatusOr<std::string> CacheManager::GetChunkHandle(
    const std::string& filename, uint32_t chunk_index) const {
  if (!file_chunk_handle_.contains(filename)) {
    return Status(google::protobuf::util::error::NOT_FOUND,
                  "Filename does not exist: " + filename);  
  }

  if (!file_chunk_handle_.at(filename).contains(chunk_index)) {
    return Status(google::protobuf::util::error::NOT_FOUND,
                  "Chunk index " + std::to_string(chunk_index) +  
                  " does not exist in file: " + filename);  
  }
  
  return file_chunk_handle_.at(filename).at(chunk_index);
}

google::protobuf::util::Status CacheManager::SetChunkHandle(
    const std::string& filename, uint32_t chunk_index, 
    const std::string& chunk_handle) {
  auto original_chunk_or(GetChunkHandle(filename, chunk_index));
  if (original_chunk_or.ok()) { 
    // If there already exists a mapping for this but we change the 
    // chunk_handle, something is wrong because master assigns unique
    // chunk_hanle. (We can extend / relax this rule if we decide to 
    // support the case of delete-then-create a file).  
    if (original_chunk_or.ValueOrDie() != chunk_handle) {
      return Status(google::protobuf::util::error::INVALID_ARGUMENT,
                 "Reassigning a chunk handle to " + filename + 
                 "chunk_index " + std::to_string(chunk_index) + 
                 "not allowed");
    }
    return google::protobuf::util::Status::OK;
  }

  file_chunk_handle_[filename][chunk_index] = chunk_handle;
  valid_chunk_handle_.insert(chunk_handle);
  return google::protobuf::util::Status::OK;
}

google::protobuf::util::StatusOr<uint32_t> CacheManager::GetChunkVersion(
    const std::string& chunk_handle) const {
  if (!chunk_handle_version_.contains(chunk_handle)) {
    return Status(google::protobuf::util::error::NOT_FOUND,
                  "Chunk handle not found: " + chunk_handle);
  }
  return chunk_handle_version_.at(chunk_handle);
}

google::protobuf::util::Status CacheManager::SetChunkVersion(
    const std::string& chunk_handle, uint32_t version) {
  if (!valid_chunk_handle_.contains(chunk_handle)) {
    return Status(google::protobuf::util::error::INVALID_ARGUMENT,
                  "Invalid chunk handle " + chunk_handle);
  }
  chunk_handle_version_[chunk_handle] = version; 
  return google::protobuf::util::Status::OK;
}

google::protobuf::util::StatusOr<CacheManager::ChunkServerLocationEntry>
    CacheManager::GetChunkServerLocation(
        const std::string& chunk_handle) const {
  if (!chunk_server_location_.contains(chunk_handle)) {
     return Status(google::protobuf::util::error::NOT_FOUND,
                  "Chunk handle not found: " + chunk_handle);
  }
 
  auto entry(chunk_server_location_.at(chunk_handle));
  const absl::Time entryTimestamp(entry.timestamp);
  const absl::Time now(absl::Now()); 

  // If this entry timesout, we return error. The application is supposed
  // to refresh the chunk server location for this chunk by contacting
  // the master again. 
  if (now - entryTimestamp > timeout_) {
    return Status(google::protobuf::util::error::DEADLINE_EXCEEDED,
                  "Chunk server location info for " + chunk_handle +
                  "timed out");
  }

  return chunk_server_location_.at(chunk_handle);
}

google::protobuf::util::Status CacheManager::SetChunkServerLocation(
    const std::string& chunk_handle, 
    const CacheManager::ChunkServerLocationEntry& entry) {
  if (!valid_chunk_handle_.contains(chunk_handle)) {
    return Status(google::protobuf::util::error::INVALID_ARGUMENT,
                  "Invalid chunk handle " + chunk_handle);
  }
  chunk_server_location_[chunk_handle] = entry;
  return google::protobuf::util::Status::OK;
}

CacheManager* CacheManager::ConstructCacheManager(
    const absl::Duration timeout) {
  return new CacheManager(timeout);
}

} // namespace client
} // namespace gfs
