#include "src/server/master_server/chunk_server_manager.h"

#include "absl/strings/str_cat.h"
#include "src/common/system_logger.h"

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

ChunkServerLocationThreadSafeFlatSet ChunkServerManager::AllocateChunkServer(
    const std::string& chunk_handle, const ushort& requested_servers_count) {
  // Check if we have allocated servers for this previously
  if (this->chunk_locations_map_.contains(chunk_handle)) {
    // Chunk handle has been allocated previously.
    // Return the previously allocated locations.
    LOG(INFO) << "Chunk servers have been previously allocated for chunk: "
              << chunk_handle;
    return this->chunk_locations_map_[chunk_handle];
  }

  ChunkServerLocationThreadSafeFlatSet allocated_locations;

  // Lock due to concurrent allocations. Acquire priority list lock,
  // only one op on priority list at a time.
  // TODO(bmokutub): Consider using read write lock for the priority list to
  // allow doing concurrent (interleaving) allocations. Will take read lock when
  // iterating the list, then drop it. Then take write lock for sorting. But
  // that may delay the allocation until readers release in a highly concurrent
  // situation.
  absl::MutexLock chunk_servers_priority_list_lock_guard(
      &chunk_servers_priority_list_lock_);

  LOG(INFO) << "Allocating chunk servers for chunk: " << chunk_handle;

  ushort allocated_servers_count = 0;
  // Since the list is sorted and servers with max available disk are in front,
  // we just pick them for allocation.
  for (auto iterator = this->chunk_servers_priority_list_.cbegin();
       iterator != this->chunk_servers_priority_list_.cend() &&
       allocated_servers_count < requested_servers_count;
       ++iterator, ++allocated_servers_count) {
    auto chunk_server = *iterator;

    // write lock on the chunkserver since we are updating it
    // It is safe to call location of the chunk_server, since it is never
    // updated during the life time of the chunkserver object. Taking a write
    // lock here, so the available disk doesn't change after our first read and
    // before the update.
    absl::WriterMutexLock chunk_server_write_lock_guard(
        this->chunk_servers_lock_map_[chunk_server->location()].get());

    // Reduce the available disk by chunk size
    // TODO(bmokutub): Use the config manager to get chunk size.
    int32_t new_available_disk = chunk_server->available_disk_mb() - 64;
    // Replace 0 with min_disk_to_maintain
    // TODO(bmokutub): Use the config manager to get min_disk_to_maintain.
    if (new_available_disk <= 0) {
      // stop iterating, won't find any chunk server with larger disk since list
      // is sorted by available disk in descending order
      break;
    }

    allocated_locations.insert(chunk_server->location());

    // Update the chunk server available disk
    chunk_server->set_available_disk_mb(new_available_disk);
    // add this chunk handle to the list of stored chunks
    chunk_server->add_stored_chunk_handles(chunk_handle);

    LOG(INFO) << "Allocated chunk server "
              << CreateChunkServerLocationString(chunk_server->location())
              << " (new available disk= " << new_available_disk << "mb)"
              << " for storing chunk " << chunk_handle;
  }

  if (allocated_servers_count > 0) {
    // We did some allocations and changed some servers availabe disk. Lets
    // reorder them since we always keep the list sorted.
    // We still have the priority list lock here.
    // Worst case this will be O(Nlog(N)), but the assumption here is that since
    // the list is mostly sorted and only requested_servers_count servers are
    // out of place, may be faster most times as long as it is little compared
    // to the number of servers.
    // TODO(bmokutub): Consider doing the sorting asynchronously. Fire a sorting
    // task here, to do it in background. Since this is in the hot path (client
    // request path).
    this->chunk_servers_priority_list_.sort(
        ChunkServerAvailableDiskGreaterCompare());

    // update chunk_handle to location mapping with this info
    this->chunk_locations_map_.insert({chunk_handle, allocated_locations});
  }

  return allocated_locations;
}

bool ChunkServerManager::RegisterChunkServer(
    const std::shared_ptr<protos::ChunkServer> chunk_server) {
  auto result =
      this->chunk_servers_map_.insert({chunk_server->location(), chunk_server});

  // Was successfully inserted, means not previously registered
  if (result.second) {
    LOG(INFO) << "Registering chunk server "
              << CreateChunkServerLocationString(chunk_server->location());

    // Lock guard scope
    {
      // create a lock for this chunkserver
      std::shared_ptr<absl::Mutex> chunk_server_lock(new absl::Mutex());

      // Acquire the newly created lock here exclusively. Ideally, we will be
      // the only one with this lock after inserting it to the lock map, since
      // this chunkserver is just being registered and not yet exposed for
      // allocation.
      absl::WriterMutexLock chunk_server_write_lock_guard(
          chunk_server_lock.get());

      this->chunk_servers_lock_map_.insert(
          {chunk_server->location(), chunk_server_lock});

      // update the chunk location map for the chunks owned by this chunkserver
      // if any.
      for (int i = 0; i < chunk_server->stored_chunk_handles_size(); ++i) {
        this->chunk_locations_map_[chunk_server->stored_chunk_handles(i)]
            .insert(chunk_server->location());
        LOG(INFO) << "Chunk server "
                  << CreateChunkServerLocationString(chunk_server->location())
                  << " reported chunk "
                  << chunk_server->stored_chunk_handles(i);
      }
    }

    // We are about to add this chunkserver to the priority list so it becomes
    // available for allocation. Take a lock since chunk allocation can be going
    // on.
    absl::MutexLock chunk_servers_priority_list_lock_guard(
        &chunk_servers_priority_list_lock_);
    // Now make the chunkserver available for chunk allocation
    this->chunk_servers_priority_list_.push_back(chunk_server);
    // we need to keep ordered
    this->chunk_servers_priority_list_.sort(
        ChunkServerAvailableDiskGreaterCompare());
  }

  return result.second;
}

void ChunkServerManager::UnRegisterChunkServer(
    const protos::ChunkServerLocation& server_location) {
  auto result = this->chunk_servers_map_.find(server_location);

  // if not found
  if (result == this->chunk_servers_map_.end()) {
    // nothing to unregister
    return;
  }

  auto chunk_server = result->second;

  // Now erase it, we no longer know about it, and won't return it for
  // GetChunkServer calls
  this->chunk_servers_map_.erase(server_location);

  // Lock guard scope
  {
    // Taking this lock here, since it is used for allocating chunks to
    // chunkserver. We want to first remove it from being selected for
    // allocation and make sure no allocation is going on as we remove this
    // chunkserver.
    absl::MutexLock chunk_servers_priority_list_lock_guard(
        &chunk_servers_priority_list_lock_);

    // remove it from the priority list, this makes sure we don't
    // use it for future chunk allocation.
    for (auto iterator = this->chunk_servers_priority_list_.cbegin();
         iterator != this->chunk_servers_priority_list_.cend(); ++iterator) {
      if ((*iterator)->location() == chunk_server->location()) {
        // found it
        this->chunk_servers_priority_list_.erase(iterator);
        break;
      }
    }
  }

  // Ideally at this point, no one should be holding any lock on the
  // chunkserver, since it has been erased. We should be the only one that knows
  // about this chunk_server. So lets erase the mutex. Watch this.
  this->chunk_servers_lock_map_.erase(chunk_server->location());

  // remove this chunk_server from the chunk handle map, so we don't return it
  // as one of the locations for the chunk
  for (int i = 0; i < chunk_server->stored_chunk_handles_size(); ++i) {
    this->chunk_locations_map_[chunk_server->stored_chunk_handles(i)].erase(
        chunk_server->location());
  }
}

void ChunkServerManager::UnRegisterAllChunkServers() {
  this->chunk_servers_map_.clear();
  this->chunk_locations_map_.clear();
  this->chunk_servers_priority_list_.clear();
  this->chunk_servers_lock_map_.clear();
}

const protos::ChunkServer ChunkServerManager::GetChunkServer(
    const protos::ChunkServerLocation& server_location) {
  protos::ChunkServer server;

  auto result = this->chunk_servers_map_.find(server_location);

  // Check if it was found
  if (result != this->chunk_servers_map_.end()) {
    server = *result->second;
  }

  return server;
}

void ChunkServerManager::UpdateChunkServer(
    const protos::ChunkServerLocation& server_location,
    const uint32_t& available_disk_mb,
    const absl::flat_hash_set<std::string>& chunks_to_add,
    const absl::flat_hash_set<std::string>& chunks_to_remove) {
  auto result = this->chunk_servers_map_.find(server_location);

  // If not found, nothing to update
  if (result == this->chunk_servers_map_.end()) {
    return;
  }

  // We need to update this chunk server. First remove the chunks that needs to
  // be removed and add the new ones.
  auto chunk_server = result->second;

  LOG(INFO) << "Updating chunk server "
            << CreateChunkServerLocationString(chunk_server->location())
            << " with newly reported info";

  // Get priority list lock before chunkserver lock. This is to avoid deadlock
  // when we are running update while allocation is also going on. Since
  // allocation also gets priority list lock before chunkserver lock.

  // Taking this lock here, since an allocation could
  // be resorting the priority list as we update the chunkserver disk.
  absl::MutexLock chunk_servers_priority_list_lock_guard(
      &chunk_servers_priority_list_lock_);

  // Acquire lock here before adding/removing chunks from the chunkserver.
  absl::WriterMutexLock chunk_server_write_lock_guard(
      chunk_servers_lock_map_[chunk_server->location()].get());

  // Remove chunks
  if (!chunks_to_remove.empty()) {
    for (auto iterator = chunk_server->stored_chunk_handles().begin();
         iterator != chunk_server->stored_chunk_handles().end();) {
      auto current_chunk_handle = *iterator;

      if (chunks_to_remove.contains(current_chunk_handle)) {
        // Remove this chunk from chunkserver
        // Returns iterator to next element
        iterator =
            chunk_server->mutable_stored_chunk_handles()->erase(iterator);
        // Remove this chunk server from the chunk handle location map.
        this->chunk_locations_map_[current_chunk_handle].erase(
            chunk_server->location());
      } else {
        ++iterator;
      }
    }
  }

  // Add chunks
  for (auto iterator = chunks_to_add.begin(); iterator != chunks_to_add.end();
       ++iterator) {
    auto current_chunk_handle = *iterator;
    chunk_server->add_stored_chunk_handles(current_chunk_handle);
    // Add to chunk location map
    this->chunk_locations_map_[current_chunk_handle].insert(
        chunk_server->location());
  }

  // Update available disk.
  chunk_server->set_available_disk_mb(available_disk_mb);

  // sort priority list since pointed chunkserver disk has changed
  // in order to place chunkserver in the right position
  this->chunk_servers_priority_list_.sort(
      ChunkServerAvailableDiskGreaterCompare());
}

ChunkServerLocationThreadSafeFlatSet ChunkServerManager::GetChunkLocations(
    const std::string& chunk_handle) {
  // Returns the set of locations if chunk_handle exist or inserts an empty
  // set and return it.
  return this->chunk_locations_map_[chunk_handle];
}

std::string ChunkServerManager::CreateChunkServerLocationString(
    const protos::ChunkServerLocation& location) {
  return absl::StrCat(location.server_hostname(), ":", location.server_port());
}

}  // namespace server
}  // namespace gfs
