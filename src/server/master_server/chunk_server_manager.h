#ifndef GFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H_
#define GFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H_

#include <functional>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "src/common/utils.h"
#include "src/protos/chunk_server.pb.h"

// Define how we do equality check for the ChunkServerLocation and ChunkServer
// injected into the proto namespace where ChunkServerLocation and ChunkServer
// are defined.
// TODO(bmokutub): Can move to a separate header file e.g. chunk_server.h
namespace protos {
bool operator==(const protos::ChunkServerLocation& lhs,
                const protos::ChunkServerLocation& rhs);

bool operator==(const protos::ChunkServer& lhs, const protos::ChunkServer& rhs);
}  // namespace protos

namespace gfs {

namespace server {

// For generating the hash value of a ChunkServerLocation. This is for use in
// hashset and maps and the ChunkServerLocation is used as the key/unique
// identifier for the ChunkServer. A simple implementation.
class ChunkServerLocationHash {
 public:
  std::size_t operator()(const protos::ChunkServerLocation& location) const
      noexcept {
    std::size_t h1 = std::hash<std::string>{}(location.server_hostname());
    std::size_t h2 = std::hash<uint32_t>{}(location.server_port());
    return h1 ^ (h2 << 1);
  }
};

// A flat hash set of ChunkServerLocation.
using ChunkServerLocationFlatSet =
    absl::flat_hash_set<protos::ChunkServerLocation, ChunkServerLocationHash>;

// A thread safe flat hash set of ChunkServerLocation.
using ChunkServerLocationThreadSafeFlatSet =
    gfs::common::thread_safe_flat_hash_set<protos::ChunkServerLocation,
                                           ChunkServerLocationHash>;

// This is used as the less than comparision class for two ChunkServers.
// This only compares their available disk and server 1 < server2 if
// server1.available_disk_mb < server2.available_disk_mb.
// Used for ordering ChunkServers by available disk.
class ChunkServerAvailableDiskLessCompare {
 public:
  bool operator()(const protos::ChunkServer& lhs,
                  const protos::ChunkServer& rhs) const {
    return lhs.available_disk_mb() < rhs.available_disk_mb();
  }
};

// This is used as the greater than comparision class for two ChunkServers.
// This only compares their available disk and server 1 > server2 if
// server1.available_disk_mb > server2.available_disk_mb.
// Used for ordering ChunkServers by available disk.
class ChunkServerAvailableDiskGreaterCompare {
 public:
  bool operator()(const protos::ChunkServer& lhs,
                  const protos::ChunkServer& rhs) const {
    return lhs.available_disk_mb() > rhs.available_disk_mb();
  }

  // Allows pointer comparision
  bool operator()(const std::shared_ptr<protos::ChunkServer>& lhs,
                  const std::shared_ptr<protos::ChunkServer>& rhs) const {
    return lhs->available_disk_mb() > rhs->available_disk_mb();
  }
};

// Singleton class for managing all the chunk servers. Selects the chunk server
// to store a chunk. Tries to balance the load on the servers.
// Knows the location where all the chunks are stored.
// A new chunk server can be registered to allow it to start being selected for
// chunk storage. And can be unregistered and after this ChunkServerManager
// doesn't know about it and won't select it for future chunk storage. A
// registered ChunkServer can also be updated when there's new information about
// the chunk sever. Chunk servers are uniquely identified by the
// ChunkServerLocation and no two chunk server can have the same location. In
// order to change a chunk server location, the chunk server must first be
// unregistered and reregistered with a new location. To reuse an exisiting
// location, the current chunk server using that location must first be
// unregistered, then register the new chunk server with that location.
//
// Note: All ChunkServerManager methods are thread safe.
//
// ChunkServerManager User:
//   Metadata manager (AllocateChunkServer, GetChunkLocations):
//   Usage: For a new chunk that needs servers for storage call,
//     if it needs 1 replica (1 server):
//     auto server_locations_set =
//     ChunkServerManager::GetInstance()::AllocateChunkServer("chunk_handle",
//     /*requested_servers_count=*/ 1);
//     if it needs 5 replicas (5 servers):
//     auto server_locations_set =
//     ChunkServerManager::GetInstance()::AllocateChunkServer("chunk_handle",
//     /*requested_servers_count=*/ 5);
//
//     To find the locations where a chunk is stored:
//     auto server_locations_set =
//     ChunkServerManager::GetInstance()::GetChunkLocations("chunk_handle");
//     Note: As stated in the method comment, if a server goes down, it will not
//     be returned as part of the locations for a stored chunk. To avoid clients
//     calling a server that is down. This is why chunk replication is good,
//     so if you allocated 3 servers in the allocation call, if 1 of the servers
//     is down, when you call GetChunkLocations, you get 2 servers. If the
//     server recovers and starts again, then subsequent GetChunkLocations will
//     return 3 servers.
//
// ChunkServerManager services that keeps it updated:
//   ChunkServerHeartBeatMonitorTask (UnregisterChunkServer)
//
//   MasterChunkServerManagerService (RegisterChunkServer, UpdateChunkServer)
//
class ChunkServerManager {
  // To access chunk servers map
  friend class ChunkServerHeartBeatMonitorTask;

 public:
  // The singleton instance
  static ChunkServerManager& GetInstance() {
    static ChunkServerManager instance;

    return instance;
  }

  // To prevent copying the ChunkServerManager
  ChunkServerManager(const ChunkServerManager&) = delete;

  void operator=(const ChunkServerManager&) = delete;

  // This will allocate the specified number of chunk servers for storing the
  // specified chunk. And will return the locations of the allocated servers.
  // This is where we do some load balancing to make sure that the chunks are
  // evenly distributed across chunkservers. We prioritize chunk servers with
  // the most available disk space. This is threadsafe and allows concurrent
  // allocations. Note:
  // 1. The specified chunk_handle is now mapped to the returned locations.
  // 2. Will return empty set of locations, if there is no chunk server
  // available for chunk storage. e.g. When chunkservers don't have enough disk
  // space left or there are no chunk servers registered (i.e. servers down).
  // 3. If allocate is called for an already allocated chunk, we will return the
  // previously allocated locations and not do new allocation.
  // TODO(bmokutub): We will need to support allocating new locations for a
  // previously allocated chunk. Useful in order to increase replica count.
  // Also just return a flat set instead of the internal threadsafe set copy.
  // Copy the internal threadsafe set to a flat set and return.
  // Caller is just getting a copy so shouldn't worry about thread safety and
  // shouldn't be confused by the return type.
  ChunkServerLocationThreadSafeFlatSet AllocateChunkServer(
      const std::string& chunk_handle, const ushort& requested_servers_count);

  // Register the ChunkServer with the manager.
  // Manager can now decide to select it for chunk storage.
  // If it's an exisiting server with allocated chunk handles, the chunk handles
  // will be updated to reflect this server location and future query of the
  // chunk handle location will show this server location. Threadsafe and allows
  // concurrent registration. Returns false if ChunkServer is already
  // registered. Could be because another chunk server is already using that
  // server location and we only allow unique locations.
  bool RegisterChunkServer(
      const std::shared_ptr<protos::ChunkServer> chunk_server);

  // Unregister the ChunkServer with the manager.
  // Manager no longer knows about this server and won't be selected for chunk
  // storage. This also removes the chunk server from the locations for
  // previously allocated chunk handles. When these chunk handles locations are
  // queried, it won't show this location. After this, the server location can
  // be used to register the same chunk server again or another chunk server.
  void UnRegisterChunkServer(
      const protos::ChunkServerLocation& server_location);

  // This is used for testing only. Unregister all the ChunkServers. Not thread
  // safe on purpose. There would be no ChunkServer for chunk storage until they
  // are registered.
  // TODO(bmokutub): Consider making this private and making the unit test class
  // a friend.
  void UnRegisterAllChunkServers();

  // Returns the ChunkServer for the specified location. Returns the default
  // chunkserver object if not registered. This is used by the
  // ChunkServerManager services only.
  // TODO(bmokutub): Consider making this private and making the service class
  // a friend.
  const protos::ChunkServer GetChunkServer(
      const protos::ChunkServerLocation& server_location);

  // Update information about a registered chunkserver. We only allow updating
  // the available disk and chunks. Location can't be updated since it uniquely
  // identifies this chunkserver. To change the chunkserver location, it needs
  // to unregistered and reregistered with a new location. This is only used by
  // the chunkserver manager service to update the chunkserver when new
  // information is received from it. It is threadsafe.
  // TODO(bmokutub): Consider making this private and making the service class
  // a friend.
  void UpdateChunkServer(
      const protos::ChunkServerLocation& server_location,
      const uint32_t& available_disk_mb,
      const absl::flat_hash_set<std::string>& chunks_to_add,
      const absl::flat_hash_set<std::string>& chunks_to_remove);

  // Returns the ChunkServerLocations for a given chunk handle.
  // This returns all the chunk server locations where this chunk is stored.
  // This returns an empty set if there is no location. This can be because the
  // chunk hasn't been allocated by calling the AllocateChunkServer (dev bug) or
  // because the chunk servers where this chunk was allocated have been
  // unregistered (due to server down). The returned locations might be smaller
  // than the number of allocated servers if some of those servers have been
  // unregistered (due to server down). The ChunkServer heartbeat monitor,
  // checks when a server is down and unregister it. If the server comes back
  // up, it will be reregistered by the chunkservermanager service.
  // TODO(bmokutub): Copy the internal threadsafe set to a flat set and return.
  // Caller is just getting a copy so shouldn't worry about thread safety and
  // shouldn't be confused by the return type.
  ChunkServerLocationThreadSafeFlatSet GetChunkLocations(
      const std::string& chunk_handle);

 private:
  // This is a thread safe map that stores the mapping from chunkserverlocation
  // to the chunkserver. The location uniquely identifies the chunkserver. This
  // is used to store the registered chunk servers and are removed when the
  // chunkserver is unregistered.
  gfs::common::thread_safe_flat_hash_map<protos::ChunkServerLocation,
                                         std::shared_ptr<protos::ChunkServer>,
                                         ChunkServerLocationHash>
      chunk_servers_map_;

  // This is a thread safe map that stores the mapping from chunk handle to the
  // chunk server locations where it is stored. Chunk handles are added here
  // once chunkservers are allocated for them. Using a thread safe set since we
  // concurrently add and remove chunk locations for a chunk handle.
  gfs::common::thread_safe_flat_hash_map<std::string,
                                         ChunkServerLocationThreadSafeFlatSet>
      chunk_locations_map_;

  // This is used to keep a priority list of chunk servers for chunk allocation.
  // Servers with most available disk are given highest priority. The reason for
  // using a list and not a heap is because we need to be able to remove a chunk
  // server from the list whenever a chunkserver is unregistered/down. Heaps
  // don't support random removal. We don't want to be allocating chunks to an
  // unavailable chunk server. And this also helps us to do load balancing, by
  // prioritizing chunk servers with most available disk first for chunk
  // allocation. we always keep this list sorted so max available disk
  // chunkservers first. There are several possible optimizations for this, but
  // will fist monitor performance of this.
  // Also not using an ordered set/multiset because they use equivalance
  // (!comp(a, b) && !comp(b, a)) for element uniqueness. So if we remove a
  // chunkserver with disk 10, it removes all with disk 10. The list gives us
  // full control of things and can periodically sort or offload sorting to
  // background thread.
  std::list<std::shared_ptr<protos::ChunkServer>> chunk_servers_priority_list_;

  // Lock to synchronize access to the priority list. Since we allow different
  // concurrent operations that use the list at some point.
  absl::Mutex chunk_servers_priority_list_lock_;

  // Thread safe map for storing the mapping for chunkserver location to a
  // mutex. Essentially this is used to store a mutex for each chunkserver. And
  // the location uniquely identifies the owning chunkserver. This is used as a
  // ReadWrite lock for reading/updating a chunkserver. Several operations
  // either reads or change the chunkserver info.
  gfs::common::thread_safe_flat_hash_map<protos::ChunkServerLocation,
                                         std::shared_ptr<absl::Mutex>,
                                         ChunkServerLocationHash>
      chunk_servers_lock_map_;

  ChunkServerManager() = default;
};
}  // namespace server
}  // namespace gfs

#endif  // GFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H_
