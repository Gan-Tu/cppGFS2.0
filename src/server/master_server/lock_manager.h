#ifndef GFS_SERVER_MASTER_SERVER_LOCK_MANAGER_H_
#define GFS_SERVER_MASTER_SERVER_LOCK_MANAGER_H_

#include <stack>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "google/protobuf/stubs/statusor.h"
#include "src/common/utils.h"

namespace gfs {
namespace server {

// The LockManager handles how we do resource locking for read/write/create to
// our file system. This objects provides locks according to file path name, and
// it manages the locks by mapping filename to actual lock objects. Furthermore,
// it shards the mapping space by storing multiple maps in an array, and for
// each path name it finds the corresponding map by computing the hash of this
// path (and mod by the total number of available cores). This is effectively a
// parallel hash map for locks.
//
// The reason for this design is that even though we meant to use the lock
// objects to lock namespace, these locks are not created ahead of time because
// we do not now the filename. So all the creation of locks need to be
// synchronized with the access to them. Without sharding them into multiple
// maps, this synchronization is a potential bottle neck when concurrent
// creation and read request are made to the master. We introduce an internal
// vector of "meta locks" to manage the synchronization of each maps.
//
// The LockManager supports methods to check whether a path name exists, add a
// lock for a given path name and get a lock.
class LockManager {
 public:
  // Check the existence of a lock given a filename
  bool Exist(const std::string& filename) const;

  // Create a lock for a given path, return error if the lock already exists
  google::protobuf::util::StatusOr<absl::Mutex*> CreateLock(
      const std::string& filename);

  // Retrieve a lock for a given path, return error if the lock does not exist
  google::protobuf::util::StatusOr<absl::Mutex*> FetchLock(
      const std::string& filename) const;

  // Get the instance of the LockManager, which is a singleton
  static LockManager* GetInstance();

 private:
  // A parallel hash map that maps from file path to mutexes, which are
  // used to synchronize read and write operations to FileMetadata
  gfs::common::thread_safe_flat_hash_map<
      std::string, std::shared_ptr<absl::Mutex>> file_path_locks_;
};

// A helper class which is an RAII wrapper to automatically acquire reader
// locks for all the parent directories of a given path name. It stores
// the relevant locks that are acquired in sequence (from the root one to
// the immediate parent directory) in a stack, and releases these locks upon
// destruction.
class ParentLocksAnchor {
 public:
  ParentLocksAnchor(LockManager* lock_manager, const std::string& filename);
  ~ParentLocksAnchor();
  bool ok() const;
  google::protobuf::util::Status status() const;
  size_t lock_size() const;

 private:
  std::stack<absl::Mutex*> locks_;
  google::protobuf::util::Status status_;
};

}  // namespace server
}  // namespace gfs

#endif  // GFS_SERVER_MASTER_SERVER_LOCK_MANAGER_H_
