#ifndef GFS_SERVER_MASTER_SERVER_LOCK_MANAGER_H_
#define GFS_SERVER_MASTER_SERVER_LOCK_MANAGER_H_

#include <stack>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

namespace gfs {
namespace server {

/* The LockManager handles how we do resource locking for read/write/create to
 * our file system. This objects provides locks according to file path name, and
 * it manages the locks by mapping filename to actual lock objects. Furthermore,
 * it shards the mapping space by storing multiple maps in an array, and for
 * each path name it finds the corresponding map by computing the hash of this
 * path (and mod by the total number of available cores). This is effectively a
 * parallel hash map for locks.
 *
 * The reason for this design is that even though we meant to use the lock
 * objects to lock namespace, these locks are not created ahead of time because
 * we do not now the pathname. So all the creation of locks need to be
 * synchronized with the access to them. Without sharding them into multiple
 * maps, this synchronization is a potential bottle neck when concurrent
 * creation and read request are made to the master. We introduce an internal
 * vector of "meta locks" to manage the synchronization of each maps.
 *
 * The LockManager supports methods to check whether a path name exists, add a
 * lock for a given path name and get a lock. It also provides a global lock 
 * so one can use for synchronization purposes. */
class LockManager {
 public:
  /* Methods to check the existence of the lock for a given path, add a lock and
   * access a lock*/
  bool Exist(const std::string& pathname) const;

  /* Instantiate a lock for a given file if it is non-existent. Otherwise return
   * a null pointer indicating that it has been created by someone else*/
  absl::Mutex* AddLockIfNonExist(const std::string& pathname);
  absl::Mutex* GetLock(const std::string& pathname) const;

  /* Access a global lock. Though named global, one can perform ReaderLock and
   * WriterLock using it. */
  absl::Mutex* globalLock() const;

  /* Get the instance of the LockManager, which is a singleton */
  static LockManager* GetInstance();

 private:
  LockManager();

  // The size of sharding, set equal to the number of hardware threads
  uint16_t shard_size_;
  // Used to lock the hash map that maps from filepath to locks
  std::vector<absl::Mutex*> metaLocks_;
  // A sharded hash map that maps from file path to mutexes, which are
  // used to lock per-file metadata, i.e. filePathToMetadata above
  std::vector<absl::flat_hash_map<std::string, absl::Mutex*>> filePathLocks_;
  // A global lock
  absl::Mutex* globalLock_;
};

/* A helper class which is an RAII wrapper to automatically acquire reader  
 * locks for all the parent directories of a given path name. It stores
 * the relevant locks that are acquired in sequence (from the root one to 
 * the immediate parent directory) in a stack, and releases these locks upon
 * destruction. 
 * */
class ParentLocksAnchor {
  public:
   ParentLocksAnchor(LockManager* _lm, const std::string& _pathname);
   ~ParentLocksAnchor();
   bool succ() const;
   size_t lock_size() const; 
  private:
   std::stack<absl::Mutex*> lks_;
   bool succ_;
};

}  // namespace server
}  // namespace gfs

#endif  // GFS_SERVER_MASTER_SERVER_LOCK_MANAGER_H_
