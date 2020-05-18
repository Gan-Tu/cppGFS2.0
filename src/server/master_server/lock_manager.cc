#include "src/server/master_server/lock_manager.h"

#include <thread>

using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;

namespace gfs {
namespace server {

LockManager::LockManager() {
  num_of_submap_ = std::max(std::thread ::hardware_concurrency(), 
                            (unsigned int)1);
  // TODO(Xi): advanced usage may require tuning the submap number so 
  // to configure the above number
  
  // Initialize the locks and submaps
  submap_lock_ = std::vector<absl::Mutex*>(num_of_submap_, new absl::Mutex());
  file_path_locks_ = std::vector<absl::flat_hash_map<
                         std::string, absl::Mutex*>>(num_of_submap_);
}

inline size_t LockManager::submap_id(const std::string& filename) const {
  // Compute the hash of given string, and mod the number of submaps
  return std::hash<std::string>{}(filename) % num_of_submap_;
}

bool LockManager::Exist(const std::string& filename) const {
  auto map_id(submap_id(filename));
  absl::Mutex* submap_lock(submap_lock_[map_id]);
  // Only a ReaderLock action is needed here as we perform lookup only
  absl::ReaderMutexLock lock_guard(submap_lock); 
  return file_path_locks_[map_id].contains(filename);
}

google::protobuf::util::StatusOr<absl::Mutex*> LockManager::CreateLock(
    const std::string& filename) {
  auto map_id(submap_id(filename));
  absl::Mutex* submap_lock(submap_lock_[map_id]);
  // Use the RAII wrapper to automatically lock a lock and unlock it 
  // when returning. This saves us from explicitly writing unlock()
  // and from making mistakes when missing these unlock() operations
  // when returning early
  absl::WriterMutexLock lock_guard(submap_lock); 
 
  // If the filename exists, that means another thread has created this
  // lock, or this thread has been created previously by the current
  // thread
  if(file_path_locks_[map_id].contains(filename)) {
    return Status(google::protobuf::util::error::ALREADY_EXISTS,
                  "Lock already exists for " + filename);
  }
 
  file_path_locks_[map_id][filename] = new absl::Mutex();
  
  // TODO: We have not finalized the plan regarding to the removal of locks.
  // The simplest approach is to say we don't remove locks (note that not 
  // removing locks doesn't equate to not removing files) so we won't have 
  // an issue that a removal takes place between the above and code below. 
  // This may seem too restricted, but dealing with concurrent creation and
  // deletion is a pain and having a full-support of this may be rather 
  // an overkill. What we can do instead is to defer the deletion of 
  // lock resources (they are relatively small comparing to file metadata)
  // and say, let's remove the lock resources only after the actual chunk 
  // resources got garbage collected. Let's see how far we can go with this 
  // project and see if we get a chance to improve the design here. 
  return file_path_locks_[map_id][filename];
}

google::protobuf::util::StatusOr<absl::Mutex*> LockManager::FetchLock(
    const std::string& filename) const {
  auto map_id(submap_id(filename));
  absl::Mutex* submap_lock(submap_lock_[map_id]);
  // Only a ReaderLock action is needed here as we perform lookup only
  absl::ReaderMutexLock lock_guard(submap_lock); 
    
  if (!file_path_locks_[map_id].contains(filename)) {
    return Status(google::protobuf::util::error::NOT_FOUND,
                  "Lock does not exist for " + filename);
  }
  // Note that this is a const method (no mutations take place), so we have
  // to use the const .at() method as opposed to [] operator, which 
  // could end up inserting an entry to map
  return file_path_locks_[map_id].at(filename);
}

LockManager* LockManager::GetInstance() {
  static LockManager* manager = new LockManager();
  return manager;
}

/* Acquire readerLock for the parent paths of a given filename, i.e. if
 * filename is "/foo/bar/baz", the lock manager acquires reader lock for
 * "/foo" and "/foo/bar", and store these locks in a stack. */
ParentLocksAnchor::ParentLocksAnchor(LockManager* lock_manager,
                                     const std::string& filename) {
  auto slashPos(filename.find('/', 1));
  while (slashPos != std::string::npos) {
    auto dir(filename.substr(0, slashPos));
    // If some of these intermediate path does not exist, return false
    // Otherwise, grab the reader lock for dir and push it to the stack
    StatusOr<absl::Mutex*> path_lock_or(lock_manager->FetchLock(dir));
    if (!path_lock_or.ok()) {
      status_ = Status(google::protobuf::util::error::NOT_FOUND,
                       "Lock for " + dir + " does not exist");
      return;
    }

    auto path_lock(path_lock_or.ValueOrDie());
    path_lock->ReaderLock();
    locks_.push(path_lock);
    slashPos = filename.find('/', slashPos + 1);
  }
  status_ = google::protobuf::util::Status::OK;
}

bool ParentLocksAnchor::ok() const { return status_.ok(); }

google::protobuf::util::Status ParentLocksAnchor::status() const {
  return status_;
}

size_t ParentLocksAnchor::lock_size() const { return locks_.size(); }

ParentLocksAnchor::~ParentLocksAnchor() {
  while (!locks_.empty()) {
    // Unlock the reader locks in a reverse sequence
    auto lock(locks_.top());
    locks_.pop();
    lock->ReaderUnlock();
  }
}

}  // namespace server
}  // namespace gfs
