#include "src/server/master_server/lock_manager.h"

#include <thread>

#include "absl/hash/hash.h"

using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;

namespace gfs {
namespace server {

/* A private helper class to compute the index in the file_path_locks_
 * collection for a pathanme. */
inline uint16_t LockManager::bucket_id(const std::string& filename) const {
  return std::hash<std::string>{}(filename) % shard_size_;
}

/* Initialize the meta locks and the vector of filePathLocks  */
LockManager::LockManager() {
  shard_size_ = std::max(std::thread::hardware_concurrency(), (unsigned int)1);
  meta_locks_ = std::vector<absl::Mutex*>(shard_size_, new absl::Mutex());
  file_path_locks_ =
      std::vector<absl::flat_hash_map<std::string, absl::Mutex*>>(shard_size_);
}

bool LockManager::Exist(const std::string& filename) const {
  auto idx(bucket_id(filename));
  absl::MutexLock lock_guard(meta_locks_[idx]);
  return file_path_locks_[idx].contains(filename);
}

google::protobuf::util::StatusOr<absl::Mutex*> LockManager::CreateLock(
    const std::string& filename) {
  auto idx(bucket_id(filename));
  absl::MutexLock lock_guard(meta_locks_[idx]);
  if (file_path_locks_[idx].contains(filename)) {
    return Status(google::protobuf::util::error::ALREADY_EXISTS,
                  "Lock already exists for " + filename);
  }

  auto ret(new absl::Mutex());
  file_path_locks_[idx][filename] = ret;
  return ret;
}

google::protobuf::util::StatusOr<absl::Mutex*> LockManager::FetchLock(
    const std::string& filename) const {
  auto idx(std::hash<std::string>{}(filename) % shard_size_);
  absl::MutexLock lock_guard(meta_locks_[idx]);
  if (!file_path_locks_[idx].contains(filename)) {
    return Status(google::protobuf::util::error::NOT_FOUND,
                  "Lock does not exist for " + filename);
  }

  return file_path_locks_[idx].at(filename);
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
