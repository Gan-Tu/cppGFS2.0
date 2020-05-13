#include "src/server/master_server/lock_manager.h"

#include <thread>

#include "absl/hash/hash.h"

namespace gfs {
namespace server {

/* Initialize the meta locks and the vector of filePathLocks  */
LockManager::LockManager() {
  shard_size_ = std::max(std::thread::hardware_concurrency(), (unsigned int) 1);
  metaLocks_ = std::vector<absl::Mutex*>(shard_size_, new absl::Mutex());
  filePathLocks_ =
      std::vector<absl::flat_hash_map<std::string, absl::Mutex*>>(shard_size_);
  globalLock_ = new absl::Mutex();
}

bool LockManager::Exist(const std::string& pathname) const {
  auto idx(std::hash<std::string>{}(pathname) % shard_size_);
  absl::MutexLock anchor(metaLocks_[idx]);
  return filePathLocks_[idx].contains(pathname);
}

absl::Mutex* LockManager::AddLockIfNonExist(const std::string& pathname) {
  auto idx(std::hash<std::string>{}(pathname) % shard_size_);
  absl::MutexLock anchor(metaLocks_[idx]);
  if (filePathLocks_[idx].contains(pathname)) return NULL;

  filePathLocks_[idx][pathname] = new absl::Mutex();
  return filePathLocks_[idx].at(pathname);
}

absl::Mutex* LockManager::GetLock(const std::string& pathname) const {
  auto idx(std::hash<std::string>{}(pathname) % shard_size_);
  absl::MutexLock anchor(metaLocks_[idx]);
  return filePathLocks_[idx].at(pathname);
}

absl::Mutex* LockManager::globalLock() const {
   return globalLock_;
}

LockManager* LockManager::GetInstance() {
  static LockManager* inst = new LockManager();
  return inst;
}

/* Acquire readerLock for the parent paths of a given pathname, i.e. if
 * pathname is "/foo/bar/baz", the lock manager acquires reader lock for
 * "/foo" and "/foo/bar", and store these locks in a stack. */
ParentLocksAnchor::ParentLocksAnchor(LockManager* lockManager, 
                                     const std::string& pathname) {
  auto slashPos(pathname.find('/', 1));
  while (slashPos != std::string::npos) {
    auto dir(pathname.substr(0, slashPos));
    // If some of these intermediate path does not exist, return false
    if (!lockManager->Exist(dir)) {
      ok_ = false;
      return;
    }
    // Grab the reader lock for dir and push it to the stack
    auto l(lockManager->GetLock(dir));
    l->ReaderLock();
    locks_.push(l);
    slashPos = pathname.find('/', slashPos + 1);
  }
  ok_ =  true;
}
     
bool ParentLocksAnchor::ok() const {
  return ok_;
}

size_t ParentLocksAnchor::lock_size() const {
  return locks_.size();
}

ParentLocksAnchor::~ParentLocksAnchor() {
  while (!locks_.empty()) {
  // Unlock the reader locks in a reverse sequence
    auto l(locks_.top());
    locks_.pop();
    l->ReaderUnlock();
  }
}

}  // namespace server
}  // namespace gfs
