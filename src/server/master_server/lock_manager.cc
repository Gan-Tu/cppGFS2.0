#include "src/server/master_server/lock_manager.h"

#include <thread>

#include "absl/hash/hash.h"

namespace gfs {
namespace server {

/* A private helper class to compute the index in the filePathLocks_ collection for a
 * pathanme. */
inline uint16_t LockManager::bucket_id(const std::string& pathname) const {
   return std::hash<std::string>{}(pathname) % shard_size_;
}

/* Initialize the meta locks and the vector of filePathLocks  */
LockManager::LockManager() {
  shard_size_ = std::max(std::thread::hardware_concurrency(), (unsigned int) 1);
  metaLocks_ = std::vector<absl::Mutex*>(shard_size_, new absl::Mutex());
  filePathLocks_ =
      std::vector<absl::flat_hash_map<std::string, absl::Mutex*>>(shard_size_);
}

bool LockManager::Exist(const std::string& pathname) const {
  auto idx(bucket_id(pathname));
  absl::MutexLock anchor(metaLocks_[idx]);
  return filePathLocks_[idx].contains(pathname);
}

google::protobuf::util::StatusOr<absl::Mutex*> LockManager::CreateLock(
  const std::string& pathname) {
  
  auto idx(bucket_id(pathname));
  absl::MutexLock anchor(metaLocks_[idx]);
  if (filePathLocks_[idx].contains(pathname)) {
    return google::protobuf::util::Status(
             google::protobuf::util::error::OUT_OF_RANGE, 
             "Lock already exists for "+pathname);
  }

  auto ret(new absl::Mutex());
  filePathLocks_[idx][pathname] = ret;
  return ret;
}

google::protobuf::util::StatusOr<absl::Mutex*> LockManager::FetchLock(
  const std::string& pathname) const {
  
  auto idx(std::hash<std::string>{}(pathname) % shard_size_);
  absl::MutexLock anchor(metaLocks_[idx]);
  if (!filePathLocks_[idx].contains(pathname)) {
    return google::protobuf::util::Status(
             google::protobuf::util::error::OUT_OF_RANGE, 
             "Lock does not exist for "+pathname);
  }
  
  return filePathLocks_[idx].at(pathname);
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
    // Otherwise, grab the reader lock for dir and push it to the stack
    auto res(lockManager->FetchLock(dir));
    if(!res.ok()) {
      status_ = google::protobuf::util::Status(
                  google::protobuf::util::error::OUT_OF_RANGE,
                  "Lock for " + dir + " does not exist"); 
      return;
    }
    
    auto l(res.ValueOrDie());
    l->ReaderLock();
    locks_.push(l);
    slashPos = pathname.find('/', slashPos + 1);
  }
  status_ = google::protobuf::util::Status::OK;
}
     
bool ParentLocksAnchor::ok() const {
  return status_.ok();
}

google::protobuf::util::Status ParentLocksAnchor::status() const {
  return status_;
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
