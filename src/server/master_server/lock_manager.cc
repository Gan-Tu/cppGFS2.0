#include "lock_manager.h"
#include "absl/hash/hash.h"
#include <thread>

namespace gfs {

/* Initialize the meta locks and the vector of filePathLocks  */
LockManager::LockManager() {
   shard_size_ = std::max(std::thread::hardware_concurrency(),(unsigned int)1);
   metaLocks_ = std::vector<absl::Mutex*>(shard_size_, new absl::Mutex());
   filePathLocks_ = std::vector<absl::flat_hash_map<std::string, absl::Mutex*>>(shard_size_);
   globalLock_ = new absl::Mutex();
}

bool LockManager::Exist(const std::string& pathname) {
   auto idx(std::hash<std::string>{}(pathname) % shard_size_);
   absl::MutexLock anchor(metaLocks_[idx]);
   return filePathLocks_[idx].count(pathname);
}

absl::Mutex* LockManager::AddLockIfNonExist(const std::string& pathname) {
   auto idx(std::hash<std::string>{}(pathname) % shard_size_);
   absl::MutexLock anchor(metaLocks_[idx]);
   if(filePathLocks_[idx].count(pathname)) return NULL;

   filePathLocks_[idx][pathname] = new absl::Mutex();
   return filePathLocks_[idx].at(pathname);
}

absl::Mutex* LockManager::GetLock(const std::string& pathname) {
   auto idx(std::hash<std::string>{}(pathname) % shard_size_);
   absl::MutexLock anchor(metaLocks_[idx]);
   return filePathLocks_[idx].at(pathname);
}

bool LockManager::AcquireLockForParentDir(const std::string& pathname, 
                                          std::stack<absl::Mutex*>& locks) {
   // Make sure that initially the stack is empty
   assert(locks.empty());
  
   auto slashPos(pathname.find('/',1));
   while(slashPos!=std::string::npos) {
      auto dir(pathname.substr(0,slashPos));
      // If some of these intermediate path does not exist, return false
      if(!Exist(dir)) {
         while(!locks.empty()) locks.pop();
         return false;
      }
      // Grab the reader lock for dir and push it to the stack
      auto l(GetLock(dir));
      l->ReaderLock();
      locks.push(l);
      slashPos = pathname.find('/',slashPos+1);
   }
   return true;
}
 
void LockManager::ReleaseLockForParentDir(std::stack<absl::Mutex*>& locks) {
   while(!locks.empty()) {
      // Unlock the reader locks in a reverse sequence
      auto l(locks.top());
      locks.pop();
      l->ReaderUnlock();
   }
}

absl::Mutex* LockManager::globalLock() {
   return globalLock_;
}

LockManager* LockManager::GetInstance() {
   static LockManager* inst = new LockManager();
   return inst;
}


}
