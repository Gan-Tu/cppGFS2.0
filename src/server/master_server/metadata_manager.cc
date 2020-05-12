#include "metadata_manager.h"
#include <stack>

namespace gfs {
namespace server {

MetadataManager::MetadataManager() {
   lockManager_ = LockManager::GetInstance();
}

bool MetadataManager::CreateFileMetadata(const std::string& pathname) {
   // Step 1. Lock (readerlock underneath) the parent directory first
   ParentLocksAnchor parentLockAnchor(lockManager_, pathname);
             
   if(!parentLockAnchor.succ()) {
      // If this operation fails, which means some of the parent directory does not
      // exist, we return false
      return false;
   }

   // Step 2. Add a new lock for this new file, and writeLock it
   auto newLock(lockManager_->AddLockIfNonExist(pathname));
   if(!newLock) {
      // We still need to check whether the return is NULL because another thread
      // could well successfully created a new lock for the same path
      return false;
   }
   
   absl::WriterMutexLock anchorForNewLock(newLock);
   // Step 3. writeLock the global lock, instantiate a FileMetadata object
   absl::WriterMutexLock anchorForGlobalLock(lockManager_->globalLock());
   // The reason that we acquire the global lock is that we need to synchronization
   // between write and read from the fileMetadata collection.
   if(fileMetadata_.count(pathname)) {
      return false;
   }
   
   fileMetadata_[pathname] = std::make_shared<FileMetadata>();
   return true;
}

bool MetadataManager::ExistFileMetadata(const std::string& pathname) {
   absl::ReaderMutexLock(lockManager_->globalLock());
   return fileMetadata_.count(pathname);
}

std::shared_ptr<FileMetadata> MetadataManager::GetFileMetadata(const std::string& pathname) {
   // readLock the global lock and retrieve the filemetadata. The reason for a
   // readLock is because we are not mutating anything in the fileMetadata_.
   absl::ReaderMutexLock(lockManager_->globalLock());
   return fileMetadata_.at(pathname);
}

std::string MetadataManager::CreateChunkHandle(const std::string& pathname, 
                                               uint32_t chunk_index) {
   // Step 1. readlock the parent directories
   ParentLocksAnchor parentLockAnchor(lockManager_, pathname);
   if(!parentLockAnchor.succ()) {
      // If this operation fails, which means some of the parent directory does not
      // exist, we return false
      return "";
   }

   // Step 2. readlock the global lock, fetch the data and unlock readerlock
   auto fdata(GetFileMetadata(pathname));
   
   // Step 3. writelock the lock for this path
   auto fLock(lockManager_->GetLock(pathname)); 
   absl::WriterMutexLock anchor(fLock);

   // Step 4. compute a new chunk handle, and insert the (chunk_index, chunkHandle)
   //         pair to fileMetadata_
   std::string newChunkHandle(CreateChunkHandle());
   fdata->set_filename(pathname);
   auto& chunk_handle_map(*fdata->mutable_chunk_handles());
  
   // Return null UIDD if this chunk_index exists
   if(chunk_handle_map.count(chunk_index)) {
      return "";
   }
   
   chunk_handle_map[chunk_index]= newChunkHandle;
   return newChunkHandle;
}

void MetadataManager::DeleteFile(const std::string& pathname) {
   // [TODO]: phase 2
}

std::string MetadataManager::CreateChunkHandle() {
   auto ret(globalChunkId_.fetch_add(1));
   return std::to_string(ret);
}

MetadataManager* MetadataManager::GetInstance() {
   static MetadataManager* inst = new MetadataManager();
   return inst;
}

} /* namespace server */
} /* namespace gfs */
