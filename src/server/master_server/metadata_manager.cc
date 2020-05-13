#include "metadata_manager.h"
#include <stack>

using protos::FileMetadata;

namespace gfs {
namespace server {

MetadataManager::MetadataManager() {
  lockManager_ = LockManager::GetInstance();
  fileMetadataLock_ = new absl::Mutex();
}

google::protobuf::util::Status MetadataManager::CreateFileMetadata(
  const std::string& pathname) {
  
  // Step 1. Lock (readerlock underneath) the parent directory first
  ParentLocksAnchor parentLockAnchor(lockManager_, pathname);
             
  if (!parentLockAnchor.ok()) {
    // If this operation fails, which means some of the parent directory does not
    // exist, we return false
    return parentLockAnchor.status();
  }

  // Step 2. Add a new lock for this new file, and writeLock it
  auto newLockRes(lockManager_->CreateLock(pathname));
  if (!newLockRes.ok()) {
    // We still need to check whether the return is NULL because another thread
    // could well successfully created a new lock for the same path
    return newLockRes.status();
  }
  
  auto newLock(newLockRes.ValueOrDie());
  absl::WriterMutexLock anchorForNewLock(newLock);
  // Step 3. writeLock the global lock, instantiate a FileMetadata object
  absl::WriterMutexLock anchorForFileMetadata(fileMetadataLock_);
  // The reason that we acquire the global lock is that we need to synchronization
  // between write and read from the fileMetadata collection.
  if(fileMetadata_.contains(pathname)) {
    return google::protobuf::util::Status(
             google::protobuf::util::error::OUT_OF_RANGE,
             "File metadata already exists for " + pathname);
  }
   
  fileMetadata_[pathname] = std::make_shared<FileMetadata>();
  // Initialize the filename
  fileMetadata_[pathname]->set_filename(pathname);
  return google::protobuf::util::Status::OK;
}

bool MetadataManager::ExistFileMetadata(const std::string& pathname) const {
  absl::ReaderMutexLock anchor(fileMetadataLock_);
  return fileMetadata_.contains(pathname);
}

google::protobuf::util::StatusOr<std::shared_ptr<FileMetadata>> 
  MetadataManager::GetFileMetadata(
  
  const std::string& pathname) const {
  // readLock the global lock and retrieve the filemetadata. The reason for a
  // readLock is because we are not mutating anything in the fileMetadata_.
  absl::ReaderMutexLock anchor(fileMetadataLock_);
  if (!fileMetadata_.contains(pathname)) {
     return google::protobuf::util::Status(
              google::protobuf::util::error::OUT_OF_RANGE,
              "File metadata does not exist: " + pathname);
  }
  return fileMetadata_.at(pathname);
}

google::protobuf::util::StatusOr<std::string> 
  MetadataManager::CreateChunkHandle(const std::string& pathname, 
                                     uint32_t chunk_index) {
  // Step 1. readlock the parent directories
  ParentLocksAnchor parentLockAnchor(lockManager_, pathname);
  if (!parentLockAnchor.ok()) {
    // If this operation fails, which means some of the parent directory does not
    // exist, we return false
    return parentLockAnchor.status();
  }

  // Step 2. readlock the global lock, fetch the data and unlock readerlock
  auto fdataRes(GetFileMetadata(pathname));
  if (!fdataRes.ok()) {
    return fdataRes.status();  
  }
  auto fdata(fdataRes.ValueOrDie());

  // Step 3. writelock the lock for this path
  auto fLockRes(lockManager_->FetchLock(pathname)); 
  if (!fLockRes.ok()) {
    return fLockRes.status();
  }
 
  auto fLock(fLockRes.ValueOrDie());
  absl::WriterMutexLock anchor(fLock);

  // Step 4. compute a new chunk handle, and insert the (chunk_index, chunkHandle)
  //         pair to fileMetadata_
  std::string newChunkHandle(AllocateNewChunkHandle());
  fdata->set_filename(pathname);
  auto& chunk_handle_map(*fdata->mutable_chunk_handles());
  
  // Return null UIDD if this chunk_index exists
  if(chunk_handle_map.contains(chunk_index)) {
     return google::protobuf::util::Status(
              google::protobuf::util::error::OUT_OF_RANGE,
                 "Chunk " + std::to_string(chunk_index) + "already exists in file " 
                 + pathname);
  }
   
  chunk_handle_map[chunk_index]= newChunkHandle;
  return newChunkHandle;
}

void MetadataManager::DeleteFile(const std::string& pathname) {
   // [TODO]: phase 2
}

std::string MetadataManager::AllocateNewChunkHandle() {
  auto ret(globalChunkId_.fetch_add(1));
  return std::to_string(ret);
}

MetadataManager* MetadataManager::GetInstance() {
  static MetadataManager* metadataManager = new MetadataManager();
  return metadataManager;
}

} /* namespace server */
} /* namespace gfs */
