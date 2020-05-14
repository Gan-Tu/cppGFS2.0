#include "metadata_manager.h"
#include <stack>

using protos::FileMetadata;

namespace gfs {
namespace server {

MetadataManager::MetadataManager() {
  lock_manager_ = LockManager::GetInstance();
  file_metadata_lock_ = new absl::Mutex();
}

google::protobuf::util::Status MetadataManager::CreateFileMetadata(
  const std::string& filename) {
  
  // Step 1. Lock (readerlock underneath) the parent directory first
  ParentLocksAnchor parentLockAnchor(lock_manager_, filename);
             
  if (!parentLockAnchor.ok()) {
    // If this operation fails, which means some of the parent directory does not
    // exist, we return false
    return parentLockAnchor.status();
  }

  // Step 2. Add a new lock for this new file, and writeLock it
  auto path_lock_or(lock_manager_->CreateLock(filename));
  if (!path_lock_or.ok()) {
    // We still need to check whether the return is NULL because another thread
    // could well successfully created a new lock for the same path
    return path_lock_or.status();
  }
  
  absl::WriterMutexLock path_writer_lock_guard(path_lock_or.ValueOrDie());
  // Step 3. writeLock the global lock, instantiate a FileMetadata object
  absl::WriterMutexLock file_metadata_writer_lock_guard(file_metadata_lock_);
  // The reason that we acquire the global lock is that we need to synchronization
  // between write and read from the fileMetadata collection.
  if(file_metadata_.contains(filename)) {
    return google::protobuf::util::Status(
             google::protobuf::util::error::ALREADY_EXISTS,
             "File metadata already exists for " + filename);
  }
   
  file_metadata_[filename] = std::make_shared<FileMetadata>();
  // Initialize the filename
  file_metadata_[filename]->set_filename(filename);
  return google::protobuf::util::Status::OK;
}

bool MetadataManager::ExistFileMetadata(const std::string& filename) const {
  absl::ReaderMutexLock reader_lock_guard(file_metadata_lock_);
  return file_metadata_.contains(filename);
}

google::protobuf::util::StatusOr<std::shared_ptr<FileMetadata>> 
  MetadataManager::GetFileMetadata(const std::string& filename) const {
  // readLock the global lock and retrieve the filemetadata. The reason for a
  // readLock is because we are not mutating anything in the file_metadata_.
  absl::ReaderMutexLock reader_lock_guard(file_metadata_lock_);
  if (!file_metadata_.contains(filename)) {
     return google::protobuf::util::Status(
              google::protobuf::util::error::NOT_FOUND,
              "File metadata does not exist: " + filename);
  }
  return file_metadata_.at(filename);
}

google::protobuf::util::StatusOr<std::string> 
  MetadataManager::CreateChunkHandle(const std::string& filename, 
                                     uint32_t chunk_index) {
  // Step 1. readlock the parent directories
  ParentLocksAnchor parentLockAnchor(lock_manager_, filename);
  if (!parentLockAnchor.ok()) {
    // If this operation fails, which means some of the parent directory does not
    // exist, we return false
    return parentLockAnchor.status();
  }

  // Step 2. readlock the global lock, fetch the data and unlock readerlock
  auto file_metadata_or(GetFileMetadata(filename));
  if (!file_metadata_or.ok()) {
    return file_metadata_or.status();  
  }
  std::shared_ptr<protos::FileMetadata> file_metadata(file_metadata_or.ValueOrDie());

  // Step 3. writelock the lock for this path
  auto file_metadata_lock_or(lock_manager_->FetchLock(filename)); 
  if (!file_metadata_lock_or.ok()) {
    return file_metadata_lock_or.status();
  }
 
  absl::WriterMutexLock file_metadata_writer_lock_guard(
                           file_metadata_lock_or.ValueOrDie());

  // Step 4. compute a new chunk handle, and insert the (chunk_index, chunkHandle)
  //         pair to file_metadata_
  std::string new_chunk_handle(AllocateNewChunkHandle());
  file_metadata->set_filename(filename);
  auto& chunk_handle_map(*file_metadata->mutable_chunk_handles());
  
  // Return null UIDD if this chunk_index exists
  if(chunk_handle_map.contains(chunk_index)) {
     return google::protobuf::util::Status(
              google::protobuf::util::error::ALREADY_EXISTS,
                 "Chunk " + std::to_string(chunk_index) + "already exists in file " 
                 + filename);
  }
   
  chunk_handle_map[chunk_index] = new_chunk_handle;
  return new_chunk_handle;
}

void MetadataManager::DeleteFile(const std::string& filename) {
   // [TODO]: phase 2
}

std::string MetadataManager::AllocateNewChunkHandle() {
  auto ret(global_chunk_id_.fetch_add(1));
  return std::to_string(ret);
}

MetadataManager* MetadataManager::GetInstance() {
  static MetadataManager* metadataManager = new MetadataManager();
  return metadataManager;
}

} /* namespace server */
} /* namespace gfs */
