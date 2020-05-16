#include "metadata_manager.h"

#include <stack>

using protos::FileMetadata;

namespace gfs {
namespace server {

MetadataManager::MetadataManager() {
  lock_manager_ = LockManager::GetInstance();
}

google::protobuf::util::Status MetadataManager::CreateFileMetadata(
    const std::string& filename) {
  // Step 1. Lock (readerlock underneath) the parent directory first
  ParentLocksAnchor parentLockAnchor(lock_manager_, filename);

  if (!parentLockAnchor.ok()) {
    // If this operation fails, which means some of the parent directory does
    // not exist, we return false
    return parentLockAnchor.status();
  }

  // Step 2. Add a new lock for this new file, and writeLock it
  auto path_lock_or(lock_manager_->CreateLock(filename));
  if (!path_lock_or.ok()) {
    return path_lock_or.status();
  }

  absl::WriterMutexLock path_writer_lock_guard(path_lock_or.ValueOrDie());
  
  // Step 3. Instantiate a FileMetadata object. Use the emplace function 
  // to detect if the creation took place (as there can be concurrent
  // creation in rare cases, and only one succeeds). 
  auto new_file_metadata(std::make_shared<FileMetadata>());
  auto new_file_metadata_and_if_took_place(
           file_metadata_.emplace(filename, new_file_metadata));
  auto has_create_taken_place(new_file_metadata_and_if_took_place.second);

  if (!has_create_taken_place) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::ALREADY_EXISTS,
        "File metadata already exists for " + filename);
  }

  // Initialize the filename
  new_file_metadata->set_filename(filename);
  return google::protobuf::util::Status::OK;
}

bool MetadataManager::ExistFileMetadata(const std::string& filename) const {
  return file_metadata_.contains(filename);
}

google::protobuf::util::StatusOr<std::shared_ptr<FileMetadata>>
MetadataManager::GetFileMetadata(const std::string& filename) const {
  // Note that there is a null check for the filemetadata below. See 
  // comments in the DeleteFile function. This is to deal with deletion,
  // if we simply remove the item from file_metadata_ when we delete,
  // we may run into time-of-check-time-of-use issue (though rare) 
  // in this function because you can check it and find it exists but
  // then access it and find it not. What we can do instead is to
  // replace the share_ptr with a default one (with a null raw ptr
  // underneath) upon deletion. This way, the .at() function succeeds. 
  // Regarding to the final removal of this filename, we defer it 
  // until the final garbage collection of the file chunks. 
  if (!file_metadata_.contains(filename) || !file_metadata_.at(filename)) {
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
    // If this operation fails, which means some of the parent directory does
    // not exist, we return false
    return parentLockAnchor.status();
  }

  // Step 2. fetch the file metadata
  auto file_metadata_or(GetFileMetadata(filename));
  if (!file_metadata_or.ok()) {
    return file_metadata_or.status();
  }
  std::shared_ptr<protos::FileMetadata> file_metadata(
      file_metadata_or.ValueOrDie());

  // Step 3. writelock the lock for this path
  auto file_path_lock_or(lock_manager_->FetchLock(filename));
  if (!file_path_lock_or.ok()) {
    return file_path_lock_or.status();
  }

  absl::WriterMutexLock file_metadata_writer_lock_guard(
      file_path_lock_or.ValueOrDie());

  // Step 4. compute a new chunk handle, and insert the (chunk_index,
  // chunkHandle)
  std::string new_chunk_handle(AllocateNewChunkHandle());
  file_metadata->set_filename(filename);
  auto& chunk_handle_map(*file_metadata->mutable_chunk_handles());

  // Return null UIDD if this chunk_index exists
  if (chunk_handle_map.contains(chunk_index)) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::ALREADY_EXISTS,
        "Chunk " + std::to_string(chunk_index) + "already exists in file " +
            filename);
  }

  chunk_handle_map[chunk_index] = new_chunk_handle;
  return new_chunk_handle;
}

// TODO(Xi): In phase 1 the deletion of file is not fully supported but
// it would be good to lay out a plan as deletion involves removing items
// from the shared states. For Filemetadata, the proposed plan here is 
// to replace the File metadata with a default shared_ptr, and defer
// the final cleanup to the point when the chunks have been garbage
// collected. 
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
