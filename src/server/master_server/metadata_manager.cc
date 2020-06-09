#include "metadata_manager.h"

#include <stack>
#include <thread>

using protos::FileMetadata;

namespace gfs {
namespace server {

MetadataManager::MetadataManager() {
  lock_manager_ = LockManager::GetInstance();
}

google::protobuf::util::Status MetadataManager::CreateFileMetadata(
    const std::string& filename) {
  // There is a series of lock operations in this function and the
  // CreateChunkHandle below, and it is important to have a consisten
  // sequence of lock acquisition. See below
  // 1) acquire read locks for all parent directories
  // 2) acquire write lock for the current file
  // 3) acquire write lock in for file_metadata_lock's submap

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
    if (path_lock_or.status().error_code() == 
            google::protobuf::util::error::ALREADY_EXISTS) {
      // If lock creation fail due to ALREADY_EXISTS, we fetch the lock. 
      // We do so because we support file metadata deletion and re-creation, 
      // and since we do not delete locks (doing so would make things even more
      // complex), the line below would be always be successful. 
      path_lock_or = lock_manager_->FetchLock(filename);
    } else {
      return path_lock_or.status();
    }
  }

  absl::WriterMutexLock path_writer_lock_guard(path_lock_or.ValueOrDie());

  // Step 3. Instantiate a FileMetadata object.
  auto new_file_metadata(std::make_shared<FileMetadata>());
  // Initialize filename
  new_file_metadata->set_filename(filename);
  // Create the new file metadata in the submap

  auto try_create_file_metadata(
      file_metadata_.TryInsert(filename, new_file_metadata));

  if (!try_create_file_metadata) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::ALREADY_EXISTS,
        "File metadata already exists for " + filename);
  }
  return google::protobuf::util::Status::OK;
}

bool MetadataManager::ExistFileMetadata(const std::string& filename) {
  return file_metadata_.Contains(filename);
}

google::protobuf::util::StatusOr<std::shared_ptr<FileMetadata>>
MetadataManager::GetFileMetadata(const std::string& filename) {
  auto try_get_file_metadata(file_metadata_.TryGetValue(filename));

  if (!try_get_file_metadata.second) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::NOT_FOUND,
        "File metadata does not exist: " + filename);
  }
  return try_get_file_metadata.first;
}

google::protobuf::util::StatusOr<std::string>
MetadataManager::CreateChunkHandle(const std::string& filename,
                                   uint32_t chunk_index) {
  std::string new_chunk_handle;

  // Note that we introduce a sub-scope below so that a series of locks
  // can be released. The first step of this function is to add a new
  // chunk handle to the designated file metadata. After this step,
  // we release all the locks that have been acquired. Then in the second
  // step, we initialize the chunk metadata and add it to the chunk
  // metadata collection. The second step purely operates on the chunk
  // level and no file-level locks are needed.
  {
    // Step 1. readlock the parent directories
    ParentLocksAnchor parentLockAnchor(lock_manager_, filename);
    if (!parentLockAnchor.ok()) {
      // If this operation fails, which means some of the parent directory
      // does not exist, we return false
      return parentLockAnchor.status();
    }

    // Step 2. writelock the lock for this path
    auto path_lock_or(lock_manager_->FetchLock(filename));
    if (!path_lock_or.ok()) {
      return path_lock_or.status();
    }

    // This writer lock is to protect the creation of a chunk handle for
    // the given file
    absl::WriterMutexLock path_writer_lock_guard(path_lock_or.ValueOrDie());

    // Step 3. fetch the file metadata
    auto file_metadata_or(GetFileMetadata(filename));
    if (!file_metadata_or.ok()) {
      return file_metadata_or.status();
    }
    auto file_metadata(file_metadata_or.ValueOrDie());

    // Step 4. compute a new chunk handle, and insert the (chunk_index,
    // chunkHandle)
    new_chunk_handle = AllocateNewChunkHandle();
    file_metadata->set_filename(filename);
    auto& chunk_handle_map(*file_metadata->mutable_chunk_handles());

    // Return null UUID if this chunk_index exists
    if (chunk_handle_map.contains(chunk_index)) {
      return google::protobuf::util::Status(
          google::protobuf::util::error::ALREADY_EXISTS,
          "Chunk " + std::to_string(chunk_index) + "already exists in file " +
              filename);
    }

    chunk_handle_map[chunk_index] = new_chunk_handle;
  }

  protos::FileChunkMetadata new_chunk_metadata;
  // Initialize chunk handle field and leave other fields default
  new_chunk_metadata.set_chunk_handle(new_chunk_handle);
  SetFileChunkMetadata(new_chunk_metadata);

  return new_chunk_handle;
}

google::protobuf::util::StatusOr<std::string> MetadataManager::GetChunkHandle(
    const std::string& filename, uint32_t chunk_index) {
  // Step 1. readlock the parent directories
  ParentLocksAnchor parentLockAnchor(lock_manager_, filename);
  if (!parentLockAnchor.ok()) {
    // If this operation fails, which means some of the parent directory does
    // not exist, we return false
    return parentLockAnchor.status();
  }

  // Step 2. readerlock the lock for this path
  auto path_lock_or(lock_manager_->FetchLock(filename));
  if (!path_lock_or.ok()) {
    return path_lock_or.status();
  }

  absl::ReaderMutexLock path_reader_lock_guard(path_lock_or.ValueOrDie());

  // Step 3. fetch the file metadata
  auto file_metadata_or(GetFileMetadata(filename));
  if (!file_metadata_or.ok()) {
    return file_metadata_or.status();
  }
  auto file_metadata(file_metadata_or.ValueOrDie());

  // Step 4. fetch the chunk handle
  auto const& chunk_handle_map(file_metadata->chunk_handles());

  // If chunk_index does not exist, return error
  if (!chunk_handle_map.contains(chunk_index)) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::NOT_FOUND,
        "Chunk " + std::to_string(chunk_index) + "not found in file " +
            filename);
  }

  return chunk_handle_map.at(chunk_index);
}

google::protobuf::util::Status MetadataManager::AdvanceChunkVersion(
    const std::string& chunk_handle) {
  auto chunk_data_or(GetFileChunkMetadata(chunk_handle));
  if (!chunk_data_or.ok()) {
    return chunk_data_or.status();
  }

  // Note that we do not fully support concurrent advancement of the same
  // chunk's version number, as GFS does not support arbitrary concurrent
  // write. Therefore, here we simply increment the chunk's version number
  // and update the FileChunkMetadata
  protos::FileChunkMetadata chunk_data(chunk_data_or.ValueOrDie());
  chunk_data.set_version(chunk_data.version() + 1);
  SetFileChunkMetadata(chunk_data);

  return google::protobuf::util::Status::OK;
}

bool MetadataManager::ExistFileChunkMetadata(const std::string& chunk_handle) {
  return chunk_metadata_.Contains(chunk_handle);
}

google::protobuf::util::StatusOr<protos::FileChunkMetadata>
MetadataManager::GetFileChunkMetadata(const std::string& chunk_handle) {
  auto try_get_chunk_data(chunk_metadata_.TryGetValue(chunk_handle));

  if (!try_get_chunk_data.second) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::NOT_FOUND,
        "Chunk handle " + chunk_handle + "'s metadata not found.");
  }

  return try_get_chunk_data.first;
}

void MetadataManager::SetFileChunkMetadata(
    const protos::FileChunkMetadata& chunk_data) {
  const std::string& chunk_handle(chunk_data.chunk_handle());
  chunk_metadata_.SetValue(chunk_handle, chunk_data);
}

void MetadataManager::DeleteFileChunkMetadata(const std::string& chunk_handle) {
  chunk_metadata_.Erase(chunk_handle);
}

void MetadataManager::SetPrimaryLeaseMetadata(
    const std::string& chunk_handle,
    const protos::ChunkServerLocation& server_location,
    const uint64_t expiration_unix_sec) {
  lease_holders_.SetValue(chunk_handle,
                          std::pair<protos::ChunkServerLocation, uint64_t>(
                              server_location, expiration_unix_sec));
}

void MetadataManager::RemovePrimaryLeaseMetadata(
    const std::string& chunk_handle) {
  lease_holders_.Erase(chunk_handle);
}

std::pair<std::pair<protos::ChunkServerLocation, uint64_t>, bool>
MetadataManager::GetPrimaryLeaseMetadata(const std::string& chunk_handle) {
  return lease_holders_.TryGetValue(chunk_handle);
}

// Delete the file metadata, furthermore, delete all chunk handles assocated
// with that file metadata, this means all the associated chunk metadata
// are removed from the metadata manager. The chunk server will detect the 
// corresonding chunk handle is no longer existing and therefore garbage collect
// them when finding them out via heartbeat mechanism.
// Note that we do not rename upon deletion as described from the paper. 
void MetadataManager::DeleteFileMetadata(const std::string& filename) {
  // Step 1. readlock the parent directories
  ParentLocksAnchor parentLockAnchor(lock_manager_, filename);
  if (!parentLockAnchor.ok()) {
    // If this operation fails, which means some of the parent directory
    // does not exist, we just return as the deletion is a no-op
    return;
  }

  // Step 2. writelock the lock for this path
  auto path_lock_or(lock_manager_->FetchLock(filename));
  if (!path_lock_or.ok()) {
    return;
  }

  // This writer lock to protect this file, as we are deleting it
  absl::WriterMutexLock path_writer_lock_guard(path_lock_or.ValueOrDie());

  // Step 3. fetch the file metadata
  auto file_metadata_or(GetFileMetadata(filename));
  if (!file_metadata_or.ok()) {
    return;
  }
  auto file_metadata(file_metadata_or.ValueOrDie());

  // Now we can remove the smart pointer of the file metadata from the 
  // file_metadata collection
  file_metadata_.Erase(filename);

  // We can still access the file metadata as the shared pointer anchored it
  // Delete all the file chunk metadata
  for (auto& chunk_index_and_chunk_handle : file_metadata->chunk_handles()) {
    DeleteFileChunkMetadata(chunk_index_and_chunk_handle.second);  
  }
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
