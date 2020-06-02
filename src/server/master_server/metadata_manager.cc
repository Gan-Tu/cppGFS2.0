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
    return path_lock_or.status();
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
          "Chunk " + std::to_string(chunk_index) + "already exists in file " 
              + filename);
    }

    chunk_handle_map[chunk_index] = new_chunk_handle;
  } 

  protos::FileChunkMetadata new_chunk_metadata;
  // Initialize chunk handle field and leave other fields default
  new_chunk_metadata.set_chunk_handle(new_chunk_handle);
  SetFileChunkMetadata(new_chunk_metadata);

  // TODO(Xi,Michael): Call ChunkServerManager::AllocateChunkServers to 
  // allocate chunk servers for this new chunk handle

  return new_chunk_handle;
}

google::protobuf::util::StatusOr<std::string>
MetadataManager::GetChunkHandle(
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

google::protobuf::util::Status
MetadataManager::AdvanceChunkVersion(const std::string& chunk_handle) {
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

void
MetadataManager::SetFileChunkMetadata(
    const protos::FileChunkMetadata& chunk_data) {
  const std::string& chunk_handle(chunk_data.chunk_handle());
  chunk_metadata_.SetValue(chunk_handle, chunk_data);
}

google::protobuf::util::Status
MetadataManager::SetPrimaryChunkServerLocation(
    const std::string& chunk_handle, 
    const protos::ChunkServerLocation& server_location) {
  auto chunk_data_or(GetFileChunkMetadata(chunk_handle));
  if (!chunk_data_or.ok()) {
    return chunk_data_or.status();
  }

  protos::FileChunkMetadata chunk_data(chunk_data_or.ValueOrDie());
  // Note: no set_primary_location function is generated as proto treats
  // ChunkServerLocation as a non-trivial type and uses pointer to store
  // it underneath, this type of syntax will likely occur frequently
  (*chunk_data.mutable_primary_location()) = server_location;
 
  SetFileChunkMetadata(chunk_data);
  return google::protobuf::util::Status::OK;
}

google::protobuf::util::Status
MetadataManager::RemovePrimaryChunkServerLocation(
    const std::string& chunk_handle) {
  // Remove the primary chunk server location by setting it to a default 
  // location, which has a null server host
  return SetPrimaryChunkServerLocation(
             chunk_handle, protos::ChunkServerLocation()); 
}

// TODO(Xi): In phase 1 the deletion of file is not fully supported but
// it would be good to lay out a plan as deletion involves removing items
// from the shared states. When DeleteFileMetadata is called, the following
// steps should be taken:
// 1) Loop over all chunk handles for a file, and mark them as deleted 
// by inserting them to deleted_chunk_handles
// 2) Mark their entries in chunk_metadata_ as default 
//
// We defer the removal of these entries from the above collections to
// when the chunks have been garbage collected. By then we
// 1) Remove the entry in file_metadata_ (TODO) there is still a bit of 
// detailed question here as to how to detect the filename when garbage
// collection occurs on chunk level
// 2) Remove the entry in chunk_version_ and chunk_metadata_
void MetadataManager::DeleteFileMetadata(const std::string& filename) {
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
