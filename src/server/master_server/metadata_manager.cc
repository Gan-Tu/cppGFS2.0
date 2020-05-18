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
  // Initialize filename
  new_file_metadata->set_filename(filename);
  
  auto new_file_metadata_and_if_took_place(
           file_metadata_.emplace(filename, new_file_metadata));
  auto has_create_taken_place(new_file_metadata_and_if_took_place.second);

  if (!has_create_taken_place) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::ALREADY_EXISTS,
        "File metadata already exists for " + filename);
  }

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
  
  protos::FileChunkMetadata new_chunk_metadata;
  // Initialize chunk handle field and leave other fields default
  new_chunk_metadata.set_chunk_handle(new_chunk_handle);
  chunk_metadata_[new_chunk_handle] = new_chunk_metadata;

  return new_chunk_handle;
}

google::protobuf::util::StatusOr<std::string>
MetadataManager::GetChunkHandle(
    const std::string& filename, uint32_t chunk_index) const {
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

  // Step 3. readerlock the lock for this path
  auto file_path_lock_or(lock_manager_->FetchLock(filename));
  if (!file_path_lock_or.ok()) {
    return file_path_lock_or.status();
  }

  absl::ReaderMutexLock file_metadata_writer_lock_guard(
      file_path_lock_or.ValueOrDie());
  
  // Step 4. fetch the chunk handle
  auto const& chunk_handle_map(file_metadata->chunk_handles());

  // If chunk_inde does not exist, return error
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
MetadataManager::GetFileChunkMetadata(const std::string& chunk_handle) const {
  if (!chunk_metadata_.contains(chunk_handle)) { 
    return google::protobuf::util::Status(
        google::protobuf::util::error::NOT_FOUND,
        "Chunk handle " + chunk_handle + "'s metadata not found.");
  }

  return chunk_metadata_.at(chunk_handle); 
}

void
MetadataManager::SetFileChunkMetadata(
    const protos::FileChunkMetadata& chunk_data) {
  const std::string& chunk_handle(chunk_data.chunk_handle());
  chunk_metadata_[chunk_handle] = chunk_data;
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
  
  // If the server_location is non-null (null corresponds to a removal), 
  // loop over the locations field and append the new server location
  // if it is not in that list
  if (!server_location.server_hostname().empty()) {
    bool append_to_locations(true);
    for (int i = 0; i < chunk_data.locations_size(); i++) {
      if (chunk_data.locations(i).server_hostname() == 
          server_location.server_hostname() && 
          chunk_data.locations(i).server_port() == 
          server_location.server_port()) {
        append_to_locations = false;
        break;
      }
    }
  
    if (append_to_locations) {
      // Note that there is a rvalue binding issue if you just pass 
      // "server_location" as is below. 
      chunk_data.mutable_locations()->Add(
          protos::ChunkServerLocation(server_location));
    }
  }
  
  SetFileChunkMetadata(chunk_data);
  return google::protobuf::util::Status::OK;
}

google::protobuf::util::Status
MetadataManager::RemovePrimaryChunkServerLocation(
    const std::string& chunk_handle) {
  // Remove the primary chunk server location by setting it to a default 
  // loation, which has a null server host
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
// 1) Remove the entry in file_metadata_ (TODO) there is stil a bit of 
// detailed quesiton here as to how to detect the filename when garbage
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
