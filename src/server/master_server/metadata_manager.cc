#include "metadata_manager.h"

#include <stack>
#include <thread>

using protos::FileMetadata;

namespace gfs {
namespace server {

MetadataManager::MetadataManager() {
  lock_manager_ = LockManager::GetInstance();
  // Set the submap number and initialize locks and each submap
  num_of_submap_ = std::max(std::thread ::hardware_concurrency(),
                       (unsigned int)1);
  file_metadata_lock_ = std::vector<absl::Mutex*>(num_of_submap_, 
                            new absl::Mutex());
  file_metadata_ = std::vector<absl::flat_hash_map<std::string,
                       std::shared_ptr<protos::FileMetadata>>>(num_of_submap_);
  chunk_metadata_lock_ = std::vector<absl::Mutex*>(num_of_submap_,
                            new absl::Mutex());
  chunk_metadata_ = std::vector<absl::flat_hash_map<std::string,
                        protos::FileChunkMetadata>>(num_of_submap_);
}

inline size_t MetadataManager::submap_id(const std::string& filename) const {
  // Compute the hash of given string, and mod the number of submaps
  return std::hash<std::string>{}(filename) % num_of_submap_;
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
  auto map_id(submap_id(filename));
  absl::Mutex* file_lock(file_metadata_lock_[map_id]);
  // Acquire a writer lock as we are creating a file metadata in the 
  // submap underneath
  absl::WriterMutexLock file_write_lock_guard(file_lock);

  if (file_metadata_[map_id].contains(filename)) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::ALREADY_EXISTS,
        "File metadata already exists for " + filename);
  }

  auto new_file_metadata(std::make_shared<FileMetadata>());
  // Initialize filename
  new_file_metadata->set_filename(filename);
  // Create the new file metadata in the submap
  file_metadata_[map_id][filename] = new_file_metadata;  

  return google::protobuf::util::Status::OK;
}

bool MetadataManager::ExistFileMetadata(const std::string& filename) const {
  auto map_id(submap_id(filename));
  absl::Mutex* file_lock(file_metadata_lock_[map_id]);
  // Acquire a reader lock for lookup
  absl::ReaderMutexLock file_read_lock_guard(file_lock);
  return file_metadata_[map_id].contains(filename);
}

google::protobuf::util::StatusOr<std::shared_ptr<FileMetadata>>
MetadataManager::GetFileMetadata(const std::string& filename) const {
  auto map_id(submap_id(filename));
  absl::Mutex* file_lock(file_metadata_lock_[map_id]);
  // Acquire a reader lock for lookup
  absl::ReaderMutexLock file_read_lock_guard(file_lock);
  if (!file_metadata_[map_id].contains(filename)) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::NOT_FOUND,
        "File metadata does not exist: " + filename);
  }
  return file_metadata_[map_id].at(filename);
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
    auto map_id(submap_id(filename));
    absl::Mutex* file_lock(file_metadata_lock_[map_id]);
    // Acquire a reader lock as we are fetching the reference for filename
    absl::ReaderMutexLock file_read_lock_guard(file_lock);

    if (!file_metadata_[map_id].contains(filename)) {
      return google::protobuf::util::Status(
          google::protobuf::util::error::NOT_FOUND,
          "File metadata does not exist: " + filename);
    }

    // Note that it may be tempting to just call GetFileMetadata here, but
    // the nuance is that this changes the life time of the lock acquisition
    // It is a bit unclear whether such operation can cause issue, and 
    // to play safe, we keep file_read_lock_guard around for a bit. 
    auto file_metadata(file_metadata_[map_id].at(filename)); 

    // Step 4. compute a new chunk handle, and insert the (chunk_index,
    // chunkHandle)
    new_chunk_handle = AllocateNewChunkHandle();
    file_metadata->set_filename(filename);
    auto& chunk_handle_map(*file_metadata->mutable_chunk_handles());

    // Return null UIDD if this chunk_index exists
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

  // Step 2. readerlock the lock for this path
  auto path_lock_or(lock_manager_->FetchLock(filename));
  if (!path_lock_or.ok()) {
    return path_lock_or.status();
  }

  absl::ReaderMutexLock path_reader_lock_guard(path_lock_or.ValueOrDie());
  
  // Step 3. fetch the file metadata
  auto map_id(submap_id(filename));
  absl::Mutex* file_lock(file_metadata_lock_[map_id]);
  // Acquire a reader lock as we are fetching the reference for filename
  absl::ReaderMutexLock file_read_lock_guard(file_lock);

  if (!file_metadata_[map_id].contains(filename)) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::NOT_FOUND,
        "File metadata does not exist: " + filename);
  }
  auto file_metadata(file_metadata_[map_id].at(filename)); 

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
  auto map_id(submap_id(chunk_handle));
  absl::Mutex* chunk_lock(chunk_metadata_lock_[map_id]);
  // Acquire a reader lock for lookup
  absl::ReaderMutexLock chunk_read_lock_guard(chunk_lock);

  if (!chunk_metadata_[map_id].contains(chunk_handle)) { 
    return google::protobuf::util::Status(
        google::protobuf::util::error::NOT_FOUND,
        "Chunk handle " + chunk_handle + "'s metadata not found.");
  }

  return chunk_metadata_[map_id].at(chunk_handle); 
}

void
MetadataManager::SetFileChunkMetadata(
    const protos::FileChunkMetadata& chunk_data) {
  const std::string& chunk_handle(chunk_data.chunk_handle());
  auto map_id(submap_id(chunk_handle));
  absl::Mutex* chunk_lock(chunk_metadata_lock_[map_id]);
  // Acquire a writer lock for updates
  absl::WriterMutexLock chunk_write_lock_guard(chunk_lock);

  chunk_metadata_[map_id][chunk_handle] = chunk_data;
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
