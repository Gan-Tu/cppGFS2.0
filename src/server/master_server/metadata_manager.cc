#include "metadata_manager.h"

#include <filesystem>
#include <stack>
#include <thread>

#include "absl/strings/numbers.h"
#include "src/common/system_logger.h"

using protos::FileMetadata;

namespace gfs {
namespace server {

using absl::AlreadyExistsError;
using absl::InternalError;
using absl::IsAlreadyExists;
using absl::NotFoundError;
using absl::OkStatus;

namespace {
// Key prefixes for the on-disk metadata store. The store plays the role of
// the operation log + checkpoint from section 2.6.3 of the GFS paper: file
// namespace entries and chunk versions are written through synchronously and
// recovered on startup.
const char kFileMetadataKeyPrefix[] = "file/";
const char kChunkMetadataKeyPrefix[] = "chunk/";
const char kChunkHandleAllocatorKey[] = "sys/next_chunk_id";

std::string FileMetadataKey(const std::string& filename) {
  return kFileMetadataKeyPrefix + filename;
}

std::string ChunkMetadataKey(const std::string& chunk_handle) {
  return kChunkMetadataKeyPrefix + chunk_handle;
}
}  // namespace

MetadataManager::MetadataManager() {
  lock_manager_ = LockManager::GetInstance();
}

absl::Status MetadataManager::EnablePersistence(const std::string& db_path) {
  // LevelDB creates the database directory itself but not its parents
  std::error_code error_code;
  const auto parent_dir = std::filesystem::path(db_path).parent_path();
  if (!parent_dir.empty()) {
    std::filesystem::create_directories(parent_dir, error_code);
  }

  leveldb::DB* database;
  leveldb::Options options;
  options.create_if_missing = true;

  leveldb::Status open_status = leveldb::DB::Open(options, db_path, &database);
  if (!open_status.ok()) {
    return InternalError("Cannot open metadata store at " + db_path + ": " +
                         open_status.ToString());
  }
  metadata_store_ = std::unique_ptr<leveldb::DB>(database);

  // Recover any previously persisted state (namespace, chunk versions, and
  // the chunk handle allocator) into memory
  uint64_t recovered_files = 0, recovered_chunks = 0;
  std::unique_ptr<leveldb::Iterator> iterator(
      metadata_store_->NewIterator(leveldb::ReadOptions()));
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    const std::string key = iterator->key().ToString();
    if (key.rfind(kFileMetadataKeyPrefix, 0) == 0) {
      auto file_metadata(std::make_shared<FileMetadata>());
      if (!file_metadata->ParseFromString(iterator->value().ToString())) {
        LOG(ERROR) << "Skipping unparsable persisted file metadata: " << key;
        continue;
      }
      const std::string& filename = file_metadata->filename();
      file_metadata_.SetValue(filename, file_metadata);
      // Recreate the namespace lock for this file, as CreateFileMetadata
      // would have done originally
      auto lock_or = lock_manager_->CreateLock(filename);
      if (!lock_or.ok() && !IsAlreadyExists(lock_or.status())) {
        return lock_or.status();
      }
      ++recovered_files;
    } else if (key.rfind(kChunkMetadataKeyPrefix, 0) == 0) {
      protos::FileChunkMetadata chunk_metadata;
      if (!chunk_metadata.ParseFromString(iterator->value().ToString())) {
        LOG(ERROR) << "Skipping unparsable persisted chunk metadata: " << key;
        continue;
      }
      chunk_metadata_.SetValue(chunk_metadata.chunk_handle(), chunk_metadata);
      ++recovered_chunks;
    } else if (key == kChunkHandleAllocatorKey) {
      uint64_t next_chunk_id = 0;
      if (absl::SimpleAtoi(iterator->value().ToString(), &next_chunk_id)) {
        global_chunk_id_.store(next_chunk_id);
      }
    }
  }

  LOG(INFO) << "Metadata store recovered " << recovered_files << " file(s), "
            << recovered_chunks << " chunk version record(s); next chunk id "
            << global_chunk_id_.load();
  return OkStatus();
}

absl::Status MetadataManager::PersistFileMetadata(const std::string& filename) {
  if (!metadata_store_) {
    return OkStatus();
  }
  auto file_metadata_or(GetFileMetadata(filename));
  if (!file_metadata_or.ok()) {
    return file_metadata_or.status();
  }
  leveldb::WriteOptions write_options;
  write_options.sync = true;
  leveldb::Status status =
      metadata_store_->Put(write_options, FileMetadataKey(filename),
                           file_metadata_or.value()->SerializeAsString());
  if (!status.ok()) {
    return InternalError("Cannot persist file metadata for " + filename + ": " +
                         status.ToString());
  }
  return OkStatus();
}

absl::Status MetadataManager::PersistChunkMetadata(
    const std::string& chunk_handle) {
  if (!metadata_store_) {
    return OkStatus();
  }
  auto chunk_metadata_or(GetFileChunkMetadata(chunk_handle));
  if (!chunk_metadata_or.ok()) {
    return chunk_metadata_or.status();
  }
  // Only the chunk handle and version are persistent state; replica
  // locations and the primary are runtime state that the master re-learns
  // from chunk server reports (GFS paper section 2.6.2)
  protos::FileChunkMetadata persisted_metadata;
  persisted_metadata.set_chunk_handle(chunk_metadata_or.value().chunk_handle());
  persisted_metadata.set_version(chunk_metadata_or.value().version());

  leveldb::WriteOptions write_options;
  write_options.sync = true;
  leveldb::Status status =
      metadata_store_->Put(write_options, ChunkMetadataKey(chunk_handle),
                           persisted_metadata.SerializeAsString());
  if (!status.ok()) {
    return InternalError("Cannot persist chunk metadata for " + chunk_handle +
                         ": " + status.ToString());
  }
  return OkStatus();
}

void MetadataManager::ErasePersistedFileMetadata(const std::string& filename) {
  if (!metadata_store_) {
    return;
  }
  leveldb::WriteOptions write_options;
  write_options.sync = true;
  metadata_store_->Delete(write_options, FileMetadataKey(filename));
}

void MetadataManager::ErasePersistedChunkMetadata(
    const std::string& chunk_handle) {
  if (!metadata_store_) {
    return;
  }
  leveldb::WriteOptions write_options;
  write_options.sync = true;
  metadata_store_->Delete(write_options, ChunkMetadataKey(chunk_handle));
}

void MetadataManager::PersistChunkHandleAllocator() {
  if (!metadata_store_) {
    return;
  }
  leveldb::WriteOptions write_options;
  write_options.sync = true;
  metadata_store_->Put(write_options, kChunkHandleAllocatorKey,
                       std::to_string(global_chunk_id_.load()));
}

absl::Status MetadataManager::CreateFileMetadata(const std::string& filename) {
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
    if (IsAlreadyExists(path_lock_or.status())) {
      // If lock creation fail due to ALREADY_EXISTS, we fetch the lock.
      // We do so because we support file metadata deletion and re-creation,
      // and since we do not delete locks (doing so would make things even more
      // complex), the line below would be always be successful.
      path_lock_or = lock_manager_->FetchLock(filename);
    } else {
      return path_lock_or.status();
    }
  }

  absl::WriterMutexLock path_writer_lock_guard(path_lock_or.value());

  // Step 3. Instantiate a FileMetadata object.
  auto new_file_metadata(std::make_shared<FileMetadata>());
  // Initialize filename
  new_file_metadata->set_filename(filename);
  // Create the new file metadata in the submap

  auto try_create_file_metadata(
      file_metadata_.TryInsert(filename, new_file_metadata));

  if (!try_create_file_metadata) {
    return AlreadyExistsError("File metadata already exists for " + filename);
  }

  // Write the namespace mutation through to the metadata store before
  // acknowledging it (the paper flushes the operation log record before
  // responding to the client)
  auto persist_status(PersistFileMetadata(filename));
  if (!persist_status.ok()) {
    LOG(ERROR) << "Rolling back creation of " << filename
               << " because it cannot be persisted: " << persist_status;
    file_metadata_.Erase(filename);
    return persist_status;
  }
  return OkStatus();
}

bool MetadataManager::ExistFileMetadata(const std::string& filename) {
  return file_metadata_.Contains(filename);
}

absl::StatusOr<std::shared_ptr<FileMetadata>> MetadataManager::GetFileMetadata(
    const std::string& filename) {
  auto try_get_file_metadata(file_metadata_.TryGetValue(filename));

  if (!try_get_file_metadata.second) {
    return NotFoundError("File metadata does not exist: " + filename);
  }
  return try_get_file_metadata.first;
}

absl::StatusOr<std::string> MetadataManager::CreateChunkHandle(
    const std::string& filename, uint32_t chunk_index) {
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
    absl::WriterMutexLock path_writer_lock_guard(path_lock_or.value());

    // Step 3. fetch the file metadata
    auto file_metadata_or(GetFileMetadata(filename));
    if (!file_metadata_or.ok()) {
      return file_metadata_or.status();
    }
    auto file_metadata(file_metadata_or.value());

    // Step 4. compute a new chunk handle, and insert the (chunk_index,
    // chunkHandle)
    new_chunk_handle = AllocateNewChunkHandle();
    file_metadata->set_filename(filename);
    auto& chunk_handle_map(*file_metadata->mutable_chunk_handles());

    // Return null UUID if this chunk_index exists
    if (chunk_handle_map.contains(chunk_index)) {
      return AlreadyExistsError("Chunk " + std::to_string(chunk_index) +
                                "already exists in file " + filename);
    }

    chunk_handle_map[chunk_index] = new_chunk_handle;

    // Persist the updated file-to-chunk mapping while still holding the
    // file's write lock
    auto persist_status(PersistFileMetadata(filename));
    if (!persist_status.ok()) {
      LOG(ERROR) << "Rolling back chunk handle creation for " << filename
                 << " at index " << chunk_index
                 << " because it cannot be persisted: " << persist_status;
      chunk_handle_map.erase(chunk_index);
      return persist_status;
    }
  }

  protos::FileChunkMetadata new_chunk_metadata;
  // Initialize chunk handle field and leave other fields default
  new_chunk_metadata.set_chunk_handle(new_chunk_handle);
  SetFileChunkMetadata(new_chunk_metadata);

  return new_chunk_handle;
}

void MetadataManager::DeleteChunkHandle(const std::string& filename,
                                        uint32_t chunk_index) {
  // Acquire the same locks as CreateChunkHandle, since this is its undo
  ParentLocksAnchor parentLockAnchor(lock_manager_, filename);
  if (!parentLockAnchor.ok()) {
    return;
  }

  auto path_lock_or(lock_manager_->FetchLock(filename));
  if (!path_lock_or.ok()) {
    return;
  }
  absl::WriterMutexLock path_writer_lock_guard(path_lock_or.value());

  auto file_metadata_or(GetFileMetadata(filename));
  if (!file_metadata_or.ok()) {
    return;
  }
  auto file_metadata(file_metadata_or.value());
  auto& chunk_handle_map(*file_metadata->mutable_chunk_handles());
  if (!chunk_handle_map.contains(chunk_index)) {
    return;
  }

  const std::string chunk_handle(chunk_handle_map.at(chunk_index));
  chunk_handle_map.erase(chunk_index);
  DeleteFileChunkMetadata(chunk_handle);

  auto persist_status(PersistFileMetadata(filename));
  if (!persist_status.ok()) {
    LOG(ERROR) << "Cannot persist chunk handle removal for " << filename
               << " at index " << chunk_index << ": " << persist_status;
  }
}

absl::StatusOr<std::string> MetadataManager::GetChunkHandle(
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

  absl::ReaderMutexLock path_reader_lock_guard(path_lock_or.value());

  // Step 3. fetch the file metadata
  auto file_metadata_or(GetFileMetadata(filename));
  if (!file_metadata_or.ok()) {
    return file_metadata_or.status();
  }
  auto file_metadata(file_metadata_or.value());

  // Step 4. fetch the chunk handle
  auto const& chunk_handle_map(file_metadata->chunk_handles());

  // If chunk_index does not exist, return error
  if (!chunk_handle_map.contains(chunk_index)) {
    return NotFoundError("Chunk " + std::to_string(chunk_index) +
                         "not found in file " + filename);
  }

  return chunk_handle_map.at(chunk_index);
}

absl::Status MetadataManager::AdvanceChunkVersion(
    const std::string& chunk_handle) {
  // Concurrent advancement of the same chunk's version is serialized by the
  // per-chunk lease lock held by the caller (see GetChunkLeaseLock); the
  // version only advances when a new lease is granted, per section 4.5 of
  // the GFS paper
  auto chunk_data_or(GetFileChunkMetadata(chunk_handle));
  if (!chunk_data_or.ok()) {
    return chunk_data_or.status();
  }
  return SetChunkVersion(chunk_handle, chunk_data_or.value().version() + 1);
}

absl::Status MetadataManager::SetChunkVersion(const std::string& chunk_handle,
                                              uint32_t version) {
  auto chunk_data_or(GetFileChunkMetadata(chunk_handle));
  if (!chunk_data_or.ok()) {
    return chunk_data_or.status();
  }

  protos::FileChunkMetadata chunk_data(chunk_data_or.value());
  chunk_data.set_version(version);
  // Update the in-memory record and persist exactly once, before any client
  // is notified, per section 4.5 of the GFS paper: "The master and these
  // replicas all record the new version number in their persistent state
  // ... before any client is notified". (SetFileChunkMetadata is not used
  // here because it would issue a second, redundant synchronous persist.)
  chunk_metadata_.SetValue(chunk_handle, chunk_data);
  return PersistChunkMetadata(chunk_handle);
}

bool MetadataManager::ExistFileChunkMetadata(const std::string& chunk_handle) {
  return chunk_metadata_.Contains(chunk_handle);
}

absl::StatusOr<protos::FileChunkMetadata>
MetadataManager::GetFileChunkMetadata(const std::string& chunk_handle) {
  auto try_get_chunk_data(chunk_metadata_.TryGetValue(chunk_handle));

  if (!try_get_chunk_data.second) {
    return NotFoundError("Chunk handle " + chunk_handle +
                         "'s metadata not found.");
  }

  return try_get_chunk_data.first;
}

void MetadataManager::SetFileChunkMetadata(
    const protos::FileChunkMetadata& chunk_data) {
  const std::string& chunk_handle(chunk_data.chunk_handle());
  chunk_metadata_.SetValue(chunk_handle, chunk_data);

  auto persist_status(PersistChunkMetadata(chunk_handle));
  if (!persist_status.ok()) {
    LOG(ERROR) << "Cannot persist chunk metadata for " << chunk_handle << ": "
               << persist_status;
  }
}

void MetadataManager::DeleteFileChunkMetadata(const std::string& chunk_handle) {
  chunk_metadata_.Erase(chunk_handle);
  ErasePersistedChunkMetadata(chunk_handle);
}

void MetadataManager::SetPrimaryLeaseMetadata(
    const std::string& chunk_handle,
    const protos::ChunkServerLocation& location,
    const uint64_t expiration_unix_sec) {
  lease_holders_.SetValue(chunk_handle,
                          std::pair<protos::ChunkServerLocation, uint64_t>(
                              location, expiration_unix_sec));
}

void MetadataManager::RemovePrimaryLeaseMetadata(
    const std::string& chunk_handle) {
  lease_holders_.Erase(chunk_handle);
  lease_replicas_.Erase(chunk_handle);
}

std::pair<std::pair<protos::ChunkServerLocation, uint64_t>, bool>
MetadataManager::GetPrimaryLeaseMetadata(const std::string& chunk_handle) {
  return lease_holders_.TryGetValue(chunk_handle);
}

absl::Mutex* MetadataManager::GetChunkLeaseLock(
    const std::string& chunk_handle) {
  return chunk_lease_locks_
      .GetOrCreate(chunk_handle, [] { return std::make_shared<absl::Mutex>(); })
      .get();
}

void MetadataManager::SetPrimaryLeaseReplicas(
    const std::string& chunk_handle,
    const std::vector<protos::ChunkServerLocation>& replicas) {
  lease_replicas_.SetValue(chunk_handle, replicas);
}

std::vector<protos::ChunkServerLocation>
MetadataManager::GetPrimaryLeaseReplicas(const std::string& chunk_handle) {
  return lease_replicas_.TryGetValue(chunk_handle).first;
}

// Delete the file metadata, furthermore, delete all chunk handles assocated
// with that file metadata, this means all the associated chunk metadata
// are removed from the metadata manager. The chunk servers will detect that
// the corresponding chunk handles no longer exist when they periodically
// report their stored chunks, and garbage collect the actual chunk data
// (GFS paper section 4.4: the identity of deleted chunks is exchanged in the
// regular report messages, and each chunk server is then free to delete its
// replicas). Note that we do not rename upon deletion as described from the
// paper; the namespace entry is removed immediately while the chunk data is
// reclaimed lazily.
void MetadataManager::DeleteFileAndChunkMetadata(const std::string& filename) {
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
  absl::WriterMutexLock path_writer_lock_guard(path_lock_or.value());

  // Step 3. fetch the file metadata
  auto file_metadata_or(GetFileMetadata(filename));
  if (!file_metadata_or.ok()) {
    return;
  }
  auto file_metadata(file_metadata_or.value());

  // Now we can remove the smart pointer of the file metadata from the
  // file_metadata collection
  file_metadata_.Erase(filename);
  ErasePersistedFileMetadata(filename);

  // We can still access the file metadata as the shared pointer anchored it
  // Delete all the file chunk metadata
  for (auto& chunk_index_and_chunk_handle : file_metadata->chunk_handles()) {
    DeleteFileChunkMetadata(chunk_index_and_chunk_handle.second);
  }
}

std::string MetadataManager::AllocateNewChunkHandle() {
  // Allocation and persistence happen under one mutex so the persisted
  // watermark can never be overwritten by a smaller value from a concurrent
  // allocation — otherwise a crash could recover a counter below an
  // already-issued handle and re-issue it, violating "chunks are uniquely
  // and eternally identified" (GFS paper section 2.6.3)
  absl::MutexLock allocator_lock(&chunk_handle_allocator_mutex_);
  auto ret(global_chunk_id_.fetch_add(1));
  PersistChunkHandleAllocator();
  return std::to_string(ret);
}

MetadataManager* MetadataManager::GetInstance() {
  static MetadataManager* metadataManager = new MetadataManager();
  return metadataManager;
}

} /* namespace server */
} /* namespace gfs */
