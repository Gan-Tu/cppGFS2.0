#include "src/server/chunk_server/file_chunk_manager.h"

#include <algorithm>
#include <memory>
#include <string>

#include "absl/strings/str_cat.h"
#include "leveldb/write_batch.h"
#include "src/common/system_logger.h"
#include "src/common/utils.h"

namespace gfs {
namespace server {

using absl::AlreadyExistsError;
using absl::DataLossError;
using absl::InternalError;
using absl::NotFoundError;
using absl::OkStatus;
using absl::OutOfRangeError;
using absl::UnimplementedError;
using absl::UnknownError;

FileChunkManager::FileChunkManager()
    : chunk_database_(nullptr), max_chunk_size_bytes_(0) {}

FileChunkManager* FileChunkManager::GetInstance() {
  static FileChunkManager instance;

  return &instance;
}

void FileChunkManager::Initialize(const std::string& chunk_database_name,
                                  const uint32_t& max_chunk_size_bytes) {
  // Currently using the default configs for leveldb. We can tune configuration
  // if we see that it isn't efficient enough to meet our needs. By default 4kb
  // (uncompressed bytes) blocks and compression is enabled. Check_sum
  // verification isn't enabled by default. Can add more configs here later.

  leveldb::DB* database;

  leveldb::Options options;
  options.create_if_missing = true;

  leveldb::Status status =
      leveldb::DB::Open(options, chunk_database_name, &database);

  LOG_ASSERT(status.ok()) << status.ToString();

  // Database is closed once pointer is deleted.
  this->chunk_database_ = std::unique_ptr<leveldb::DB>(database);
  this->max_chunk_size_bytes_ = max_chunk_size_bytes;
}

uint32_t FileChunkManager::GetMaxChunkSizeBytes() const {
  return this->max_chunk_size_bytes_;
}

absl::Status FileChunkManager::CreateChunk(
    const std::string& chunk_handle, const uint32_t& create_version) {
  // Allocates new iterator from heap. Using iterator instead of getting to
  // avoid copying because we don't need the data, just needs to see if handle
  // already exist.
  std::unique_ptr<leveldb::Iterator> db_read_iterator(
      chunk_database_->NewIterator(leveldb::ReadOptions()));

  db_read_iterator->Seek(chunk_handle);

  // If iterator is valid (points to a chunk), and no error was encountered and
  // the pointed chunk has the same handle. Checking handle even after validity,
  // because it can point to another chunk after seek, if chunk_handle isn't
  // found.
  if (db_read_iterator->Valid() && db_read_iterator->status().ok() &&
      db_read_iterator->key() == chunk_handle) {
    // already exist
    return AlreadyExistsError(absl::StrCat(
        "Chunk already exist with the specified handle. Found handle=",
        db_read_iterator->key().ToString()));
  }

  // Assuming no concurrent create operation for the same handle. Since it's
  // prevented by master. So no time-of-check to time-of-use condition here.

  // Doesn't exist, let us create it with no data.
  protos::FileChunk new_chunk;
  new_chunk.set_version(create_version);

  leveldb::Status status = WriteFileChunk(chunk_handle, &new_chunk);

  if (!status.ok()) {
    // write failed
    return UnknownError(
        absl::StrCat("Failed creating new chunk. Status: ", status.ToString()));
  }

  return OkStatus();
}

absl::StatusOr<std::string> FileChunkManager::ReadFromChunk(
    const std::string& chunk_handle, const uint32_t& read_version,
    const uint32_t& start_offset, const uint32_t& length) {
  auto result = GetFileChunk(chunk_handle, read_version);

  if (!result.ok()) {
    // Read failed
    return result.status();
  }

  auto file_chunk = result.value();

  // Check that we aren't trying to read data that isn't there
  if (start_offset > file_chunk->data().length()) {
    return OutOfRangeError(absl::StrCat(
        "Read start offset is after end of chunk. End of chunk (bytes): ",
        file_chunk->data().length()));
  }

  // Verify the checksums of all blocks that overlap the read range before
  // returning any data, so this chunk server never propagates silent disk
  // corruption to the requester (GFS paper section 5.2)
  auto verify_status =
      VerifyBlockChecksums(file_chunk.get(), start_offset, length);
  if (!verify_status.ok()) {
    LOG(ERROR) << "Data corruption detected while reading chunk "
               << chunk_handle << ": " << verify_status.ToString();
    return verify_status;
  }

  // This may return less than length, if chunk doesn't have up to that.
  return file_chunk->data().substr(start_offset, length);
}

absl::StatusOr<uint32_t> FileChunkManager::WriteToChunk(
    const std::string& chunk_handle, const uint32_t& write_version,
    const uint32_t& start_offset, const uint32_t& length,
    const std::string& new_data) {
  auto result = GetFileChunk(chunk_handle, write_version);

  if (!result.ok()) {
    // Read failed
    return result.status();
  }

  auto file_chunk = result.value();

  // Update the fetched data

  // Write must start from an existing offset or current end of chunk.
  if (start_offset > file_chunk->data().length()) {
    return OutOfRangeError(absl::StrCat(
        "Write start offset is after end of chunk. End of chunk (bytes): ",
        file_chunk->data().length()));
  }

  // Can write past the current length but not more than max chunk size.
  uint32_t remaining_bytes = this->max_chunk_size_bytes_ - start_offset;

  if (remaining_bytes == 0) {
    // Chunk is full, can't write
    return OutOfRangeError(
        absl::StrCat("Chunk is full. Max chunk size (bytes): ",
                     this->max_chunk_size_bytes_));
  }

  // Don't want to write more than the bytes left for the chunk to be full.
  uint32_t actual_write_length = std::min(length, remaining_bytes);

  // Per section 5.2 of the GFS paper, before overwriting an existing range we
  // must verify the first and last blocks of the range being overwritten:
  // if a partially-overwritten corrupted block were not detected now, the new
  // checksum would hide the corruption in the region not being overwritten.
  auto verify_status = VerifyBlockChecksums(
      file_chunk.get(), start_offset,
      std::min<uint64_t>(actual_write_length,
                         file_chunk->data().length() - start_offset));
  if (!verify_status.ok()) {
    LOG(ERROR) << "Data corruption detected while writing chunk "
               << chunk_handle << ": " << verify_status.ToString();
    return verify_status;
  }

  // Writes to the same chunk are serialized by the file service layer using
  // per-chunk mutation locks (mirroring the mutation order the primary
  // replica imposes), so the read-modify-write below is race free.

  // write to the existing data in memory
  file_chunk->mutable_data()->replace(
      start_offset, actual_write_length, new_data.data(),
      /*start_position_in_new_data=*/0, actual_write_length);

  // Incrementally update the checksums of the blocks the write touched
  UpdateBlockChecksums(file_chunk.get(), start_offset, actual_write_length);

  leveldb::Status status = WriteFileChunk(chunk_handle, file_chunk.get());

  if (!status.ok()) {
    // write failed
    return UnknownError(
        absl::StrCat("Failed while writing data. Status: ", status.ToString()));
  }

  return actual_write_length;
}

absl::Status FileChunkManager::VerifyBlockChecksums(
    const protos::FileChunk* file_chunk, const uint32_t& offset,
    const uint32_t& length) {
  // Chunks written before checksums were introduced carry none; skip
  // verification for them (they become covered on their next write)
  if (file_chunk->block_checksums_size() == 0 || length == 0 ||
      offset >= file_chunk->data().length()) {
    return OkStatus();
  }

  const std::string& data = file_chunk->data();
  // Clamp the verified range to the stored data
  const uint64_t range_end =
      std::min<uint64_t>((uint64_t)offset + length, data.length());
  const uint32_t first_block = offset / kChecksumBlockSize;
  const uint32_t last_block = (range_end - 1) / kChecksumBlockSize;

  for (uint32_t block = first_block; block <= last_block; ++block) {
    const uint64_t block_start = (uint64_t)block * kChecksumBlockSize;
    const uint64_t block_length =
        std::min<uint64_t>(kChecksumBlockSize, data.length() - block_start);
    if ((int)block >= file_chunk->block_checksums_size()) {
      // Legacy chunk data beyond the stored checksums; nothing to verify
      break;
    }
    const uint32_t expected_checksum = file_chunk->block_checksums(block);
    const uint32_t actual_checksum =
        gfs::common::utils::calc_crc32c(data.data() + block_start,
                                        block_length);
    if (expected_checksum != actual_checksum) {
      return DataLossError(absl::StrCat(
          "Checksum mismatch in block ", block, ": expected ",
          expected_checksum, " but computed ", actual_checksum));
    }
  }

  return OkStatus();
}

void FileChunkManager::UpdateBlockChecksums(protos::FileChunk* file_chunk,
                                            const uint32_t& offset,
                                            const uint32_t& length) {
  // A chunk that predates checksums has data but no stored checksums; in
  // that case recompute every block so the whole chunk becomes covered,
  // instead of leaving untouched blocks with placeholder values
  const bool full_recompute = file_chunk->block_checksums_size() == 0;

  const std::string& data = file_chunk->data();
  const uint32_t block_count =
      (data.length() + kChecksumBlockSize - 1) / kChecksumBlockSize;

  // Resize the checksum list to match the (possibly grown) data size
  while ((uint32_t)file_chunk->block_checksums_size() < block_count) {
    file_chunk->add_block_checksums(0);
  }
  while ((uint32_t)file_chunk->block_checksums_size() > block_count) {
    file_chunk->mutable_block_checksums()->RemoveLast();
  }

  if (block_count == 0) {
    return;
  }

  uint32_t first_block = offset / kChecksumBlockSize;
  uint32_t last_block = 0;
  if (length > 0) {
    last_block = std::min<uint32_t>(
        ((uint64_t)offset + length - 1) / kChecksumBlockSize, block_count - 1);
  }

  if (full_recompute) {
    first_block = 0;
    last_block = block_count - 1;
  }

  for (uint32_t block = first_block; block <= last_block; ++block) {
    const uint64_t block_start = (uint64_t)block * kChecksumBlockSize;
    const uint64_t block_length =
        std::min<uint64_t>(kChecksumBlockSize, data.length() - block_start);
    file_chunk->set_block_checksums(
        block, gfs::common::utils::calc_crc32c(data.data() + block_start,
                                               block_length));
  }
}

absl::Status FileChunkManager::StoreChunkData(const std::string& chunk_handle,
                                              const uint32_t& version,
                                              const std::string& data) {
  protos::FileChunk file_chunk;
  file_chunk.set_version(version);
  *file_chunk.mutable_data() = data;
  UpdateBlockChecksums(&file_chunk, 0, data.length());

  leveldb::Status status = WriteFileChunk(chunk_handle, &file_chunk);
  if (!status.ok()) {
    return UnknownError(absl::StrCat(
        "Failed while storing cloned chunk data. Status: ", status.ToString()));
  }

  return OkStatus();
}

absl::Status FileChunkManager::UpdateChunkVersion(
    const std::string& chunk_handle, const uint32_t& from_version,
    const uint32_t& to_version) {
  auto result = GetFileChunk(chunk_handle, from_version);

  if (!result.ok()) {
    // Read failed
    return result.status();
  }

  auto file_chunk = result.value();

  // Update the version in memory, since we found the chunk with the
  // from_version
  file_chunk->set_version(to_version);

  leveldb::Status status = WriteFileChunk(chunk_handle, file_chunk.get());

  if (!status.ok()) {
    // version update failed
    return UnknownError(absl::StrCat("Failed while updating version. Status: ",
                                     status.ToString()));
  }

  return OkStatus();
}

absl::StatusOr<uint32_t> FileChunkManager::GetChunkVersion(
    const std::string& chunk_handle) {
  auto result = GetFileChunk(chunk_handle);

  if (!result.ok()) {
    // Read failed
    return result.status();
  }

  return result.value()->version();
}

absl::StatusOr<uint32_t> FileChunkManager::AppendToChunk(
    const std::string& chunk_handle, const uint32_t& append_version,
    const uint32_t& length, const std::string& new_data) {
  return UnimplementedError("Append not implemented yet.");
}

absl::StatusOr<std::shared_ptr<protos::FileChunk>>
FileChunkManager::GetFileChunk(const std::string& chunk_handle) {
  std::string existing_data;
  leveldb::Status status = this->chunk_database_->Get(
      leveldb::ReadOptions(), chunk_handle, &existing_data);

  if (!status.ok()) {
    // chunk handle not found
    return NotFoundError(
        absl::StrCat("Chunk not found. Handle=", chunk_handle,
                     " Status: ", status.ToString()));
  }

  std::shared_ptr<protos::FileChunk> file_chunk(new protos::FileChunk());

  if (!file_chunk->ParseFromString(existing_data)) {
    return InternalError("Failed to parse data from disk.");
  }

  return file_chunk;
}

absl::StatusOr<std::shared_ptr<protos::FileChunk>>
FileChunkManager::GetFileChunk(const std::string& chunk_handle,
                               const uint32_t& version) {
  auto result = GetFileChunk(chunk_handle);

  if (!result.ok()) {
    // Failed
    return result.status();
  }

  auto& file_chunk = result.value();

  // check version
  if (file_chunk->version() != version) {
    // wrong version
    return NotFoundError(
        absl::StrCat("Specified version for ", chunk_handle,
                     " doesn't match current version. Specified=",
                     std::to_string(version),
                     " Current=" + std::to_string(file_chunk->version())));
  }

  return file_chunk;
}

leveldb::Status FileChunkManager::WriteFileChunk(
    const std::string& chunk_handle, const protos::FileChunk* file_chunk) {
  leveldb::WriteOptions write_options;

  // Synchronous at the moment, and returns when update is applied to disk. This
  // isn't the best, but we can improve this and do asynchronous writes, by
  // using a Write Ahead Log (WAL) to maintain durability.
  write_options.sync = true;

  return this->chunk_database_->Put(write_options, chunk_handle,
                                    file_chunk->SerializeAsString());
}

absl::Status FileChunkManager::DeleteChunk(
    const std::string& chunk_handle) {
  // Doing delete asynchronously
  leveldb::Status status =
      this->chunk_database_->Delete(leveldb::WriteOptions(), chunk_handle);

  if (!status.ok()) {
    return UnknownError(
        absl::StrCat("Deletion failed. Status: ", status.ToString()));
  }

  return OkStatus();
}

std::list<protos::FileChunkMetadata>
FileChunkManager::GetAllFileChunkMetadata() {
  std::unique_ptr<leveldb::Iterator> iterator(
      this->chunk_database_->NewIterator(leveldb::ReadOptions()));

  std::list<protos::FileChunkMetadata> metadatas;

  // Iterate the entire database for chunks
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    protos::FileChunk file_chunk;

    if (file_chunk.ParseFromString(iterator->value().ToString())) {
      protos::FileChunkMetadata metadata;
      metadata.set_chunk_handle(iterator->key().ToString());
      metadata.set_version(file_chunk.version());

      metadatas.push_back(metadata);
    }
  }

  return metadatas;
}

}  // namespace server
}  // namespace gfs
