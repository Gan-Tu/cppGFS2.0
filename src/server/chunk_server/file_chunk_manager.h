#ifndef GFS_SERVER_CHUNK_SERVER_FILE_CHUNK_MANAGER_H_
#define GFS_SERVER_CHUNK_SERVER_FILE_CHUNK_MANAGER_H_

#include <list>
#include <memory>
#include <string>

#include "google/protobuf/stubs/statusor.h"
#include "leveldb/db.h"
#include "src/protos/chunk_server.pb.h"
#include "src/protos/metadata.pb.h"

namespace gfs {

namespace server {

// Singleton class responsible for storing and retrieving file chunks.
// The max size of the chunk is configurable during initialization, and
// chunks are identified using their chunk_handle as the key. This also
// supports external data versioning where clients set the version of data.
// File chunks are serialized to binary and stored on disk. We use std::string
// for passing the data but the data doesn't have to be string type. We just
// use std::string to represent the data bytes and treat it as bytes, regardless
// of the actual data type.
//
// Uses an internal database (leveldb) as the underlying storage system.
// Since the underlying database is abstracted, we can change it while still
// maintaining the interfaces we provide to the class clients.
// Using a database like leveldb, we are able to handoff concurrency safety to
// it, without having to handle it externally.
// Leveldb is very configurable and currently using some of the default
// configurations which includes 4Kb block size, compression etc. We can tune
// this for improve our performance if we see that it doesn't work well for us.
//
// We don't maintain any cache and rely on the system buffer cache, since we
// write to disk.
//
// Note - All operations are synchronous at the moment, and returns when update
// is applied to disk. This isn't the best, but we can improve this and do
// asynchronous writes, by using a Write Ahead Log (WAL) to maintain durability.
//
// Usage (please see the method declaration for more info):
// The file chunk manager needs to be initialize only once and before any chunk
// operation is called.
//
// Initialization:
// Note - The specified database_name can only be accessed by one file
// chunk manager (process) at a time, and locks it to itself.
//
//    auto file_chunk_mgr = FileChunkManager::GetInstance();
//    file_chunk_mgr->Initialize(
//        /*chunk_database_name=*/"/tmp/file_chunk_mgr_test_db",
//        /*max_chunk_size_bytes=*/100);
//
// New empty chunk creation: auto result =
// file_chunk_mgr->CreateChunk("chunk_handle",
// /*create_version=*/1);
//
// Update the chunk version: auto result =
// file_chunk_mgr->UpdateChunkVersion("chunk_handle",
// /*from_version=*/1, /*to_version=*/2);
//
// Write data for a specific version:
// auto result = file_chunk_mgr->WriteToChunk("chunk_handle",
// /*write_version=*/2,
// /*start_offset=*/0, /*length=*/10, /*data=*/"data data data ");
//
// Read data for a specific version: auto result =
// file_chunk_mgr->ReadFromChunk(
//      "chunk_handle", /*read_version=*/2, /*start_offset=*/2, /*length=*/4);
//
// Delete a chunk: auto result = file_chunk_mgr->DeleteChunk("chunk_handle");
//
// Append to chunk: Not yet supported
//
// Get the metadata (handle, version) of all stored chunk. Useful when reporting
// stored chunks to master: auto all_chunks =
// file_chunk_mgr->GetAllFileChunkMetadata();
//
class FileChunkManager {
 public:
  // The singleton instance of the file chunk manager.
  static FileChunkManager* GetInstance();

  // To prevent copying the file chunk manager. Pointer copy is allowed.
  FileChunkManager(const FileChunkManager&) = delete;

  void operator=(const FileChunkManager&) = delete;

  // The file chunk manager must be initialized only once for a process, before
  // any chunk operation is perfomed. This does the setup for the internal
  // database needed by the manager. The database name (path plus name e.g.
  // /tmp/mydb) passed can be an existing chunk database, or name for a new one
  // to be created.
  // The specified database_name can only be accessed by one file
  // chunk manager (process) at a time, and locks it to itself. You can't use
  // the same database name for different processes (each with a file chunk
  // manager) on the same machine. You can also set the max size for each chunk.
  // Once this size is met, the chunk is considered to be full and can't grow
  // pass that. Writes that will make it larger will be truncated.
  void Initialize(const std::string& chunk_database_name,
                  const uint32_t& max_chunk_size_bytes);

  // Returns the configured max size of a chunk in bytes.
  uint32_t GetMaxChunkSizeBytes() const;

  // Returns the metadata (chunk_handle and version) for all chunks stored by
  // the file chunk manager. If a chunk data is corrupted, it won't return it.
  // This is useful for reporting the chunks stored on a chunk server. Returns
  // an empty list if none is stored.
  std::list<protos::FileChunkMetadata> GetAllFileChunkMetadata();

  // Create a new empty chunk with the specified create_version. Not intializing
  // the create_version because the file chunk mgr allows versioning to be
  // managed externally. Fails if a chunk with the specified handle already
  // exists.
  google::protobuf::util::Status CreateChunk(const std::string& chunk_handle,
                                             const uint32_t& create_version);

  // Reads data of the specified length from a chunk with the specified handle
  // with the specified version, from the start_offset. Returns the data read if
  // successful.
  //
  // Fails (Error: NOT_FOUND), if a chunk with the specified handle isn't found
  // or if the specified read_version doesn't match the version of the stored
  // chunk. Fails (Error: INTERNAL), if it is unable to deserialize the binary
  // data from disk. Fails (Error: OUT_OF_RANGE), if the start_offset is greater
  // than the offset of the end of the chunk.
  google::protobuf::util::StatusOr<std::string> ReadFromChunk(
      const std::string& chunk_handle, const uint32_t& read_version,
      const uint32_t& start_offset, const uint32_t& length);

  // Writes the specified data of the specified length to a chunk with the
  // specified handle with the specified version, from the start_offset. Returns
  // the actual length of data written if successful. The length of the data
  // written can be smaller than specified length, if the chunk max size was
  // reached.
  //
  // Fails (Error: NOT_FOUND), if a chunk with the specified handle isn't found
  // or if the specified write_version doesn't match the version of the stored
  // chunk.
  // Fails (Error: INTERNAL), if it is unable to deserialize the binary
  // data from disk for update.
  // Fails (Error: OUT_OF_RANGE), if the start_offset is greater than the offset
  // of the end of the chunk.
  // Fails (Error: UNKNOWN), if writing the update to disk failed.
  google::protobuf::util::StatusOr<uint32_t> WriteToChunk(
      const std::string& chunk_handle, const uint32_t& write_version,
      const uint32_t& start_offset, const uint32_t& length,
      const std::string& new_data);

  // Writes a new version to a chunk with the specified handle with the
  // specified from_version. Returns successful, if the chunk has been updated
  // to the to_version.
  //
  // Fails (Error: NOT_FOUND), if a chunk with the specified handle isn't found
  // or if the specified from_version doesn't match the version of the stored
  // chunk.
  // Fails (Error: INTERNAL), if it is unable to deserialize the binary
  // data from disk for update.
  // Fails (Error: UNKNOWN), if writing the update to disk failed.
  google::protobuf::util::Status UpdateChunkVersion(
      const std::string& chunk_handle, const uint32_t& from_version,
      const uint32_t& to_version);

  // Not yet implemented.
  // Append the specified data of the specified length to the end of the chunk
  // with the specified handle with the specified version. Returns the actual
  // length of data appended if successful. The length of the data appended can
  // be smaller than specified length, if the chunk max size was reached.
  google::protobuf::util::StatusOr<uint32_t> AppendToChunk(
      const std::string& chunk_handle, const uint32_t& append_version,
      const uint32_t& length, const std::string& new_data);

  // Delete the chunk with the specified handle from the chunk database. Returns
  // successfully, if chunk has been deleted.
  // Fails (Error: UNKNOWN), if deletion, failed. Could be the chunk with
  // specified handle doesn't exist.
  google::protobuf::util::Status DeleteChunk(const std::string& chunk_handle);

 private:
  // Helper used by couple of the public interfaces to get a file chunk from
  // database for a specific version. Returns the file chunk if successful.
  // Fails (Error: NOT_FOUND), if a chunk with the specified handle isn't found
  // or if the specified version doesn't match the version of the stored
  // chunk.
  // Fails (Error: INTERNAL), if it is unable to deserialize the binary
  // data from disk.
  google::protobuf::util::StatusOr<std::shared_ptr<protos::FileChunk>>
  GetFileChunk(const std::string& chunk_handle, const uint32_t& version);

  // The leveldb database used internally for chunk storage.
  // Since the underlying database is abstracted, we can change it while still
  // maintaining the interfaces we provide to the class clients.
  // Using a database like leveldb, we are able to handoff concurrency safety to
  // it, without having to handle it externally. Database is closed once pointer
  // is deleted.
  std::unique_ptr<leveldb::DB> chunk_database_;

  // Configured max size of a chunk in bytes.
  uint32_t max_chunk_size_bytes_;

  FileChunkManager();
};
}  // namespace server
}  // namespace gfs

#endif  // GFS_SERVER_CHUNK_SERVER_FILE_CHUNK_MANAGER_H_
