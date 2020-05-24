#ifndef GFS_SERVER_MASTER_SERVER_METADATA_MANAGER_H_
#define GFS_SERVER_MASTER_SERVER_METADATA_MANAGER_H_

#include "absl/container/flat_hash_set.h"
#include "google/protobuf/stubs/statusor.h"
#include "src/common/utils.h"
#include "src/protos/metadata.pb.h"
#include "src/server/master_server/lock_manager.h"

namespace gfs {
namespace server {

// The MetadataManager manages the following resources that are central to the
// master node in GFS.
// 1) the assignment of chunk handle, which is a UUID.
// 2) the mapping between a file name to the chunk handles associated with it.
// 3) the set of deleted chunk handles.
//
// The MetadataManager provides the following thread-safe methods:
// 1) create a (default) file metadata for a given file. This involves also the
//    creation of underlying locks associated with this file.
// 2) get the file metadata for a given file
// 3) create a file chunk handle
// 4) delete a file, this involves the deletion of all chunk handles assigned to
//    this file.
// 5) assigning new chunk handle, this provides a unique UUID each time when the
//    function is called.
//
// TODO(Xi): TBD whether to put chunk location and version information in this
// MetadataManager
class MetadataManager {
 public:
  // Create the file metadata (and a lock associated with this file) for a
  // given file path. This function returns error if the file path already
  // exists or if any of the intermediate parent directory not found.
  google::protobuf::util::Status CreateFileMetadata(
      const std::string& filename);

  // Check if metadata file a file exists
  bool ExistFileMetadata(const std::string& filename);

  // Delete a file metadata, and delete all chunk handles associated with 
  // this file
  void DeleteFileMetadata(const std::string& filename);

  // Access the file metadata for a given file path. The caller of this
  // function needs to ensure the lock for this file is properly used.
  // return error if fileMetadata not found
  google::protobuf::util::StatusOr<std::shared_ptr<protos::FileMetadata>>
      GetFileMetadata(const std::string& filename);

  // Create a file chunk for a given filename and a chunk index.
  google::protobuf::util::StatusOr<std::string> CreateChunkHandle(
      const std::string& filename, uint32_t chunk_index);

  // Retrieve a chunk handle for a given filename and chunk index. Return 
  // error if filename or chunk not found
  google::protobuf::util::StatusOr<std::string> GetChunkHandle(
      const std::string& filename, uint32_t chunk_index);

  // Advance the chunk version number for a chunk handle, return error if
  // chunk handle not found
  google::protobuf::util::Status AdvanceChunkVersion(
      const std::string& chunk_handle);

  // Get the chunk metadata for a given chunk handle, return error if 
  // chunk handle not found 
  google::protobuf::util::StatusOr<protos::FileChunkMetadata> 
      GetFileChunkMetadata(const std::string& chunk_handle);

  // Set the chunk metadata for a given chunk handle
  void SetFileChunkMetadata(const protos::FileChunkMetadata& chunk_data);

  // Set the primary chunk location for a given chunk handle, return error
  // if chunk handle not found.  
  google::protobuf::util::Status SetPrimaryChunkServerLocation(
      const std::string& chunk_handle, 
      const protos::ChunkServerLocation& server_location);

  // Unset the primary chunk location for a given chunk handle, 
  // this happens when a lease expires / gets revoked. 
  google::protobuf::util::Status RemovePrimaryChunkServerLocation(
      const std::string& chunk_handle);

  // Assign a new chunk handle. This function returns a unique chunk handle
  // everytime when it gets called
  std::string AllocateNewChunkHandle();

  // Instance function to access the singleton
  static MetadataManager* GetInstance();

 private:
  MetadataManager();

  // An atomic uint64 used to assign UUID for each chunk
  std::atomic<uint64_t> global_chunk_id_{0};
  
  // Store all deleted chunk handles in a thread-safe hashset
  absl::flat_hash_set<std::string> deleted_chunk_handles_;
  // TODO: add a lock for deleted_chunk_handles_ once starting implementing
  // the deletion logic
 
  // Parallel hash map for file metadata
  gfs::common::parallel_hash_map<std::string, 
      std::shared_ptr<protos::FileMetadata>> file_metadata_;
 
  // Map from chunk handle to FileChunkMetadata, which includes all 
  // the chunk server (replica) locations. Similar to file_metadata_
  // this is a parallel hash map
  gfs::common::parallel_hash_map<std::string, 
      protos::FileChunkMetadata> chunk_metadata_; 

  // Note that the file_metadata_ maps to the reference of the actual
  // FileMetadata, but file_chunk_metadata_ maps to actual copy of
  // FileChunkMetadata. This is a design decision. Because FileMetadata can
  // be much bigger as a file can be made of hundres / thousands of chunks,
  // mapping to the reference offers us flexibility when updating
  // filemetadata as copying would be expensive (one may think about
  // accessing the value by reference and do the update upon the reference,
  // but it is unclear if any concurrent operation can invalidate such
  // a reference). On the other hande, FileChunkMetadata is relatively
  // small as we do not expect a chunk to have say more than 10
  // replications. Therefore, we simply map to the actual copy of such
  // a data. Last but not least, we do not expect the chunk metadata
  // gets updated frequently, as failure of chunk replica occurs rarely,
  // so some copy operation here is presumbaly tolerable. 

  // Lock manager to manager the synchronization of operations
  LockManager* lock_manager_;
};

}  // namespace server
}  // namespace gfs

#endif // GFS_SERVER_MASTER_SERVER_METADATA_MANAGER_H_
