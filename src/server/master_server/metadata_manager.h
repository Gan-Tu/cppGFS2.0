#ifndef GFS_SERVER_MASTER_SERVER_METADATA_MANAGER_H_
#define GFS_SERVER_MASTER_SERVER_METADATA_MANAGER_H_

#include "absl/container/flat_hash_set.h"
#include "google/protobuf/stubs/statusor.h"
#include "src/protos/metadata.pb.h"
#include "src/server/master_server/lock_manager.h"

namespace gfs {
namespace server {

/* The MetadataManager manages the following resources that are central to the
 * master node in GFS.
 * 1) the assignment of chunk handle, which is a UUID.
 * 2) the mapping between a file name to the chunk handles associated with it.
 * 3) the set of deleted chunk handles.
 *
 * The MetadataManager provides the following thread-safe methods:
 * 1) create a (default) file metadata for a given file. This involves also the
 *    creation of underlying locks associated with this file.
 * 2) get the file metadata for a given file
 * 3) create a file chunk handle
 * 4) delete a file, this involves the deletion of all chunk handles assigned to
 *    this file.
 * 5) assigning new chunk handle, this provides a unique UUID each time when the
 *    function is called.
 *
 * TODO(Xi): TBD whether to put chunk location and version information in this
 * MetadataManager
 */

class MetadataManager {
 public:
  MetadataManager();

  /* Create the file metadata (and a lock associated with this file) for a
   * given file path. This function returns error if the file path already
   * exists or if any of the intermediate parent directory does not exist. */
  google::protobuf::util::Status CreateFileMetadata(
      const std::string& filename);

  /* Check if metadata file a file exists */
  bool ExistFileMetadata(const std::string& filename) const;

  /* Access the file metadata for a given file path. The caller of this
   * function needs to ensure the lock for this file is properly used.
   * return error if fileMetadata does not exist */
  google::protobuf::util::StatusOr<std::shared_ptr<protos::FileMetadata>>
  GetFileMetadata(const std::string& filename) const;

  /* Create a file chunk for a given filename and a chunk index.  */
  google::protobuf::util::StatusOr<std::string> CreateChunkHandle(
      const std::string& filename, uint32_t chunk_index);

  /* Delete a file, and delete all chunk handles associated with this file */
  void DeleteFile(const std::string& filename);

  /* Assign a new chunk handle. This function returns a unique chunk handle
   * everytime when it gets called */
  std::string AllocateNewChunkHandle();

  /* Instance function to access the singleton */
  static MetadataManager* GetInstance();

 private:
  /* An atomic uint64 used to assign UUID for each chunk */
  std::atomic<uint64_t> global_chunk_id_{0};
  /* Store all deleted chunk handles in a hashset  */
  absl::flat_hash_set<std::string> deleted_chunk_handles_;
  /* Map from file path to FileMetadata */
  absl::flat_hash_map<std::string, std::shared_ptr<protos::FileMetadata>>
      file_metadata_;
  /* Lock manager to manager the synchronization of operations */
  LockManager* lock_manager_;
  /* A lock to lock file_metadata_ */
  absl::Mutex* file_metadata_lock_;
};

}  // namespace server
}  // namespace gfs

#endif // GFS_SERVER_MASTER_SERVER_METADATA_MANAGER_H_
