#ifndef GFS_SERVER_MASTER_SERVER_METADATA_MANAGER_H_
#define GFS_SERVER_MASTER_SERVER_METADATA_MANAGER_H_

#include <memory>

#include "absl/status/statusor.h"
#include "leveldb/db.h"
#include "src/common/utils.h"
#include "src/protos/metadata.pb.h"
#include "src/server/master_server/lock_manager.h"

namespace gfs {
namespace server {

// The MetadataManager manages the following resources that are central to the
// master node in GFS.
// 1) the assignment of chunk handle, which is a UUID.
// 2) the mapping between a file name to the chunk handles associated with it.
// 3) the version and lease state of each chunk.
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
// Following section 2.6.3 of the GFS paper, the namespace (file -> chunk
// handles) and the chunk versions are the master's only *persistent* state:
// when persistence is enabled (EnablePersistence), every namespace or version
// mutation is synchronously written through to an on-disk LevelDB store
// before the mutation is acknowledged — playing the role of the paper's
// operation log + checkpoint — and the state is recovered from disk on
// startup. Chunk *locations* are deliberately not persisted: the master
// re-learns them from chunk server reports (GFS paper section 2.6.2), and
// lease state is soft state that is simply re-established after a restart.
class MetadataManager {
 public:
  // Create the file metadata (and a lock associated with this file) for a
  // given file path. This function returns error if the file path already
  // exists or if any of the intermediate parent directory not found.
  absl::Status CreateFileMetadata(const std::string& filename);

  // Check if metadata file a file exists
  bool ExistFileMetadata(const std::string& filename);

  // Delete a file metadata, and delete all chunk handles associated with
  // this file
  void DeleteFileAndChunkMetadata(const std::string& filename);

  // Access the file metadata for a given file path. The caller of this
  // function needs to ensure the lock for this file is properly used.
  // return error if fileMetadata not found
  absl::StatusOr<std::shared_ptr<protos::FileMetadata>> GetFileMetadata(
      const std::string& filename);

  // Create a file chunk for a given filename and a chunk index.
  absl::StatusOr<std::string> CreateChunkHandle(const std::string& filename,
                                                uint32_t chunk_index);

  // Remove the chunk handle mapped at (filename, chunk_index) and its chunk
  // metadata. Used to roll back a failed chunk creation, when the chunk could
  // not be initialized on any chunk server.
  void DeleteChunkHandle(const std::string& filename, uint32_t chunk_index);

  // Retrieve a chunk handle for a given filename and chunk index. Return
  // error if filename or chunk not found
  absl::StatusOr<std::string> GetChunkHandle(const std::string& filename,
                                             uint32_t chunk_index);

  // Advance the chunk version number for a chunk handle, return error if
  // chunk handle not found
  absl::Status AdvanceChunkVersion(const std::string& chunk_handle);

  // Set the chunk version for a chunk handle to the given value. Used when
  // the master learns from a chunk server report that replicas hold a newer
  // version than recorded, which means the master failed after instructing
  // replicas to advance but before recording the new version; per section
  // 4.5 of the GFS paper the master takes the higher version to be
  // up-to-date.
  absl::Status SetChunkVersion(const std::string& chunk_handle,
                               uint32_t version);

  // Check whether file chunk metadata exists
  bool ExistFileChunkMetadata(const std::string& chunk_handle);

  // Get the chunk metadata for a given chunk handle, return error if
  // chunk handle not found
  absl::StatusOr<protos::FileChunkMetadata> GetFileChunkMetadata(
      const std::string& chunk_handle);

  // Set the chunk metadata for a given chunk handle
  void SetFileChunkMetadata(const protos::FileChunkMetadata& chunk_data);

  // Delete the file chunk metadata for a given chunk handle
  void DeleteFileChunkMetadata(const std::string& chunk_handle);

  // Set the primary chunk location that holds the lease for a given chunk
  // handle, and its lease expiration time
  void SetPrimaryLeaseMetadata(const std::string& chunk_handle,
                               const protos::ChunkServerLocation& location,
                               const uint64_t expiration_unix_sec);

  // Record which replicas confirmed the version advance when the current
  // lease was granted. While the lease is reused, only these replicas (the
  // up-to-date ones) may be returned to writing clients.
  void SetPrimaryLeaseReplicas(
      const std::string& chunk_handle,
      const std::vector<protos::ChunkServerLocation>& replicas);

  // Return the replicas recorded by SetPrimaryLeaseReplicas for the current
  // lease (empty if none recorded).
  std::vector<protos::ChunkServerLocation> GetPrimaryLeaseReplicas(
      const std::string& chunk_handle);

  // Unset the primary chunk location that holds the lease for a given chunk
  // handle; this happens when a lease expires / gets revoked.
  void RemovePrimaryLeaseMetadata(const std::string& chunk_handle);

  // Return the chunk server location that last held the lease for the handle,
  // which may or may not be expired; it's up to caller to check the expiration
  std::pair<std::pair<protos::ChunkServerLocation, uint64_t>, bool>
  GetPrimaryLeaseMetadata(const std::string& chunk_handle);

  // Return a per-chunk lock used by the master to serialize the version
  // advancement / lease grant sequence for a chunk. Without this, two
  // concurrent write-opens for the same chunk could both advance the chunk
  // version and hand out conflicting leases.
  absl::Mutex* GetChunkLeaseLock(const std::string& chunk_handle);

  // Assign a new chunk handle. This function returns a unique chunk handle
  // everytime when it gets called
  std::string AllocateNewChunkHandle();

  // Enable write-through persistence of the namespace, chunk versions, and
  // the chunk handle allocator to an on-disk LevelDB store at |db_path|
  // (creating it if needed), and recover any previously persisted state into
  // memory. Should be called once at master startup, before serving.
  absl::Status EnablePersistence(const std::string& db_path);

  // Whether persistence is enabled. Among other things this decides whether
  // a chunk unknown to the master can be assumed deleted (with persistence,
  // the master's recovered state is authoritative) or must be trusted when a
  // chunk server first registers (without persistence, an unknown chunk
  // after a master restart is expected, not garbage).
  bool HasPersistence() const { return metadata_store_ != nullptr; }

  // Instance function to access the singleton
  static MetadataManager* GetInstance();

 private:
  MetadataManager();

  // Persist helpers; no-ops when persistence is not enabled. Failures are
  // returned so mutations can be refused (the paper's master does not apply a
  // mutation whose log record cannot be flushed).
  absl::Status PersistFileMetadata(const std::string& filename);
  absl::Status PersistChunkMetadata(const std::string& chunk_handle);
  void ErasePersistedFileMetadata(const std::string& filename);
  void ErasePersistedChunkMetadata(const std::string& chunk_handle);
  void PersistChunkHandleAllocator();

  // An atomic uint64 used to assign UUID for each chunk
  std::atomic<uint64_t> global_chunk_id_{0};

  // Parallel hash map for file metadata
  gfs::common::parallel_hash_map<std::string,
                                 std::shared_ptr<protos::FileMetadata>>
      file_metadata_;

  // Map from chunk handle to FileChunkMetadata, which includes all
  // the chunk server (replica) locations. Similar to file_metadata_
  // this is a parallel hash map
  gfs::common::parallel_hash_map<std::string, protos::FileChunkMetadata>
      chunk_metadata_;

  // chunk handle to its lease holder and lease expiration time
  gfs::common::parallel_hash_map<
      std::string, std::pair<protos::ChunkServerLocation, uint64_t>>
      lease_holders_;

  // chunk handle to the replicas that confirmed the version advance when the
  // current lease was granted; see SetPrimaryLeaseReplicas
  gfs::common::parallel_hash_map<std::string,
                                 std::vector<protos::ChunkServerLocation>>
      lease_replicas_;

  // Serializes chunk handle allocation with its persistence, so the
  // persisted allocator watermark can never go backwards relative to a
  // handle that has already been handed out
  absl::Mutex chunk_handle_allocator_mutex_;

  // Per-chunk locks used to serialize version advancement and lease grants;
  // see GetChunkLeaseLock
  gfs::common::parallel_hash_map<std::string, std::shared_ptr<absl::Mutex>>
      chunk_lease_locks_;

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

  // On-disk store for the persistent metadata; null when persistence is not
  // enabled (e.g. in unit tests)
  std::unique_ptr<leveldb::DB> metadata_store_;
};

}  // namespace server
}  // namespace gfs

#endif  // GFS_SERVER_MASTER_SERVER_METADATA_MANAGER_H_
