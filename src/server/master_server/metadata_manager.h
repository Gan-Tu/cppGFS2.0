#ifndef SERVER_MASTER_SERVER_METADATA_MANAGER_H
#define SERVER_MASTER_SERVER_METADATA_MANAGER_H

#include "absl/container/flat_hash_set.h"
#include "src/server/master_server/lock_manager.h"
#include "src/protos/metadata.pb.h"

using protos::FileMetadata;

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
 *    function is called.  */

class MetadataManager {
  public:
   MetadataManager();
      
   /* Create the file metadata (and a lock associated with this file) for a 
    * given file path. This function returns false if the file path already 
    * exists or if any of the intermediate parent directory does not exist. */
   bool CreateFileMetadata(const std::string& pathname);

   /* Check if metadata file a file exists */
   bool ExistFileMetadata(const std::string& pathname) const;
      
   /* Access the file metadata for a given file path. The caller of this 
    * function needs to ensure the lock for this file is properly used */
   std::shared_ptr<FileMetadata> GetFileMetadata(
     const std::string& pathname) const;

   /* Create a file chunk for a given pathname and a chunk index.  */
   std::string CreateChunkHandle(const std::string& pathname, 
                                 uint32_t chunk_index);

   /* Delete a file, and delete all chunk handles associated with this file */
   void DeleteFile(const std::string& pathname);

   /* Assign a new chunk handle. This function returns a unique chunk handle
    * everytime when it gets called */
   std::string AllocateNewChunkHandle(); 

   /* Instance function to access the singleton */
   static MetadataManager* GetInstance();

  private:
   /* An atomic uint64 used to assign UUID for each chunk */
   std::atomic<uint64_t> globalChunkId_{0};
   /* Store all deleted chunk handles in a hashset  */
   absl::flat_hash_set<std::string> deletedChunkHandles_;
   /* Map from file path to FileMetadata */
   absl::flat_hash_map<std::string, std::shared_ptr<FileMetadata>> fileMetadata_;
   /* Lock manager to manager the synchronization of operations */
   LockManager* lockManager_;
};

} /* namespace server  */
} /* namespace gfs */

#endif /* SERVER_MASTER_SERVER_METADATA_MANAGER */
