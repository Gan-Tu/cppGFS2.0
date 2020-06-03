#ifndef GFS_CLIENT_CLIENT_IMPL_H_
#define GFS_CLIENT_CLIENT_IMPL_H_

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "src/client/client_cache_manager.h"
#include "src/common/config_manager.h"
#include "src/common/protocol_client/chunk_server_service_gfs_client.h"
#include "src/common/protocol_client/master_metadata_service_client.h"
#include "src/common/utils.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"

namespace gfs {
namespace client {

// The ClientImpl class provides interfaces for the client library to access
// the configuration manager, cache manager and grpc clients
class ClientImpl {
 public:
  // Internal impl call that issues an OpenFileRequest to the master. 
  google::protobuf::util::Status CreateFile(const std::string& filename);
  
  // Internal impl call that reads data from a file, the first return field
  // is the bytes that actual got read and the second field is a pointer 
  // to the actual data buffer
  google::protobuf::util::StatusOr<std::pair<size_t, void*>> ReadFile(
      const char* filename, size_t offset, size_t nbytes);

  // Internal impl call that writes buffer to a file, and the return is 
  // the status which indicates if this write succeeds
  google::protobuf::util::Status WriteFile(const char* filename, void* buffer,
      size_t offset, size_t nbytes);

  // Construct and return a ClientImpl objects with proper configurations 
  // using the given config file. The ClientImpl object uses the config 
  // file to initialize the cache manager and two clients objects used to 
  // issue rpcs to the master and chunk server nodes, respectively.
  static google::protobuf::util::StatusOr<ClientImpl*> ConstructClientImpl(
      const std::string& config_filename, const std::string& master_name,
      const bool resolve_hostname = false);

 private:
  ClientImpl() = default;
  ClientImpl(common::ConfigManager* config_manager, 
             const std::string& master_name, 
             const bool resolve_hostname = false);

  // Internal function to cache file chunk metadata returned by master
  void cache_file_chunk_metadata(
      const std::string& filename,
      const uint32_t chunk_index,
      const protos::grpc::OpenFileReply& open_file_reply);

  // Internal function to read a file chunk
  google::protobuf::util::StatusOr<protos::grpc::ReadFileChunkReply>
      ReadFileChunk(const char* filename, size_t chunk_index, size_t offset, 
                    size_t nbytes); 

  // Internal function to write a file chunk
  google::protobuf::util::StatusOr<protos::grpc::WriteFileChunkReply>
      WriteFileChunk(const char* filename, void* buffer, size_t chunk_index,
                     size_t offset, size_t nbytes);

  // Get the client end point for chunk server service client. 
  std::shared_ptr<service::ChunkServerServiceGfsClient> 
      GetChunkServerServiceClient(const std::string& server_address);

  // Get the metadata for a chunk, including chunk_handle, version and 
  // the corresponding ChunkServerLocationEntry. This function checks first 
  // if all data above have been cached, if not then issue an OpenFileRequest
  // to fetch the information. This function is used by both ReadFileChunk
  // and WriteFileChunk. The last param forces an OpenFileRequest to get sent.
  // This is used when client finds its no primary no longer holds the lease. 
  // The returned status indicates if this operation is successful. 
  google::protobuf::util::Status 
      GetMetadataForChunk(
          const char* filename, size_t chunk_index,
          protos::grpc::OpenFileRequest::OpenMode file_open_mode,
          std::string& chunk_handle, uint32_t& version,
          CacheManager::ChunkServerLocationEntry& entry,
          bool force_get = false);

  // Register a client end-point for chunk server service client
  void RegisterChunkServerServiceClient(const std::string& server_address);

  // Reference to the configuration manager
  common::ConfigManager* config_manager_;

  // Reference to the cache manager
  CacheManager* cache_manager_;
  
  // Reference to the MasterMetadataService client
  std::shared_ptr<service::MasterMetadataServiceClient> 
      master_metadata_service_client_;

  // Reference to ChunkServerServiceGfs clients which can be accessed by the
  // chunk server addresses, the client will have to connect to different 
  // chunk servers. 
  common::parallel_hash_map<
      std::string, std::shared_ptr<service::ChunkServerServiceGfsClient>> 
          chunk_server_service_client_;
};

} // namespace client
} // namespace gfs

#endif // GFS_CLIENT_CLIENT_IMPL_H_
