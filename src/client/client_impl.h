#ifndef GFS_CLIENT_CLIENT_IMPL_H_
#define GFS_CLIENT_CLIENT_IMPL_H_

#include <vector>

#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "src/client/client_cache_manager.h"
#include "src/common/config_manager.h"
#include "src/common/protocol_client/chunk_server_service_gfs_client.h"
#include "src/common/protocol_client/master_metadata_service_client.h"

namespace gfs {
namespace client {

// The ClientImpl class provides interfaces for the client library to access
// the configuration manager, cache manager and grpc clients
class ClientImpl {
 public:
  // Internal impl call that issues an OpenFileRequest to the master. 
  google::protobuf::util::Status CreateFile(const std::string& filename);
    
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

  // Helper function to set deadline for a client context object using the 
  // timeout configuration obtained from the config manager
  void SetClientContextDeadline(grpc::ClientContext& client_context); 

  // Reference to the configuration manager
  common::ConfigManager* config_manager_;

  // Reference to the cache manager
  CacheManager* cache_manager_;
  
  // Reference to the MasterMetadataService client
  service::MasterMetadataServiceClient* master_metadata_service_client_;

  // Reference to a list of ChunkServerServiceGfs clients, as the client 
  // will have to connect to different chunk servers. 
  std::vector<service::ChunkServerServiceGfsClient*> 
      chunk_server_service_client_;
};

} // namespace client
} // namespace gfs

#endif // GFS_CLIENT_CLIENT_IMPL_H_
