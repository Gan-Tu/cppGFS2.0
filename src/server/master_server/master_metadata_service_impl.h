#ifndef GFS_SERVER_MASTER_SERVER_MASTER_METADATA_SERVICE_IMPL_H_
#define GFS_SERVER_MASTER_SERVER_MASTER_METADATA_SERVICE_IMPL_H_

#include "absl/container/flat_hash_map.h"
#include "grpcpp/grpcpp.h"
#include "src/common/config_manager.h"
#include "src/common/protocol_client/chunk_server_service_server_client.h"
#include "src/common/utils.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"
#include "src/server/master_server/chunk_server_manager.h"
#include "src/server/master_server/metadata_manager.h"

namespace gfs {
namespace service {

// The synchronous implementation for handling MasterMetadataService requests
class MasterMetadataServiceImpl final
    : public protos::grpc::MasterMetadataService::Service {
 public:
  MasterMetadataServiceImpl(common::ConfigManager* config_manager) : 
      config_manager_(config_manager) {}

  // Register a protocol client for talking to the lease and file service
  // of given |server_name| chunk server, using the gRPC |channel|.
  // If the client already exits, return false.
  bool RegisterChunkServerRpcClient(std::string server_name,
                                    std::shared_ptr<grpc::Channel> channel);

 protected:
  // Accessor to the MetadataManager instance
  server::MetadataManager* metadata_manager();

  // Accessor to the ChunkServerManager instance
  server::ChunkServerManager& chunk_server_manager();

  // Handle file creation request. This function is called by OpenFile
  // function to dispatch the task for creating a file
  grpc::Status HandleFileCreation(const protos::grpc::OpenFileRequest* request,
                                  protos::grpc::OpenFileReply* reply);

  // Handle file read request. This function is called by OpenFile
  // function to dispatch the task for read a file chunk
  grpc::Status HandleFileChunkRead(const protos::grpc::OpenFileRequest* request,
                                   protos::grpc::OpenFileReply* reply);

  // Handle file read request. This function is called by OpenFile
  // function to dispatch the task for writing to a file chunk
  grpc::Status HandleFileChunkWrite(
      const protos::grpc::OpenFileRequest* request,
      protos::grpc::OpenFileReply* reply);

  // Handle an OpenFileRequest request sent by the client.
  grpc::Status OpenFile(grpc::ServerContext* context,
                        const protos::grpc::OpenFileRequest* request,
                        protos::grpc::OpenFileReply* reply) override;

  // Handle a DeleteFileRequest request sent by the client.
  grpc::Status DeleteFile(grpc::ServerContext* context,
                          const protos::grpc::DeleteFileRequest* request,
                          google::protobuf::Empty* reply) override;

  // Chunk server name and its corresponding GFS protocol client. Use a thread
  // safe hashmap as the server code is intrinsically multi-threaded and this
  // data structure needs to be protected
  common::parallel_hash_map<
      std::string,
      std::shared_ptr<gfs::service::ChunkServerServiceMasterServerClient>>
      chunk_server_service_clients_;

  // Reference to the config manager in order to access some configurable 
  // params such as grpc timeout
  common::ConfigManager* config_manager_;
};

// The asynchronous implementation for handling MasterMetadataService requests
// TODO(tugan): support handling client requests asynchronously
class MasterMetadataServiceAsyncImpl final
    : public protos::grpc::MasterMetadataService::Service {};

}  // namespace service
}  // namespace gfs

#endif  // GFS_SERVER_MASTER_SERVER_MASTER_METADATA_SERVICE_IMPL_H_
