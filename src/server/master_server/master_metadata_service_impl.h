#ifndef GFS_SERVER_MASTER_SERVER_MASTER_METADATA_SERVICE_IMPL_H_
#define GFS_SERVER_MASTER_SERVER_MASTER_METADATA_SERVICE_IMPL_H_

#include "absl/container/flat_hash_map.h"
#include "grpcpp/grpcpp.h"
#include "src/common/protocol_client/chunk_server_service_client.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"

namespace gfs {
namespace service {

// The synchronous implementation for handling MasterMetadataService requests
class MasterMetadataServiceImpl final
    : public protos::grpc::MasterMetadataService::Service {
 public:
  // Register a protocol client for talking to the lease and file service
  // of given |server_name| chunk server, using the gRPC |channel|.
  // If the client already exits, return false.
  bool RegisterChunkServerRpcClient(std::string server_name,
                                    std::shared_ptr<grpc::Channel> channel);

 protected:
  // Handle an OpenFileRequest request sent by the client.
  grpc::Status OpenFile(grpc::ServerContext* context,
                        const protos::grpc::OpenFileRequest* request,
                        protos::grpc::OpenFileReply* reply) override;

  // Handle a DeleteFileRequest request sent by the client.
  grpc::Status DeleteFile(grpc::ServerContext* context,
                          const protos::grpc::DeleteFileRequest* request,
                          google::protobuf::Empty* reply) override;

  // Chunk server name and its corresponding GFS protocol client
  absl::flat_hash_map<std::string,
                      std::shared_ptr<gfs::service::ChunkServerServiceClient>>
      chunk_server_service_clients_;
};

// The asynchronous implementation for handling MasterMetadataService requests
// TODO(tugan): support handling client requests asynchronously
class MasterMetadataServiceAsyncImpl final
    : public protos::grpc::MasterMetadataService::Service {};

}  // namespace service
}  // namespace gfs

#endif  // GFS_SERVER_MASTER_SERVER_MASTER_METADATA_SERVICE_IMPL_H_
