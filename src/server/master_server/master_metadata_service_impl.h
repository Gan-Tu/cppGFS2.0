#ifndef GFS_SERVER_MASTER_SERVER_MASTER_METADATA_SERVICE_IMPL_H_
#define GFS_SERVER_MASTER_SERVER_MASTER_METADATA_SERVICE_IMPL_H_

#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"
#include "src/server/master_server/metadata_manager.h"

namespace gfs {
namespace service {

// The synchronous implementation for handling MasterMetadataService requests
class MasterMetadataServiceImpl final
    : public protos::grpc::MasterMetadataService::Service {
  // Accessor to the MetadataManager instance
  server::MetadataManager* metadata_manager(); 

       
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
};

// The asynchronous implementation for handling MasterMetadataService requests
// TODO(tugan): support handling client requests asynchronously
class MasterMetadataServiceAsyncImpl final
    : public protos::grpc::MasterMetadataService::Service {};

}  // namespace service
}  // namespace gfs

#endif // GFS_SERVER_MASTER_SERVER_MASTER_METADATA_SERVICE_IMPL_H_
