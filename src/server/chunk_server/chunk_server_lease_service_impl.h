#ifndef GFS_SERVER_CHUNK_SERVER_CHUNK_SERVER_LEASE_SERVICE_IMPL_H_
#define GFS_SERVER_CHUNK_SERVER_CHUNK_SERVER_LEASE_SERVICE_IMPL_H_

#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/chunk_server_lease_service.grpc.pb.h"
#include "src/server/chunk_server/chunk_server_impl.h"

namespace gfs {
namespace service {

// The synchronous implementation for handling ChunkServerLeaseService requests
class ChunkServerLeaseServiceImpl final
    : public protos::grpc::ChunkServerLeaseService::Service {
 public:
  ChunkServerLeaseServiceImpl(gfs::server::ChunkServerImpl* chunk_server_impl)
      : protos::grpc::ChunkServerLeaseService::Service(),
        chunk_server_impl_(chunk_server_impl) {}
        
  // Handle an GrantLeaseRequest request sent by the master.
  grpc::Status GrantLease(grpc::ServerContext* context,
                          const protos::grpc::GrantLeaseRequest* request,
                          protos::grpc::GrantLeaseReply* reply) override;

  // Handle a RevokeLeaseRequest request sent by the master.
  grpc::Status RevokeLease(grpc::ServerContext* context,
                           const protos::grpc::RevokeLeaseRequest* request,
                           protos::grpc::RevokeLeaseReply* reply) override;

  gfs::server::ChunkServerImpl* chunk_server_impl_;
};

// The asynchronous implementation for handling ChunkServerLeaseService requests
// TODO(tugan): support handling master requests asynchronously
class ChunkServerLeaseServiceAsyncImpl final
    : public protos::grpc::ChunkServerLeaseService::Service {};

}  // namespace service
}  // namespace gfs

#endif  // GFS_SERVER_CHUNK_SERVER_CHUNK_SERVER_LEASE_SERVICE_IMPL_H_