#ifndef GFS_SERVER_CHUNK_SERVER_CHUNK_SERVER_FILE_SERVICE_IMPL_H_
#define GFS_SERVER_CHUNK_SERVER_CHUNK_SERVER_FILE_SERVICE_IMPL_H_

#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"

namespace gfs {
namespace service {

// The synchronous implementation for handling ChunkServerFileService requests
class ChunkServerFileServiceImpl final
    : public protos::grpc::ChunkServerFileService::Service {
  // Handle an InitFileChunkRequest request sent by the master.
  grpc::Status InitFileChunk(grpc::ServerContext* context,
                             const protos::grpc::InitFileChunkRequest* request,
                             protos::grpc::InitFileChunkReply* reply) override;

  // Handle an ReadFileChunkRequest request by anyone.
  grpc::Status ReadFileChunk(grpc::ServerContext* context,
                             const protos::grpc::ReadFileChunkRequest* request,
                             protos::grpc::ReadFileChunkReply* reply) override;

  // Handle an WriteFileChunkRequest request sent by the client.
  grpc::Status WriteFileChunk(
      grpc::ServerContext* context,
      const protos::grpc::WriteFileChunkRequest* request,
      protos::grpc::WriteFileChunkReply* reply) override;

  // Handle an AdvanceFileChunkVersionRequest request sent by the master.
  grpc::Status AdvanceFileChunkVersion(
      grpc::ServerContext* context,
      const protos::grpc::AdvanceFileChunkVersionRequest* request,
      protos::grpc::AdvanceFileChunkVersionReply* reply) override;
};

// The asynchronous implementation for handling ChunkServerFileService requests
// TODO(tugan): support handling master requests asynchronously
class ChunkServerFileServiceAsyncImpl final
    : public protos::grpc::ChunkServerFileService::Service {};

}  // namespace service
}  // namespace gfs

#endif  // GFS_SERVER_CHUNK_SERVER_CHUNK_SERVER_FILE_SERVICE_IMPL_H_