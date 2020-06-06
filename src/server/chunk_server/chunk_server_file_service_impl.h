#ifndef GFS_SERVER_CHUNK_SERVER_CHUNK_SERVER_FILE_SERVICE_IMPL_H_
#define GFS_SERVER_CHUNK_SERVER_CHUNK_SERVER_FILE_SERVICE_IMPL_H_

#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"
#include "src/server/chunk_server/chunk_server_impl.h"
#include "src/server/chunk_server/file_chunk_manager.h"

namespace gfs {
namespace service {

// The synchronous implementation for handling ChunkServerFileService requests
class ChunkServerFileServiceImpl final
    : public protos::grpc::ChunkServerFileService::Service {
 public:
  ChunkServerFileServiceImpl(gfs::server::ChunkServerImpl* chunk_server_impl,
                             const bool clear_cached_data_after_write = true)
      : protos::grpc::ChunkServerFileService::Service(),
        chunk_server_impl_(chunk_server_impl),
        clear_cached_data_after_write_(clear_cached_data_after_write) {}

  // Handle an InitFileChunkRequest request sent by the master.
  grpc::Status InitFileChunk(grpc::ServerContext* context,
                             const protos::grpc::InitFileChunkRequest* request,
                             protos::grpc::InitFileChunkReply* reply) override;

  // Handle a ReadFileChunkRequest request by client or another chunk server.
  grpc::Status ReadFileChunk(grpc::ServerContext* context,
                             const protos::grpc::ReadFileChunkRequest* request,
                             protos::grpc::ReadFileChunkReply* reply) override;

  // Handle a SendChunkDataRequest request sent by the client.
  grpc::Status SendChunkData(grpc::ServerContext* context,
                             const protos::grpc::SendChunkDataRequest* request,
                             protos::grpc::SendChunkDataReply* reply) override;

  // Handle a WriteFileChunkRequest request sent by the client.
  grpc::Status WriteFileChunk(
      grpc::ServerContext* context,
      const protos::grpc::WriteFileChunkRequest* request,
      protos::grpc::WriteFileChunkReply* reply) override;

  // Handle an ApplyMutationsRequest request sent by the primary replica.
  grpc::Status ApplyMutations(
      grpc::ServerContext* context,
      const protos::grpc::ApplyMutationsRequest* request,
      protos::grpc::ApplyMutationsReply* reply) override;

  // Handle an AdvanceFileChunkVersionRequest request sent by the master.
  grpc::Status AdvanceFileChunkVersion(
      grpc::ServerContext* context,
      const protos::grpc::AdvanceFileChunkVersionRequest* request,
      protos::grpc::AdvanceFileChunkVersionReply* reply) override;

  gfs::server::ChunkServerImpl* chunk_server_impl_;
  gfs::server::FileChunkManager* file_manager_ =
      gfs::server::FileChunkManager::GetInstance();

 private:
  // Should we clear the cached data after write/apply mutation is done.
  // Mostly set to false for testing.
  bool clear_cached_data_after_write_;

  // Internal helper method to write file chunk to the local file chunk manager
  // and set reply status to the appropriate FileChunkMutationStatus. This is
  // used by both WriteFileChunk and ApplyMutations, to apply writes locally. It
  // gets the write data from cache if exist, and uses the file manager to write
  // data to the file chunk on disk.
  grpc::Status WriteFileChunkInternal(
      const protos::grpc::WriteFileChunkRequestHeader& request_header,
      protos::grpc::WriteFileChunkReply* const reply);
};

// The asynchronous implementation for handling ChunkServerFileService requests
// TODO(tugan): support handling master requests asynchronously
class ChunkServerFileServiceAsyncImpl final
    : public protos::grpc::ChunkServerFileService::Service {};

}  // namespace service
}  // namespace gfs

#endif  // GFS_SERVER_CHUNK_SERVER_CHUNK_SERVER_FILE_SERVICE_IMPL_H_