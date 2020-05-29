#ifndef GFS_SERVER_CHUNK_SERVER_IMPL_H_
#define GFS_SERVER_CHUNK_SERVER_IMPL_H_

#include "absl/container/flat_hash_map.h"
#include "absl/time/time.h"
#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "src/common/config_manager.h"
#include "src/common/protocol_client/chunk_server_service_server_client.h"
#include "src/common/protocol_client/master_chunk_server_manager_service_client.h"
#include "src/common/utils.h"

namespace gfs {
namespace server {

class ChunkServerImpl {
 public:
  static google::protobuf::util::StatusOr<ChunkServerImpl*>
  ConstructChunkServerImpl(const std::string& config_filename,
                           const std::string& chunk_server_name,
                           const bool resolve_hostname = false);

  // Write lease management
  void AddOrUpdateLease(const std::string& file_handle,
                        const uint64_t expiration_usec);
  void RemoveLease(const std::string& file_handle);
  bool HasWriteLease(const std::string& file_handle);
  // Return NOT_FOUND, if no lease exists for file handle
  google::protobuf::util::StatusOr<absl::Time> GetLeaseExpirationTime(
      const std::string& file_handle);

  // mock functions for chunk file manager
  // TODO(tugan,michael): use chunk file manager instead, when ready
  void SetChunkVersion(const std::string& file_handle, const uint32_t version);
  // Return NOT_FOUND, if no chunk exists on this chunk server
  google::protobuf::util::StatusOr<uint32_t> GetChunkVersion(
      const std::string& file_handle);

  // Register the gRPC protocol client for connecting to the server listening
  // on the given channel; if a client has already been registered for the
  // server, return false
  bool RegisterMasterServerRpcClient(const std::string& server_name,
                                     std::shared_ptr<grpc::Channel> channel);
  bool RegisterChunkServerRpcClient(const std::string& server_name,
                                    std::shared_ptr<grpc::Channel> channel);

 private:
  ChunkServerImpl() = default;
  ChunkServerImpl(gfs::common::ConfigManager* config_manager)
      : config_manager_(config_manager) {}

  gfs::common::ConfigManager* config_manager_;

  // Master server name and its corresponding GFS protocol client
  gfs::common::thread_safe_flat_hash_map<
      std::string,
      std::shared_ptr<gfs::service::MasterChunkServerManagerServiceClient>>
      master_server_clients_;

  // Chunk server name and its corresponding GFS protocol client
  gfs::common::thread_safe_flat_hash_map<
      std::string,
      std::shared_ptr<gfs::service::ChunkServerServiceChunkServerClient>>
      chunk_server_clients_;

  // File handles and its version
  // TODO(tugan,michael): use chunk file manager instead, when ready
  gfs::common::thread_safe_flat_hash_map<std::string, uint32_t> chunk_versions_;

  // Write leases that chunk server holds, and their respective expiration time
  gfs::common::thread_safe_flat_hash_map<std::string, uint64_t>
      lease_and_expiration_usec_;
};

}  // namespace server
}  // namespace gfs

#endif  // GFS_SERVER_CHUNK_SERVER_IMPL_H_
