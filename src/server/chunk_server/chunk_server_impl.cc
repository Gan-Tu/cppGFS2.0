#include "src/server/chunk_server/chunk_server_impl.h"

#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "src/common/system_logger.h"

using gfs::common::ConfigManager;
using gfs::common::thread_safe_flat_hash_map;
using gfs::service::ChunkServerServiceChunkServerClient;
using gfs::service::MasterChunkServerManagerServiceClient;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;

namespace gfs {
namespace server {

ChunkServerImpl::~ChunkServerImpl() {
  master_server_clients_.clear();
  chunk_server_clients_.clear();
  chunk_versions_.clear();
  lease_and_expiration_unix_sec_.clear();
}

StatusOr<ChunkServerImpl*> ChunkServerImpl::ConstructChunkServerImpl(
    const std::string& config_filename, const std::string& chunk_server_name,
    const bool resolve_hostname) {
  LOG(INFO) << "Parsing configuration file...";
  // Instantiate a ConfigManager with the given filename
  StatusOr<ConfigManager*> config_manager_or(
      ConfigManager::GetConfig(config_filename));
  if (!config_manager_or.ok()) {
    return config_manager_or.status();
  }
  return new ChunkServerImpl(config_manager_or.ValueOrDie(), chunk_server_name,
                             resolve_hostname);
}

//
// Lease Management
//

void ChunkServerImpl::AddOrUpdateLease(const std::string& file_handle,
                                       const uint64_t expiration_unix_sec) {
  lease_and_expiration_unix_sec_[file_handle] = expiration_unix_sec;
}

bool ChunkServerImpl::HasWriteLease(const std::string& file_handle) {
  if (!lease_and_expiration_unix_sec_.contains(file_handle)) {
    return false;
  }
  // there could be a time-of-check and time-of-use race condition, but we will
  // allow it happen for now, and simply let caller retry
  absl::Time expiration_time =
      absl::FromUnixSeconds(lease_and_expiration_unix_sec_[file_handle]);
  return absl::Now() < expiration_time;
}

StatusOr<absl::Time> ChunkServerImpl::GetLeaseExpirationTime(
    const std::string& file_handle) {
  if (!lease_and_expiration_unix_sec_.contains(file_handle)) {
    return Status(
        google::protobuf::util::error::NOT_FOUND,
        absl::StrCat("Lease is not found for file handle: ", file_handle));
  } else {
    return absl::FromUnixSeconds(lease_and_expiration_unix_sec_[file_handle]);
  }
}

void ChunkServerImpl::RemoveLease(const std::string& file_handle) {
  lease_and_expiration_unix_sec_.erase(file_handle);
}

//
// Mock Functions - File Chunks management
//

void ChunkServerImpl::SetChunkVersion(const std::string& file_handle,
                                      const uint32_t version) {
  // TODO(tugan,michael): use chunk file manager instead, when ready
  chunk_versions_[file_handle] = version;
}

google::protobuf::util::StatusOr<uint32_t> ChunkServerImpl::GetChunkVersion(
    const std::string& file_handle) {
  // TODO(tugan,michael): use chunk file manager instead, when ready
  // if (!chunk_versions_.contains(file_handle)) {
  //   return Status(google::protobuf::util::error::NOT_FOUND,
  //                 absl::StrCat("File chunk is not found: ", file_handle));
  // } else {
  //   return chunk_versions_[file_handle];
  // }
  return 1;
}

//
// gRPC Protocol Helpers
//

std::shared_ptr<MasterChunkServerManagerServiceClient>
ChunkServerImpl::GetMasterProtocolClient(const std::string& server_address) {
  if (master_server_clients_.contains(server_address)) {
    return master_server_clients_[server_address];
  } else {
    LOG(INFO) << "Estabalishing new connection to master:" << server_address;
    master_server_clients_[server_address] =
        std::make_shared<MasterChunkServerManagerServiceClient>(
            grpc::CreateChannel(server_address,
                                grpc::InsecureChannelCredentials()));
    return master_server_clients_[server_address];
  }
}

std::shared_ptr<ChunkServerServiceChunkServerClient>
ChunkServerImpl::GetChunkServerProtocolClient(
    const std::string& server_address) {
  if (chunk_server_clients_.contains(server_address)) {
    return chunk_server_clients_[server_address];
  } else {
    LOG(INFO) << "Estabalishing new connection to chunk server:"
              << server_address;
    chunk_server_clients_[server_address] =
        std::make_shared<ChunkServerServiceChunkServerClient>(
            grpc::CreateChannel(server_address,
                                grpc::InsecureChannelCredentials()));
    return chunk_server_clients_[server_address];
  }
}

}  // namespace server
}  // namespace gfs