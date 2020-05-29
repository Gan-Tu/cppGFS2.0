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
  ConfigManager* config_manager = config_manager_or.ValueOrDie();
  ChunkServerImpl* chunks_server_impl = new ChunkServerImpl(config_manager);

  auto credentials = grpc::InsecureChannelCredentials();

  // initialize gRPC clients to talk to all master servers
  LOG(INFO) << "Estabalishing connections to all master servers...";
  for (std::string& server_name : config_manager->GetAllMasterServers()) {
    const std::string master_server_address =
        config_manager->GetServerAddress(server_name, resolve_hostname);
    if (!chunks_server_impl->RegisterMasterServerRpcClient(
            server_name,
            grpc::CreateChannel(master_server_address, credentials))) {
      return Status(
          google::protobuf::util::error::INTERNAL,
          absl::StrCat("Failed to connect to ", master_server_address));
    }
  }

  // initialize gRPC clients to talk to all other chunk servers
  LOG(INFO) << "Estabalishing connections to all other chunk servers...";
  for (std::string& server_name : config_manager->GetAllChunkServers()) {
    // connect to all chunk servers except itself
    // -  compare returns 0 : if both strings are equal.
    if (server_name.compare(chunk_server_name)) {
      const std::string chunk_server_address =
          config_manager->GetServerAddress(server_name, resolve_hostname);
      if (!chunks_server_impl->RegisterChunkServerRpcClient(
              server_name,
              grpc::CreateChannel(chunk_server_address, credentials))) {
        return Status(
            google::protobuf::util::error::INTERNAL,
            absl::StrCat("Failed to connect to ", chunk_server_address));
      }
    }
  }

  return chunks_server_impl;
}

void ChunkServerImpl::AddOrUpdateLease(std::string file_handle,
                                       uint64_t expiration_usec) {
  if (lease_and_expiration_usec_.contains(file_handle)) {
    lease_and_expiration_usec_[file_handle] = expiration_usec;
  } else {
    lease_and_expiration_usec_.insert({file_handle, expiration_usec});
  }
}

bool ChunkServerImpl::HasWriteLease(std::string file_handle) {
  if (!lease_and_expiration_usec_.contains(file_handle)) {
    return false;
  }
  // there could be a time-of-check and time-of-use race condition, but we will
  // allow it happen for now, and simply let caller retry
  absl::Time expiration_time =
      absl::FromUnixNanos(lease_and_expiration_usec_[file_handle]);
  return absl::Now() < expiration_time;
}

StatusOr<absl::Time> ChunkServerImpl::GetLeaseExpirationTime(
    std::string file_handle) {
  if (!lease_and_expiration_usec_.contains(file_handle)) {
    return Status(
        google::protobuf::util::error::NOT_FOUND,
        absl::StrCat("Lease is not found for file handle: ", file_handle));
  } else {
    return absl::FromUnixNanos(lease_and_expiration_usec_[file_handle]);
  }
}

void ChunkServerImpl::RemoveLease(std::string file_handle) {
  lease_and_expiration_usec_.erase(file_handle);
}

bool ChunkServerImpl::RegisterMasterServerRpcClient(
    std::string server_name, std::shared_ptr<grpc::Channel> channel) {
  auto iter_and_inserted = master_server_clients_.insert(
      {server_name,
       std::make_shared<MasterChunkServerManagerServiceClient>(channel)});
  return iter_and_inserted.second;
}

bool ChunkServerImpl::RegisterChunkServerRpcClient(
    std::string server_name, std::shared_ptr<grpc::Channel> channel) {
  auto iter_and_inserted = chunk_server_clients_.insert(
      {server_name,
       std::make_shared<ChunkServerServiceChunkServerClient>(channel)});
  return iter_and_inserted.second;
}

}  // namespace server
}  // namespace gfs