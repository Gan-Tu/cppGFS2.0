#include "src/server/chunk_server/chunk_server_impl.h"

#include <chrono>

#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "src/common/system_logger.h"
#include "src/server/chunk_server/file_chunk_manager.h"

using gfs::common::ConfigManager;
using gfs::common::thread_safe_flat_hash_map;
using gfs::service::ChunkServerServiceChunkServerClient;
using gfs::service::MasterChunkServerManagerServiceClient;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using protos::grpc::ReportChunkServerRequest;

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

google::protobuf::util::StatusOr<uint32_t> ChunkServerImpl::GetChunkVersion(
    const std::string& file_handle) {
  return file_manager_->GetChunkVersion(file_handle);
}

//
// gRPC Protocol Helpers
//

std::shared_ptr<MasterChunkServerManagerServiceClient>
ChunkServerImpl::GetMasterProtocolClient(const std::string& server_address) {
  if (master_server_clients_.contains(server_address)) {
    return master_server_clients_[server_address];
  } else {
    RegisterMasterProtocolClient(server_address);
    return master_server_clients_[server_address];
  }
}

void ChunkServerImpl::RegisterMasterProtocolClient(
    const std::string& server_address) {
  LOG(INFO) << "Establishing new connection to master:" << server_address;
  master_server_clients_[server_address] =
      std::make_shared<MasterChunkServerManagerServiceClient>(
          grpc::CreateChannel(server_address,
                              grpc::InsecureChannelCredentials()));
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

bool ChunkServerImpl::ReportToMaster() {
  // Prepare the report chunk server request
  ReportChunkServerRequest request;

  // Add the chunk server information to the request
  auto chunk_server = request.mutable_chunk_server();
  auto chunk_server_location = chunk_server->mutable_location();

  chunk_server_location->set_server_hostname(
      config_manager_->GetServerHostname(chunk_server_name_));
  chunk_server_location->set_server_port(
      config_manager_->GetServerPort(chunk_server_name_));

  // TODO(bmokutub): Use std::filesystem to get the available disk space.
  // Setting to 20GB for now, fix this.
  chunk_server->set_available_disk_mb(/*available_disk_mb=*/20 * 1024);

  // We also need to tell the master all the chunks we have, if any.
  auto all_chunks_metadata =
      FileChunkManager::GetInstance()->GetAllFileChunkMetadata();

  LOG(INFO) << "Found " + std::to_string(all_chunks_metadata.size()) +
                   " stored chunks to report to master.";

  for (auto& chunk_metadata : all_chunks_metadata) {
    // TODO(bmokutub): Also include chunk version in request so master can check
    // if it is stale. Not needed for now.
    chunk_server->add_stored_chunk_handles(chunk_metadata.chunk_handle());
  }

  // send the request to the master server(s), if more than one
  uint32_t successful_report = 0;
  for (auto& master_chunk_server_mgr_client : master_server_clients_) {
    LOG(INFO) << "Reporting to master server: " +
                     master_chunk_server_mgr_client.first;

    auto reply = master_chunk_server_mgr_client.second->SendRequest(request);
    if (reply.ok()) {
      // Check the reply for stale chunks, if any, for deletion. Call the 
      // FileChunkManager to delete the file chunk
      auto report_reply(reply.ValueOrDie());
      for (auto& stale_chunk_handle : report_reply.stale_chunk_handles()) {
        LOG(INFO) << "Received stale / deleted chunk handle " 
                  << stale_chunk_handle << ". File chunk server deleting "
                  << "the actual file chunk";
        FileChunkManager::GetInstance()->DeleteChunk(stale_chunk_handle);
      }
      ++successful_report;
    } else {
      // failed
      LOG(ERROR) << "Report chunk server request to master server: " +
                        master_chunk_server_mgr_client.first +
                        " failed. Status: " + reply.status().ToString();
    }
  }

  // For now return true if atleast one of the report request succeeded. Meaning
  // atleast one master server knows about this chunk server.
  return successful_report > 0;
}

void ChunkServerImpl::StartReportToMaster() {
  chunk_reporting_thread_ = new std::thread([&]() {
    // Periodically run ReportToMaster until instructed to terminate
    while (!reporting_thread_terminated_.load()) {
      if (!this->ReportToMaster()) {
        LOG(ERROR) << "Failed to report to any master server, retry laster";
      }

      // TODO(Xi/bmokutub): Ideally need to make this sleep duration a separate
      // configurable parameter.
      auto sleep_duration_secs 
          = this->config_manager_->GetHeartBeatTaskSleepDuration() 
                / absl::Seconds(1);
      LOG(INFO) << "Chunk server ReportToMaster service going to sleep for "
                << sleep_duration_secs << " secs";
      // Sleep for the heartbeat interval
      std::this_thread::sleep_for(std::chrono::seconds(sleep_duration_secs));
    }
  }); 
}

void ChunkServerImpl::TerminateReportToMaster() {
  // Simply set the automic flag to be true
  reporting_thread_terminated_.store(true);
}

gfs::common::ConfigManager* ChunkServerImpl::GetConfigManager() const {
  return config_manager_;
}

bool ChunkServerImpl::ResolveHostname() const { return resolve_hostname_; }

}  // namespace server
}  // namespace gfs
