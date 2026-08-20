#include "src/server/master_server/chunk_server_heartbeat_monitor_task.h"

#include <chrono>
#include <memory>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "src/common/protocol_client/chunk_server_control_service_client.h"
#include "src/common/protocol_client/grpc_client_utils.h"
#include "src/common/system_logger.h"
#include "src/server/master_server/chunk_server_manager.h"
#include "src/server/master_server/metadata_manager.h"

using gfs::common::ConfigManager;
using gfs::service::ChunkServerControlServiceClient;
using gfs::service::ChunkServerServiceMasterServerClient;
using absl::Status;
using absl::StatusOr;
using protos::grpc::CheckHeartBeatReply;
using protos::grpc::CheckHeartBeatRequest;
using protos::grpc::CloneFileChunkReply;
using protos::grpc::CloneFileChunkRequest;

namespace gfs {

namespace server {

ChunkServerHeartBeatMonitorTask::ChunkServerHeartBeatMonitorTask()
    : thread_(nullptr),
      terminate_promise_(nullptr),
      terminate_future_(nullptr),
      config_mgr_(nullptr),
      resolve_hostname_(false) {}

ChunkServerHeartBeatMonitorTask::~ChunkServerHeartBeatMonitorTask() {
  Terminate();
}

ChunkServerHeartBeatMonitorTask*
ChunkServerHeartBeatMonitorTask::GetInstance() {
  static ChunkServerHeartBeatMonitorTask instance;

  return &instance;
}

void ChunkServerHeartBeatMonitorTask::Start(ConfigManager* config_mgr,
                                            const bool resolve_hostname) {
  if (!this->terminate_promise_) {
    LOG(INFO) << "Chunk server heartbeat monitor task is starting...";

    LOG_ASSERT(config_mgr);
    this->config_mgr_ = config_mgr;

    this->resolve_hostname_ = resolve_hostname;

    // Create the promise used to signal the background thread to terminate.
    this->terminate_promise_ =
        std::unique_ptr<std::promise<void>>(new std::promise<void>());

    this->terminate_future_ =
        std::unique_ptr<std::future<void>>(new std::future<void>(
            std::move(this->terminate_promise_->get_future())));

    // Create the background thread to run the heartbeat monitoring work.
    this->thread_ = std::unique_ptr<std::thread>(new std::thread(
        &ChunkServerHeartBeatMonitorTask::MonitorHeartBeat, this));
  }
}

void ChunkServerHeartBeatMonitorTask::MonitorHeartBeat() {
  LOG(INFO) << "Chunk server heartbeat monitor task is now running in the "
               "background...";

  // TODO(bmokutub): Make this configurable, with the config manager.
  // Max number of times we should attempt to send heartbeat consecutively to a
  // chunkserver, before declaring it as unavailable.
  const ushort max_attempts = 3;

  while (true) {
    // Snapshot the registered chunk server locations first: the live map
    // must not be iterated while report threads may concurrently register
    // servers (phmap's per-operation mutexes do not protect iteration)
    std::vector<protos::ChunkServerLocation> registered_servers =
        ChunkServerManager::GetInstance().GetRegisteredServerLocations();

    for (auto& server_location : registered_servers) {
      // Check if we have been asked to terminate before proceeding.
      if (IsTerminateSignalled()) {
        return;
      }

      CheckHeartBeatRequest request;

      auto server_address = ResolveServerAddress(server_location);

      auto client = GetOrCreateChunkServerControlClient(server_address);

      LOG(INFO) << "Sending heartbeat message to chunk server: "
                << server_address;

      StatusOr<CheckHeartBeatReply> reply;
      for (ushort attempts = 1; attempts <= max_attempts; ++attempts) {
        reply = client->SendRequest(request);

        if (reply.ok()) {
          LOG(INFO) << "Received heartbeat from chunk server: "
                    << server_address;
          break;
        } else {
          // failed
          LOG(ERROR) << "Failed to receive heartbeat from chunk server: "
                     << server_address << " after " << attempts
                     << " attempt(s)."
                     << " Status: " << reply.status().ToString();
        }
      }

      // If reply isn't ok after all the attempts. We declare it as unavailable.
      // Lets unregister this chunkserver; the replication repair scan below
      // restores the replication level of the chunks it was holding.
      if (!reply.ok()) {
        LOG(INFO) << "Unregistering chunk server: " << server_address;
        ChunkServerManager::GetInstance().UnRegisterChunkServer(
            /*server_location=*/server_location);
      }
    }

    // Check for termination before the replication scan.
    if (IsTerminateSignalled()) {
      return;
    }

    // Regular background scan (GFS paper section 2.6.1): restore the
    // replication level of any chunk that lost replicas, e.g. because a
    // chunk server was just unregistered above or a stale replica was
    // discarded, as soon as a candidate server is available
    ScanAndRepairChunkReplication();

    // Lets check for termination before going to sleep.
    if (IsTerminateSignalled()) {
      return;
    }

    auto sleep_duration_secs =
        this->config_mgr_->GetHeartBeatTaskSleepDuration() / absl::Seconds(1);
    LOG(INFO) << "Chunk server heartbeat monitor task is going to sleep for "
              << sleep_duration_secs << " secs";
    // Sleep for the heartbeat interval
    std::this_thread::sleep_for(std::chrono::seconds(sleep_duration_secs));

    LOG(INFO) << "Chunk server heartbeat monitor task is now awake.";
  }
}

void ChunkServerHeartBeatMonitorTask::Terminate() {
  // Check if the task was ever started and its thread is still active
  if (this->thread_ && this->thread_->joinable()) {
    LOG(INFO)
        << "Sending terminate signal to chunk server heartbeat monitor task.";
    // Set value into promise to notify thread to terminate
    this->terminate_promise_->set_value();
    // wait for termination to complete
    this->thread_->join();
  }
}

std::string ChunkServerHeartBeatMonitorTask::ResolveServerAddress(
    const protos::ChunkServerLocation& location) {
  return this->config_mgr_->GetServerAddress(location,
                                             this->resolve_hostname_);
}

void ChunkServerHeartBeatMonitorTask::ScanAndRepairChunkReplication() {
  // Snapshot the chunk handles the master knows locations for (safe against
  // concurrent map mutation; see GetAllChunkHandles)
  for (const auto& chunk_handle :
       ChunkServerManager::GetInstance().GetAllChunkHandles()) {
    if (IsTerminateSignalled()) {
      return;
    }
    ReReplicateChunk(chunk_handle);
  }
}

void ChunkServerHeartBeatMonitorTask::ReReplicateChunk(
    const std::string& chunk_handle) {
  auto& chunk_server_manager = ChunkServerManager::GetInstance();
  auto* metadata_manager = MetadataManager::GetInstance();
  const uint32_t replication_goal = config_mgr_->GetReplicationFactor();

  // Only re-replicate chunks that still exist in the file metadata; chunks
  // of deleted files are garbage collected instead (GFS paper section 4.3:
  // "we prefer to first re-replicate chunks for live files")
  auto chunk_metadata_or = metadata_manager->GetFileChunkMetadata(chunk_handle);
  if (!chunk_metadata_or.ok()) {
    return;
  }
  const uint32_t chunk_version = chunk_metadata_or.value().version();

  auto live_locations = chunk_server_manager.GetChunkLocations(chunk_handle);
  if (live_locations.size() >= replication_goal) {
    return;  // still fully replicated
  }
  if (live_locations.empty()) {
    LOG(ERROR) << "Chunk " << chunk_handle
               << " has no live replicas left; cannot re-replicate it";
    return;
  }
  const std::vector<protos::ChunkServerLocation> source_candidates(
      live_locations.begin(), live_locations.end());

  // Pick target servers that do not yet hold a replica of this chunk
  // (snapshot; safe against concurrent registration)
  std::vector<protos::ChunkServerLocation> candidate_targets;
  for (auto& server_location :
       chunk_server_manager.GetRegisteredServerLocations()) {
    if (!live_locations.contains(server_location)) {
      candidate_targets.push_back(server_location);
    }
  }

  uint32_t replicas_needed = replication_goal - live_locations.size();
  for (const auto& target_location : candidate_targets) {
    if (replicas_needed == 0) {
      break;
    }
    const std::string target_address = ResolveServerAddress(target_location);

    // Rotate through source replicas on failure: a single bad source (e.g.
    // one with a corrupted block, whose read will fail checksum
    // verification) must not block repair forever while healthy sources
    // exist
    bool cloned = false;
    for (const auto& source_location : source_candidates) {
      LOG(INFO) << "Re-replicating chunk " << chunk_handle << " (version "
                << chunk_version << "): instructing " << target_address
                << " to clone it from " << source_location.server_hostname();

      CloneFileChunkRequest clone_request;
      clone_request.set_chunk_handle(chunk_handle);
      clone_request.set_chunk_version(chunk_version);
      *clone_request.mutable_source_location() = source_location;

      // Bound the wait: a hung target must not stall the heartbeat thread
      // (and with it all failure detection and repair)
      grpc::ClientContext client_context;
      gfs::common::SetClientContextDeadline(client_context, config_mgr_);
      auto client = GetOrCreateChunkServerFileServiceClient(target_address);
      StatusOr<CloneFileChunkReply> clone_reply_or =
          client->SendRequest(clone_request, client_context);

      if (clone_reply_or.ok() &&
          (clone_reply_or.value().status() == CloneFileChunkReply::OK ||
           clone_reply_or.value().status() ==
               CloneFileChunkReply::ALREADY_UP_TO_DATE)) {
        cloned = true;
        break;
      }
      LOG(ERROR) << "Re-replication of chunk " << chunk_handle << " to "
                 << target_address << " from "
                 << source_location.server_hostname()
                 << " failed; trying another source";
    }
    if (!cloned) {
      LOG(ERROR) << "Re-replication of chunk " << chunk_handle << " to "
                 << target_address << " failed from every source; will "
                 << "retry on a later scan";
      continue;
    }

    // Record the new replica so clients can use it right away (the chunk
    // server will also confirm it in its next report)
    chunk_server_manager.AddChunkReplica(target_location, chunk_handle);
    --replicas_needed;
    LOG(INFO) << "Chunk " << chunk_handle << " successfully re-replicated "
              << "to " << target_address;
  }
}

std::shared_ptr<ChunkServerServiceMasterServerClient>
ChunkServerHeartBeatMonitorTask::GetOrCreateChunkServerFileServiceClient(
    const std::string& server_address) {
  if (this->chunk_server_file_service_clients_.contains(server_address)) {
    return this->chunk_server_file_service_clients_[server_address];
  } else {
    LOG(INFO) << "Establishing new connection to chunk server:"
              << server_address << " for re-replication";

    this->chunk_server_file_service_clients_[server_address] =
        std::make_shared<ChunkServerServiceMasterServerClient>(
            grpc::CreateChannel(server_address,
                                grpc::InsecureChannelCredentials()));

    return this->chunk_server_file_service_clients_[server_address];
  }
}

bool ChunkServerHeartBeatMonitorTask::IsTerminateSignalled() {
  // Check if terminate promise has been set.
  if (this->terminate_future_->wait_for(std::chrono::milliseconds(1)) !=
      std::future_status::timeout) {
    LOG(INFO) << "Chunk server heartbeat monitor task received signal to "
                 "terminate. Terminating...";
    return true;
  }

  return false;
}

std::shared_ptr<ChunkServerControlServiceClient>
ChunkServerHeartBeatMonitorTask::GetOrCreateChunkServerControlClient(
    const std::string& server_address) {
  if (this->chunk_server_control_clients_.contains(server_address)) {
    return this->chunk_server_control_clients_[server_address];
  } else {
    LOG(INFO) << "Establishing new connection to chunk server:"
              << server_address << " for heartbeat monitoring";

    this->chunk_server_control_clients_[server_address] =
        std::make_shared<ChunkServerControlServiceClient>(grpc::CreateChannel(
            server_address, grpc::InsecureChannelCredentials()));

    return this->chunk_server_control_clients_[server_address];
  }
}

}  // namespace server
}  // namespace gfs
