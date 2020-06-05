#include "src/server/master_server/chunk_server_heartbeat_monitor_task.h"

#include <chrono>
#include <memory>

#include "absl/strings/str_cat.h"
#include "src/common/protocol_client/chunk_server_control_service_client.h"
#include "src/common/system_logger.h"
#include "src/server/master_server/chunk_server_manager.h"

using gfs::common::ConfigManager;
using gfs::service::ChunkServerControlServiceClient;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using protos::grpc::CheckHeartBeatReply;
using protos::grpc::CheckHeartBeatRequest;

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

void ChunkServerHeartBeatMonitorTask::Start(const std::string& config_file,
                                            const bool resolve_hostname) {
  if (!this->terminate_promise_) {
    LOG(INFO) << "Chunk server heartbeat monitor task is starting...";

    // Instantiate a ConfigManager with the given filename
    StatusOr<ConfigManager*> config_manager_or(
        ConfigManager::GetConfig(config_file));

    this->config_mgr_ =
        std::unique_ptr<ConfigManager>(config_manager_or.ValueOrDie());

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
    for (auto& chunk_server :
         ChunkServerManager::GetInstance().chunk_servers_map_) {
      // Check if we have been asked to terminate before proceeding.
      if (IsTerminateSignalled()) {
        return;
      }

      CheckHeartBeatRequest request;

      std::string hostname = chunk_server.first.server_hostname();
      if (this->resolve_hostname_) {
        hostname = this->config_mgr_->ResolveHostname(hostname);
      }

      auto server_address =
          absl::StrCat(hostname, ":", chunk_server.first.server_port());

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
      // Lets unregister this chunkserver.
      if (!reply.ok()) {
        LOG(INFO) << "Unregistering chunk server: " << server_address;
        ChunkServerManager::GetInstance().UnRegisterChunkServer(
            /*server_location=*/chunk_server.first);
      }
    }

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
  // Check if thread is still active
  if (this->thread_->joinable()) {
    LOG(INFO)
        << "Sending terminate signal to chunk server heartbeat monitor task.";
    // Set value into promise to notify thread to terminate
    this->terminate_promise_->set_value();
    // wait for termination to complete
    this->thread_->join();
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
