#include "src/server/master_server/chunk_server_heartbeat_monitor_task.h"

#include <chrono>
#include <thread>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/status/statusor.h"
#include "grpcpp/grpcpp.h"
#include "gtest/gtest.h"
#include "src/common/config_manager.h"
#include "src/server/chunk_server/chunk_server_control_service_impl.h"
#include "src/server/chunk_server/chunk_server_impl.h"
#include "src/server/chunk_server/file_chunk_manager.h"
#include "src/server/master_server/chunk_server_manager.h"
#include "src/server/master_server/master_chunk_server_manager_service_impl.h"

using gfs::common::ConfigManager;
using gfs::server::ChunkServerHeartBeatMonitorTask;
using gfs::server::ChunkServerImpl;
using gfs::server::ChunkServerManager;
using gfs::server::FileChunkManager;
using gfs::service::ChunkServerControlServiceImpl;
using gfs::service::MasterChunkServerManagerServiceImpl;
using absl::StatusOr;
using grpc::Server;
using grpc::ServerBuilder;
using protos::grpc::CheckHeartBeatReply;
using protos::grpc::CheckHeartBeatRequest;
using protos::grpc::ReportChunkServerReply;
using protos::grpc::ReportChunkServerRequest;

const std::string kTestConfigPath =
    "tests/server/master_server/test_config.yml";
const std::string kTestMasterServerName = "test_master";
const std::string kTestMasterServerAddress = "0.0.0.0:10022";

namespace {
// A running test gRPC server plus the resources it needs to stay alive.
// Killing server threads with pthread_cancel (as this test previously did)
// aborts the whole process with modern gRPC, so servers are stopped with a
// clean Shutdown() instead; from the master's point of view a shut-down
// server is just as dead (connections are refused).
struct TestGrpcServer {
  std::unique_ptr<ChunkServerControlServiceImpl> control_service;
  std::unique_ptr<MasterChunkServerManagerServiceImpl> master_service;
  std::unique_ptr<Server> server;
  std::thread wait_thread;
};

std::unique_ptr<TestGrpcServer> StartChunkServerControlService(
    const std::string& server_address, ChunkServerImpl* const chunk_server) {
  auto test_server = std::unique_ptr<TestGrpcServer>(new TestGrpcServer());
  ServerBuilder builder;
  auto credentials = grpc::InsecureServerCredentials();
  builder.AddListeningPort(server_address, credentials);

  // Register a synchronous service for the chunkserver to respond to heartbeat.
  test_server->control_service =
      std::unique_ptr<ChunkServerControlServiceImpl>(
          new ChunkServerControlServiceImpl(chunk_server));
  builder.RegisterService(test_server->control_service.get());

  // Start the server, and let it run until shut down
  test_server->server = builder.BuildAndStart();
  Server* server = test_server->server.get();
  test_server->wait_thread = std::thread([server]() { server->Wait(); });
  return test_server;
}

std::unique_ptr<TestGrpcServer> StartTestMasterChunkServerManagerService() {
  auto test_server = std::unique_ptr<TestGrpcServer>(new TestGrpcServer());
  ServerBuilder builder;
  auto credentials = grpc::InsecureServerCredentials();
  builder.AddListeningPort(kTestMasterServerAddress, credentials);

  // Register a synchronous service for coordinating with chunkservers
  test_server->master_service =
      std::unique_ptr<MasterChunkServerManagerServiceImpl>(
          new MasterChunkServerManagerServiceImpl());
  builder.RegisterService(test_server->master_service.get());

  // Start the server, and let it run until shut down
  test_server->server = builder.BuildAndStart();
  Server* server = test_server->server.get();
  test_server->wait_thread = std::thread([server]() { server->Wait(); });
  return test_server;
}

void ShutdownServer(TestGrpcServer& test_server) {
  test_server.server->Shutdown();
  test_server.wait_thread.join();
}
}  // namespace

class ChunkServerHeartBeatMonitorTaskTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    // Need file chunk manager to be initialized because of the report chunk
    // server needs to check for stored chunks.
    // The DB can only be initialized and open once by each thread, so we put
    // it in SetUpTestSuite for one-time setup
    FileChunkManager* file_chunk_mgr = FileChunkManager::GetInstance();
    file_chunk_mgr->Initialize(
        // Do NOT use /tmp as the test artifacts will persist this way
        // Instead, use relative directory for Bazel test so it will be cleaned
        // up after the test finishes
        /*chunk_database_name=*/"chunk_server_heartbeat_test_db",
        /*max_chunk_size_bytes=*/100);
  }

  void SetUp() override {
    // Create the config manager for the test to read test config.
    config_mgr_ = std::shared_ptr<ConfigManager>(
        ConfigManager::GetConfig(kTestConfigPath).value());

    ChunkServerHeartBeatMonitorTask::GetInstance()->Start(
        config_mgr_.get(), /*resolve_hostname=*/true);
  }

  // Executed after each test method in the suite.
  void TearDown() override {
    ChunkServerHeartBeatMonitorTask::GetInstance()->Terminate();

    // Unregister all the ChunkServers registered by the test.
    ChunkServerManager::GetInstance().UnRegisterAllChunkServers();
  }

  std::shared_ptr<ConfigManager> config_mgr_;
};

// This tests the end to end heartbeat functionality. A chunkservers report
// to the master, and heartbeat task keeps checking the chunkservers, then
// we kill one of the chunkservers and make sure it is no longer available for
// chunk allocation.
TEST_F(ChunkServerHeartBeatMonitorTaskTest, MonitorHeartBeatTest) {
  std::vector<std::string> chunk_server_names = {"test_chunk_server_01",
                                                 "test_chunk_server_02"};

  std::vector<std::unique_ptr<TestGrpcServer>> chunk_server_servers;

  for (auto& chunk_server_name : chunk_server_names) {
    // Create the chunkserver
    StatusOr<ChunkServerImpl*> chunk_server_or =
        ChunkServerImpl::ConstructChunkServerImpl(kTestConfigPath,
                                                  chunk_server_name);
    ChunkServerImpl* chunk_server = chunk_server_or.value();

    // Start the chunkserver control service in another thread, that responds to
    // heartbeat messages from master.
    chunk_server_servers.push_back(StartChunkServerControlService(
        config_mgr_->GetServerAddress(chunk_server_name,
                                      /*resolve_hostname=*/true),
        chunk_server));

    // Get the master server address
    const std::string master_server_address = config_mgr_->GetServerAddress(
        kTestMasterServerName, /*resolve_hostname=*/true);
    // Check that we got the right master address from config manager
    EXPECT_EQ(kTestMasterServerAddress, master_server_address);

    // Chunk server creates connection to the master server.
    chunk_server->RegisterMasterProtocolClient(master_server_address);

    // Check that the chunk server was able to report itself to the master
    // server. Once a chunkserver reports to the master, then the heartbeat task
    // starts monitoring it.
    EXPECT_TRUE(chunk_server->ReportToMaster());
  }

  // The chunk server manager runs on the master server, but here since master
  // is just running as a seperate thread we can check if the report chunkserver
  // was truly successful and master can now select chunkservers for chunk
  // allocation.
  // Allocate using all the reported chunkservers.
  auto allocated_locations =
      ChunkServerManager::GetInstance().AllocateChunkServer(
          /*chunk_handle=*/"TestHandle",
          /*server_count=*/chunk_server_names.size());

  EXPECT_EQ(chunk_server_names.size(), allocated_locations.size());

  // Shut down the first chunk server so it stops answering heartbeats
  ShutdownServer(*chunk_server_servers[0]);

  // Wait for heartbeat task to notice this chunkserver is down
  auto heartbeat_task_sleep_duration =
      config_mgr_->GetHeartBeatTaskSleepDuration() / absl::Seconds(1);
  // Give enough time just incase heartbeat task was sleeping when the
  // chunkserver aborted.
  std::this_thread::sleep_for(
      std::chrono::seconds(heartbeat_task_sleep_duration * 2));

  // Now lets check to make sure that the aborted chunkserver is no longer used
  // for allocations (i.e. unregistered by heartbeat task).
  allocated_locations = ChunkServerManager::GetInstance().AllocateChunkServer(
      /*chunk_handle=*/"TestHandle2",
      /*server_count=*/chunk_server_names.size());

  EXPECT_EQ(1, allocated_locations.size());

  auto allocated_server_address =
      absl::StrCat(config_mgr_->ResolveHostname(
                       (*allocated_locations.begin()).server_hostname()),
                   ":", (*allocated_locations.begin()).server_port());

  EXPECT_EQ(config_mgr_->GetServerAddress(chunk_server_names[1],
                                          /*resolve_hostname=*/true),
            allocated_server_address);

  // Shut down the second chunk server
  ShutdownServer(*chunk_server_servers[1]);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Start the MasterChunkServerManagerService in the background, and wait for
  // some time for the server to be successfully started in the background.
  auto master_server = StartTestMasterChunkServerManagerService();
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Run tests
  int exit_code = RUN_ALL_TESTS();

  // Clean up background server
  ShutdownServer(*master_server);

  return exit_code;
}
