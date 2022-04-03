#include <chrono>
#include <thread>

#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "gtest/gtest.h"
#include "src/common/config_manager.h"
#include "src/server/chunk_server/chunk_server_impl.h"
#include "src/server/chunk_server/file_chunk_manager.h"
#include "src/server/master_server/chunk_server_manager.h"
#include "src/server/master_server/master_chunk_server_manager_service_impl.h"

using gfs::common::ConfigManager;
using gfs::server::ChunkServerImpl;
using gfs::server::ChunkServerManager;
using gfs::server::FileChunkManager;
using gfs::service::MasterChunkServerManagerServiceImpl;
using google::protobuf::util::StatusOr;
using grpc::Server;
using grpc::ServerBuilder;
using protos::grpc::ReportChunkServerReply;
using protos::grpc::ReportChunkServerRequest;

const std::string kTestConfigPath = "tests/common/test_config.yml";
const std::string kTestMasterServerName = "test_master";
const std::string kTestMasterServerAddress = "0.0.0.0:10002";
const std::string kTestChunkServerName = "test_chunk_server_01";

namespace {
void StartTestMasterChunkServerManagerService() {
  ServerBuilder builder;
  auto credentials = grpc::InsecureServerCredentials();
  builder.AddListeningPort(kTestMasterServerAddress, credentials);

  // Register a synchronous service for coordinating with chunkservers
  MasterChunkServerManagerServiceImpl chunk_server_mgr_service;
  builder.RegisterService(&chunk_server_mgr_service);

  // Start the server, and let it run until thread is cancelled
  std::unique_ptr<Server> server(builder.BuildAndStart());

  server->Wait();
}
}  // namespace

class MasterChunkServerManagerServiceTest : public ::testing::Test {
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
        /*chunk_database_name=*/"master_chunk_server_manager_service_test_db",
        /*max_chunk_size_bytes=*/100);
  }

  void SetUp() override {
    // Create the config manager for the test to read test config.
    config_mgr_ = std::shared_ptr<ConfigManager>(
        ConfigManager::GetConfig(kTestConfigPath).value());
  }

  // Executed after each test method in the suite.
  void TearDown() override {
    // Unregister all the ChunkServers registered by the test.
    ChunkServerManager::GetInstance().UnRegisterAllChunkServers();
  }

  std::shared_ptr<ConfigManager> config_mgr_;
};

// This tests a chunkserver that reports itself to master and master registers
// it and starts using it for chunk allocation.
TEST_F(MasterChunkServerManagerServiceTest, ReportChunkServerTest) {
  // Create the chunkserver
  StatusOr<ChunkServerImpl*> chunk_server_or =
      ChunkServerImpl::ConstructChunkServerImpl(kTestConfigPath,
                                                kTestChunkServerName);
  ChunkServerImpl* chunk_server = chunk_server_or.value();

  // Get the master server address
  const std::string master_server_address = config_mgr_->GetServerAddress(
      kTestMasterServerName, /*resolve_hostname=*/true);
  // Check that we got the right master address from config manager
  EXPECT_EQ(kTestMasterServerAddress, master_server_address);

  // Chunk server creates connection to the master server.
  chunk_server->RegisterMasterProtocolClient(master_server_address);

  // Check that the chunk server was able to report itself to the master server.
  EXPECT_TRUE(chunk_server->ReportToMaster());

  // The chunk server manager runs on the master server, but here since master
  // is just running as a seperate thread we can check if the report chunkserver
  // was truly successful and master can now select chunkserver for chunk
  // allocation.
  auto allocated_locations =
      ChunkServerManager::GetInstance().AllocateChunkServer(
          /*chunk_handle=*/"TestHandle", /*server_count=*/1);

  EXPECT_EQ(1, allocated_locations.size());

  // Now verify that the allocated chunk server is the chunk server we reported,
  // since its the only one reported.
  protos::ChunkServerLocation chunk_server_location;
  chunk_server_location.set_server_hostname(
      config_mgr_->GetServerHostname(kTestChunkServerName));
  chunk_server_location.set_server_port(
      config_mgr_->GetServerPort(kTestChunkServerName));

  EXPECT_EQ(chunk_server_location, *allocated_locations.begin());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Start the MasterChunkServerManagerService in the background, and wait for
  // some time for the server to be successfully started in the background.
  std::thread master_server_thread =
      std::thread(StartTestMasterChunkServerManagerService);
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Run tests
  int exit_code = RUN_ALL_TESTS();

  // Clean up background server
  pthread_cancel(master_server_thread.native_handle());
  master_server_thread.join();

  return exit_code;
}
