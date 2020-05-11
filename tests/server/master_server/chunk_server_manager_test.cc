#include "src/server/master_server/chunk_server_manager.h"

#include <memory>

#include "gtest/gtest.h"

using namespace gfs::server;

// The fixture for testing ChunkServerManager. Handles setup and cleanup for the
// test methods.
class ChunkServerManagerTest : public ::testing::Test {
 protected:
  // Executed after each test method in the suite.
  void TearDown() override {
    // Unregister all the ChunkServers registered by the test.
    ChunkServerManager::GetInstance().UnRegisterAllChunkServers();
  }
};

TEST_F(ChunkServerManagerTest, RegisterAndGetChunkServer) {
  protos::ChunkServerLocation* server_location =
      new protos::ChunkServerLocation();
  server_location->set_server_hostname("192.168.1.1");
  server_location->set_server_port(3030);

  std::shared_ptr<protos::ChunkServer> chunk_server(new protos::ChunkServer());
  chunk_server->set_allocated_location(server_location);
  EXPECT_TRUE(
      ChunkServerManager::GetInstance().RegisterChunkServer(chunk_server));

  EXPECT_EQ(*chunk_server, *ChunkServerManager::GetInstance().GetChunkServer(
                               *server_location));
}

TEST_F(ChunkServerManagerTest, AllocateChunkServer) {
  protos::ChunkServerLocation* small_server_location =
      new protos::ChunkServerLocation();
  small_server_location->set_server_hostname("192.168.1.1");
  small_server_location->set_server_port(3030);

  std::shared_ptr<protos::ChunkServer> small_chunk_server(
      new protos::ChunkServer());
  small_chunk_server->set_allocated_location(small_server_location);
  small_chunk_server->set_available_memory_mb(100);

  EXPECT_TRUE(ChunkServerManager::GetInstance().RegisterChunkServer(
      small_chunk_server));

  protos::ChunkServerLocation* big_server_location =
      new protos::ChunkServerLocation();
  big_server_location->set_server_hostname("192.168.100.1");
  big_server_location->set_server_port(6060);

  std::shared_ptr<protos::ChunkServer> big_chunk_server(
      new protos::ChunkServer());
  big_chunk_server->set_allocated_location(big_server_location);
  big_chunk_server->set_available_memory_mb(200);

  EXPECT_TRUE(
      ChunkServerManager::GetInstance().RegisterChunkServer(big_chunk_server));

  EXPECT_EQ(*big_chunk_server,
            *ChunkServerManager::GetInstance().AllocateChunkServer());
}
