#include "chunk_server_manager.h"

#include <memory>

#include "gtest/gtest.h"

namespace gfs {

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
  ChunkServerKey server_key("192.168.1.1" /*host_name*/, "3030" /*port*/);
  std::shared_ptr<ChunkServer> chunk_server(new ChunkServer(server_key));

  ASSERT_TRUE(
      ChunkServerManager::GetInstance().RegisterChunkServer(chunk_server));

  ASSERT_EQ(*chunk_server,
            *ChunkServerManager::GetInstance().GetChunkServer(server_key));
}

TEST_F(ChunkServerManagerTest, GetAvailableChunkServer) {
  ChunkServerKey small_server_key("192.168.1.1" /*host_name*/, "3030" /*port*/);
  std::shared_ptr<ChunkServer> small_chunk_server(
      new ChunkServer(small_server_key));
  small_chunk_server->SetAvailableMemoryMb(100);

  ASSERT_TRUE(ChunkServerManager::GetInstance().RegisterChunkServer(
      small_chunk_server));

  ChunkServerKey big_server_key("192.168.100.1" /*host_name*/, "6060" /*port*/);
  std::shared_ptr<ChunkServer> big_chunk_server(
      new ChunkServer(big_server_key));
  big_chunk_server->SetAvailableMemoryMb(200);

  ASSERT_TRUE(
      ChunkServerManager::GetInstance().RegisterChunkServer(big_chunk_server));

  ASSERT_EQ(*big_chunk_server,
            *ChunkServerManager::GetInstance().GetAvailableChunkServer());
}
}  // namespace gfs
