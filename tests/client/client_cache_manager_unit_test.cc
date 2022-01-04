#include "gtest/gtest.h"

#include "absl/time/time.h"
#include "src/client/client_cache_manager.h"
#include "tests/utils.h"

using namespace gfs::client;
using namespace tests;

class ClientCacheManagerUnitTest : public ::testing::Test {
};
// Note that here although Client's CacheManager is supposed to be a singleton,
// i.e. one per process. For testing purpose, we instantiate a new manager
// for each sub-test case. So there is no interference from one another

// Verify the behavior of Setting and Getting the chunk handle from filename
// and chunk index
TEST_F(ClientCacheManagerUnitTest, ChunkHandleMappingTest) {
  CacheManager* manager(CacheManager::ConstructCacheManager(absl::Hours(1)));
  std::string filename("/foo");
 
  // Assign "0",..."9" to ("/foo", 0,...9)
  int num_of_chunk(10);
  for (int i = 0; i < num_of_chunk; i++) {
    auto set_status(manager->SetChunkHandle(filename, 
                                 i, std::to_string(i)));
    EXPECT_TRUE(set_status.ok());
  }

  // Ensure that all chunk_handle can be found
  for (int i = 0; i < num_of_chunk; i++) {
    auto get_status(manager->GetChunkHandle(filename, i));
    EXPECT_TRUE(get_status.ok());
    EXPECT_EQ(get_status.ValueOrDie(), std::to_string(i)); 
  }

  // Try to set an existing mapping to a different chunk handle, this 
  // should result in an error
  auto set_status(manager->SetChunkHandle(filename, 0, "1"));
  EXPECT_FALSE(set_status.ok());
  EXPECT_EQ(set_status.error_code(),
            google::protobuf::util::kInvalidArgument);

  // Try to get a chunk from a non-existing filename
  auto get_status(manager->GetChunkHandle("/nonExist", 0));
  EXPECT_FALSE(get_status.ok());
  EXPECT_EQ(get_status.status().error_code(),
            google::protobuf::util::kNotFound);

  // Try to get a chunk index that is non-existing in a file
  get_status = manager->GetChunkHandle(filename, num_of_chunk);
  EXPECT_FALSE(get_status.ok());
  EXPECT_EQ(get_status.status().error_code(),
            google::protobuf::util::kNotFound);

  delete manager;
}

// Verify the behavior of setting and getting chunk version
TEST_F(ClientCacheManagerUnitTest, ChunkVersionTest) {
  CacheManager* manager(CacheManager::ConstructCacheManager(absl::Hours(1)));
  // Before adding any chunk_handle, try set version number for one, this
  // should lead to an error
  std::string chunk_handle("1");

  auto set_status(manager->SetChunkVersion(chunk_handle, 0));
  EXPECT_FALSE(set_status.ok());
  EXPECT_EQ(set_status.error_code(),
            google::protobuf::util::kInvalidArgument);
  auto get_status(manager->GetChunkVersion(chunk_handle));
  EXPECT_FALSE(get_status.ok());
  EXPECT_EQ(get_status.status().error_code(),
            google::protobuf::util::kNotFound);

  // Now we assign a valid chunk_handle, the set version action should suceed
  std::string filename("/bar");
  manager->SetChunkHandle(filename, 10, chunk_handle);
  set_status = manager->SetChunkVersion(chunk_handle, 0);
  EXPECT_TRUE(set_status.ok());
  get_status = manager->GetChunkVersion(chunk_handle);
  EXPECT_TRUE(get_status.ok());
  EXPECT_EQ(get_status.ValueOrDie(), 0);

  manager->SetChunkVersion(chunk_handle, 100);
  EXPECT_EQ(manager->GetChunkVersion(chunk_handle).ValueOrDie(), 100);

  delete manager;
}

// Verify the behavior of setting and updating chunk server location
TEST_F(ClientCacheManagerUnitTest, ChunkServerLocationTest) {
  // Configure a cache manager with time out = 2s
  CacheManager* manager(CacheManager::ConstructCacheManager(absl::Seconds(2)));
  int num_of_chunk(10);
  std::string filename("/baz");

  // Generate a number of chunks first
  for (int i = 0; i < num_of_chunk; i++) {
    std::string chunk_handle(std::to_string(i));
    auto set_chunk_status(manager->SetChunkHandle(filename,
                                       i, chunk_handle));
    EXPECT_TRUE(set_chunk_status.ok());

    CacheManager::ChunkServerLocationEntry chunk_server_location_entry;
    chunk_server_location_entry.primary_location = 
        ChunkServerLocationBuilder("localhost", 5000 + i);
    chunk_server_location_entry.locations.push_back(
        ChunkServerLocationBuilder("localhost", 5000 + i));
    chunk_server_location_entry.locations.push_back(
        ChunkServerLocationBuilder("localhost", 5000 + i + 1));
    chunk_server_location_entry.locations.push_back(
        ChunkServerLocationBuilder("localhost", 5000 + i + 2));
    manager->SetChunkServerLocation(chunk_handle, chunk_server_location_entry);
  }

  // Verify immediately that all cache entries are there and are valid
  for (int i = 0; i < num_of_chunk; i++) {
    std::string chunk_handle(std::to_string(i));
    auto get_chunk_location_or(
             manager->GetChunkServerLocation(chunk_handle));
    EXPECT_TRUE(get_chunk_location_or.ok()); 
 
    CacheManager::ChunkServerLocationEntry cache_entry(
        get_chunk_location_or.ValueOrDie());
    EXPECT_EQ(cache_entry.primary_location.server_port(), 5000 + i);
    EXPECT_EQ(cache_entry.locations.size(), 3);
  }

  // Sleep for 3 seconds, let all caches expire so we expect timeout error
  // when accessing
  sleep(3.0);

  for (int i = 0; i < num_of_chunk; i++) {
    std::string chunk_handle(std::to_string(i));
    auto get_chunk_location_or(
             manager->GetChunkServerLocation(chunk_handle));
    EXPECT_FALSE(get_chunk_location_or.ok());
    EXPECT_EQ(get_chunk_location_or.status().error_code(),
              google::protobuf::util::kDeadlineExceeded);
  }

  // Re-insert an entry again and verify that we can still access it
  CacheManager::ChunkServerLocationEntry chunk_server_location_entry;
  chunk_server_location_entry.primary_location = 
      ChunkServerLocationBuilder("localhost", 5002);
  chunk_server_location_entry.locations.push_back(
      ChunkServerLocationBuilder("localhost", 5000));
  chunk_server_location_entry.locations.push_back(
      ChunkServerLocationBuilder("localhost", 5001));
  chunk_server_location_entry.locations.push_back(
      ChunkServerLocationBuilder("localhost", 5002));
  
  manager->SetChunkServerLocation("0", chunk_server_location_entry);

  auto get_chunk_location_or(manager->GetChunkServerLocation("0"));
  EXPECT_TRUE(get_chunk_location_or.ok()); 
  auto cache_entry = get_chunk_location_or.ValueOrDie();
  EXPECT_EQ(cache_entry.primary_location.server_port(), 5002);
  EXPECT_EQ(cache_entry.locations.size(), 3);
  EXPECT_EQ(cache_entry.locations[0].server_port(), 5000);
  EXPECT_EQ(cache_entry.locations[1].server_port(), 5001);
  EXPECT_EQ(cache_entry.locations[2].server_port(), 5002);

  delete manager;
}
