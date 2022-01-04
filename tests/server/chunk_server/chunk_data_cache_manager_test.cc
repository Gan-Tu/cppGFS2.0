#include "src/server/chunk_server/chunk_data_cache_manager.h"

#include <thread>
#include "gtest/gtest.h"
#include "tests/utils.h"

using namespace gfs::server;
using namespace tests;

class ChunkDataCacheManagerTest : public ::testing::Test {
 protected:  
  void SetUp() override {
    chunk_data_cache_mgr_ = ChunkDataCacheManager::GetInstance();
  }
  ChunkDataCacheManager* chunk_data_cache_mgr_;
};

TEST_F(ChunkDataCacheManagerTest, BasicTestCase) {
  std::string key_base_name("foo");
  std::string data_base("foo_data");

  // Set value for 100 items
  for (int i = 0; i < 100; i++) {
    chunk_data_cache_mgr_->SetValue(key_base_name + std::to_string(i),
                                    data_base + std::to_string(i));
  }

  // Check value for all
  for (int i = 0; i < 100; i++) {
    auto get_or(chunk_data_cache_mgr_->GetValue(
                    key_base_name + std::to_string(i)));
    EXPECT_TRUE(get_or.ok());
    EXPECT_EQ(get_or.ValueOrDie(), data_base + std::to_string(i));
  }
}

TEST_F(ChunkDataCacheManagerTest, ConcurrentInsertionAndDeletionTest) {
  auto num_of_threads(50);
  std::string key_base_name("bar");
  std::string data_base("bar_data");
  std::vector<std::thread> threads;

  // Concurrent insertion
  for (int i = 0; i < num_of_threads; i++) {
    threads.push_back(std::thread([&, i]() {
      std::string key_base_thread(key_base_name + std::to_string(i));
      std::string data_base_thread(data_base + std::to_string(i));
      for (int j = 0; j < 100; j++) {
        chunk_data_cache_mgr_->SetValue(key_base_thread + std::to_string(j),
                                        data_base_thread + std::to_string(j));
      }
    }));
  }

  JoinAndClearThreads(threads);

  // Concurrent access and checking
  for (int i = 0; i < num_of_threads; i++) {
    threads.push_back(std::thread([&, i]() {
      std::string key_base_thread(key_base_name + std::to_string(i));
      std::string data_base_thread(data_base + std::to_string(i));
      for (int j = 0; j < 100; j++) {
        auto get_or(chunk_data_cache_mgr_->GetValue(key_base_thread 
                                                        + std::to_string(j)));
        EXPECT_TRUE(get_or.ok());
        EXPECT_EQ(get_or.ValueOrDie(), data_base_thread + std::to_string(j));
      }
    }));
  }

  JoinAndClearThreads(threads);

  // Concurrent deletion and checking
  for (int i = 0; i < num_of_threads; i++) {
    threads.push_back(std::thread([&, i]() {
      std::string key_base_thread(key_base_name + std::to_string(i));
      std::string data_base_thread(data_base + std::to_string(i));
      for (int j = 0; j < 100; j++) {
        std::string key(key_base_thread + std::to_string(j));
        chunk_data_cache_mgr_->RemoveValue(key);
        auto get_or(chunk_data_cache_mgr_->GetValue(key));
        EXPECT_EQ(get_or.status().error_code(),
                  google::protobuf::util::kNotFound);
      }
    }));
  }

  JoinAndClearThreads(threads);
}

