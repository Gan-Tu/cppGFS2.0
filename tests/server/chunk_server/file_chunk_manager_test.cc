#include "src/server/chunk_server/file_chunk_manager.h"

#include <future>
#include <iostream>  //remove??
#include <memory>
#include <thread>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"

using namespace gfs::server;

// The fixture for testing ChunkServerManager. Handles setup and cleanup for the
// test methods.
class FileChunkManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    file_chunk_mgr = FileChunkManager::GetInstance();
    file_chunk_mgr->Initialize(
        /*chunk_database_name=*/"/tmp/file_chunk_mgr_test_db",
        /*max_chunk_size_bytes=*/100);
  }

  FileChunkManager* file_chunk_mgr;
};

// Test registering a chunk server with no chunks. Happens when a new
// chunkserver is being registered.
TEST_F(FileChunkManagerTest, BasicCrudOperationTest) {
  std::string data = "Testing the filechunkmanager.";
  std::string handle = "BasicCrudOperationTest";

  uint32_t version = 1;
  EXPECT_TRUE(file_chunk_mgr->CreateChunk(handle, version).ok());

  // bump version before write
  EXPECT_TRUE(
      file_chunk_mgr->UpdateChunkVersion(handle, version, ++version).ok());

  auto write_result =
      file_chunk_mgr->WriteToChunk(handle, version, 0, data.size(), data);

  EXPECT_TRUE(write_result.ok());

  EXPECT_EQ(data.size(), write_result.ValueOrDie());

  const uint32_t read_start_offset = 5;
  const uint32_t read_length = 7;
  auto read_result = file_chunk_mgr->ReadFromChunk(
      handle, version, read_start_offset, read_length);

  EXPECT_TRUE(read_result.ok());

  EXPECT_EQ(data.substr(read_start_offset, read_length),
            read_result.ValueOrDie());

  EXPECT_TRUE(
      file_chunk_mgr->UpdateChunkVersion(handle, version, ++version).ok());

  const uint32_t update_offset = 10;
  const std::string update_data = " Updating the previous write.";
  auto update_result = file_chunk_mgr->WriteToChunk(
      handle, version, update_offset, update_data.size(), update_data);

  EXPECT_TRUE(update_result.ok());

  EXPECT_EQ(update_data.size(), update_result.ValueOrDie());

  auto update_read_result = file_chunk_mgr->ReadFromChunk(
      handle, version, update_offset, update_data.size());

  EXPECT_TRUE(update_read_result.ok());

  EXPECT_EQ(update_data, update_read_result.ValueOrDie());

  EXPECT_TRUE(file_chunk_mgr->DeleteChunk(handle).ok());

  EXPECT_FALSE(
      file_chunk_mgr
          ->ReadFromChunk(handle, version, update_offset, update_data.size())
          .ok());
}

// Test registering a chunk server with no chunks. Happens when a new
// chunkserver is being registered.
TEST_F(FileChunkManagerTest, GetAllFileChunkMetadata) {
  const std::string data = "Testing the filechunkmanager.";
  const std::string handle_prefix = "GetAllFileChunkMetadata";
  const uint32_t chunk_count = 10;

  absl::flat_hash_set<std::string> chunk_handles;

  for (uint32_t chunk_id = 0; chunk_id < chunk_count; ++chunk_id) {
    uint32_t version = 1;
    auto handle = handle_prefix + std::to_string(chunk_id);
    EXPECT_TRUE(file_chunk_mgr->CreateChunk(handle, version).ok());

    EXPECT_TRUE(
        file_chunk_mgr->UpdateChunkVersion(handle, version, ++version).ok());

    auto write_result =
        file_chunk_mgr->WriteToChunk(handle, version, 0, data.size(), data);

    EXPECT_TRUE(write_result.ok());

    EXPECT_EQ(data.size(), write_result.ValueOrDie());

    chunk_handles.insert(handle);
  }

  auto all_chunks = file_chunk_mgr->GetAllFileChunkMetadata();

  EXPECT_EQ(chunk_count, all_chunks.size());

  for (auto iterator = all_chunks.begin(); iterator != all_chunks.end();
       ++iterator) {
    // Remove to show that we've seen this handle.
    // If we removed nothing, means that we didn't create the handle or
    // handle was stored more than once in file chunk mgr.
    EXPECT_TRUE(chunk_handles.erase(iterator->chunk_handle()));
  }
}
