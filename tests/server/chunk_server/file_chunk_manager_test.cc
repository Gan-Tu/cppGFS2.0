#include "src/server/chunk_server/file_chunk_manager.h"

#include <memory>

#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"

using namespace gfs::server;

// The fixture for testing FileChunkManager. Handles setup and cleanup for the
// test methods.
class FileChunkManagerTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    // The DB can only be initialized and open once by each thread, so we put
    // it in SetUpTestSuite for one-time setup
    FileChunkManager* file_chunk_mgr = FileChunkManager::GetInstance();
    file_chunk_mgr->Initialize(
        // Do NOT use /tmp as the test artifacts will persist this way
        // Instead, use relative directory for Bazel test so it will be cleaned
        // up after the test finishes
        /*chunk_database_name=*/"file_chunk_mgr_test_db",
        /*max_chunk_size_bytes=*/100);
  }

  void SetUp() override { file_chunk_mgr = FileChunkManager::GetInstance(); }

  FileChunkManager* file_chunk_mgr;
};

// Test basic create, read, update and delete operations with the file chunk
// manager.
TEST_F(FileChunkManagerTest, BasicCrudOperationTest) {
  std::string data = "Testing the filechunkmanager.";
  const std::string handle = "BasicCrudOperationTest";

  // create the v1 of the chunk
  uint32_t version = 1;
  EXPECT_TRUE(file_chunk_mgr->CreateChunk(handle, version).ok());

  // bump version before write
  EXPECT_TRUE(
      file_chunk_mgr->UpdateChunkVersion(handle, version, version+1).ok());
  version++;

  // write new data to chunk
  auto write_result = file_chunk_mgr->WriteToChunk(
      handle, version, /*start_offset=*/0, data.size(), data);

  // verify that all the data was written.
  EXPECT_EQ(data.size(), write_result.ValueOrDie());

  // Read the data
  const uint32_t read_start_offset = 5;
  const uint32_t read_length = 7;
  auto read_result = file_chunk_mgr->ReadFromChunk(
      handle, version, read_start_offset, read_length);

  EXPECT_EQ(data.substr(read_start_offset, read_length),
            read_result.ValueOrDie());

  // bump the chunk version
  EXPECT_TRUE(
      file_chunk_mgr->UpdateChunkVersion(handle, version, version+1).ok());
  version++;

  // make another write at a different offset
  const uint32_t update_offset = 10;
  const std::string update_data = " Updating the previous write.";
  auto update_result = file_chunk_mgr->WriteToChunk(
      handle, version, update_offset, update_data.size(), update_data);

  EXPECT_EQ(update_data.size(), update_result.ValueOrDie());

  // Read the last data we wrote
  auto update_read_result = file_chunk_mgr->ReadFromChunk(
      handle, version, update_offset, update_data.size());

  EXPECT_EQ(update_data, update_read_result.ValueOrDie());

  // Verify chunk version
  EXPECT_EQ(version, file_chunk_mgr->GetChunkVersion(handle).ValueOrDie());

  EXPECT_TRUE(file_chunk_mgr->DeleteChunk(handle).ok());

  // Can't read deleted chunk
  EXPECT_FALSE(
      file_chunk_mgr
          ->ReadFromChunk(handle, version, update_offset, update_data.size())
          .ok());
}

// Test that we can get all the stored chunks metadata (handle, version).
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
        file_chunk_mgr->UpdateChunkVersion(handle, version, version+1).ok());
    
    version++;

    auto write_result = file_chunk_mgr->WriteToChunk(
        handle, version, /*start_offset=*/0, data.size(), data);

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
