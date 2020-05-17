#include <set>
#include <thread>

#include "gtest/gtest.h"
#include "src/server/master_server/metadata_manager.h"

using namespace gfs::server;

class MetadataManagerUnitTest : public ::testing::Test {
 protected:
  void SetUp() override { metadata_manager_ = MetadataManager::GetInstance(); }

  MetadataManager* metadata_manager_;
};

// Helper function to join a colletion of threads and cleanup the container for
// these threads
void JoinAndClearThreads(std::vector<std::thread>& threads) {
  for (auto& t : threads) {
    t.join();
  }
  threads.clear();
}

// Helper function to compute a filename given a base string and a nested 
// level. The name scheme takes the following form:
// "/{base}0/{base}1/..../{base}{nested_level-1}"
std::string ComputeNestedFileName(const std::string& base, 
                                  const int nested_level) {
  std::string filename;
  for (int i = 0; i < nested_level; i++) {
    filename += ("/" + base + std::to_string(i));
  }
  return filename;
}


// The simplest case that one creates a file /foo, and add a file chunk
TEST_F(MetadataManagerUnitTest, CreateSingleFileMetadata) {
  auto create_metadata(metadata_manager_->CreateFileMetadata("/foo"));
  EXPECT_TRUE(create_metadata.ok());

  EXPECT_TRUE(metadata_manager_->ExistFileMetadata("/foo"));
  auto foo_metadata_or(metadata_manager_->GetFileMetadata("/foo"));
  EXPECT_TRUE(foo_metadata_or.ok());
  auto foo_metadata(foo_metadata_or.ValueOrDie());
  EXPECT_EQ(foo_metadata->filename(), "/foo");

  auto first_chunk_handle_or(metadata_manager_->CreateChunkHandle("/foo", 0));
  EXPECT_TRUE(first_chunk_handle_or.ok());
  auto first_chunk_handle(first_chunk_handle_or.ValueOrDie());
  EXPECT_EQ(first_chunk_handle, "0");
  EXPECT_EQ(foo_metadata->chunk_handles_size(), (unsigned int)1);
}

// Create multiple files in parallel, the file names are /1, /2, /3...
// and file each file, create a chunkHandle at index 0
TEST_F(MetadataManagerUnitTest, CreateMultiFileMetadataInParallel) {
  auto numOfThreads(10);
  std::vector<std::thread> threads;
  for (int i = 0; i < numOfThreads; i++) {
    threads.push_back(std::thread([&, i]() {
      auto filename("/" + std::to_string(i));
      metadata_manager_->CreateFileMetadata(filename);
      metadata_manager_->CreateChunkHandle(filename, 0);
    }));
  }

  // Join all threads
  JoinAndClearThreads(threads);

  std::set<std::string> unique_id;
  for (int i = 0; i < numOfThreads; i++) {
    auto filename("/" + std::to_string(i));
    // Ensure that the files are created successfully
    EXPECT_TRUE(metadata_manager_->ExistFileMetadata(filename));
    auto file_metadata_or(metadata_manager_->GetFileMetadata(filename));
    EXPECT_TRUE(file_metadata_or.ok());
    auto file_metadata(file_metadata_or.ValueOrDie());
    EXPECT_EQ(file_metadata->filename(), filename);
    auto& chunk_handles(*file_metadata->mutable_chunk_handles());
    // Ensure that chunk handle exists for chunk_index 0 for each file
    EXPECT_EQ(chunk_handles.count(0), 1);
    unique_id.insert(chunk_handles.at(0));
  }

  // Ensure that there are {numOfThreads} number of unique chunk handles
  // assigned
  EXPECT_EQ(unique_id.size(), (unsigned int)numOfThreads);
}

// Multiple threads creating the same file, which is a contentious condition and
// only one of them succeeds. Furthermore, multiple threads adding different
// chunk index and creating new chunk handles in parallel.
TEST_F(MetadataManagerUnitTest, CreateSingleFileMultiChunksInParallel) {
  auto numOfThreads(10);
  std::vector<std::thread> threads;
  auto filename("/sameFile");
  std::atomic<int> cnts{0};

  // Create the same file concurrently
  for (int i = 0; i < numOfThreads; i++) {
    threads.push_back(std::thread([&, i]() {
      auto create_metadata(metadata_manager_->CreateFileMetadata(filename));
      if (create_metadata.ok()) {
        cnts++;
      }
    }));
  }

  // Join all threads and clear up
  JoinAndClearThreads(threads);

  // Only one of the create function succeeds because it is the same file
  EXPECT_EQ(cnts.load(), 1);

  // Create disjoint chunk handles concurrently
  for (int i = 0; i < numOfThreads; i++) {
    threads.push_back(std::thread(
        [&, i]() { metadata_manager_->CreateChunkHandle(filename, i); }));
  }

  // Join all threads
  JoinAndClearThreads(threads);

  auto file_metadata_or(metadata_manager_->GetFileMetadata(filename));
  EXPECT_TRUE(file_metadata_or.ok());
  auto file_metadata(file_metadata_or.ValueOrDie());
  EXPECT_EQ(file_metadata->filename(), filename);
  std::set<std::string> unique_id;
  EXPECT_EQ(file_metadata->chunk_handles_size(), numOfThreads);
  auto& chunk_handles(*file_metadata->mutable_chunk_handles());
  for (int i = 0; i < numOfThreads; i++) {
    unique_id.insert(chunk_handles.at(i));
  }

  // Ensure that all chunk handles created are unique
  EXPECT_EQ(unique_id.size(), numOfThreads);
}

// Multiple threads create file chunks but some of the indexes are 
// overlaping, so some of the CreateFileChunk calls fails
// Verify that the final state of filemetadata is legitimate
TEST_F(MetadataManagerUnitTest, ConcurrentChunkCreationOverlap) {
  int num_of_threads(24);
  int num_of_chunk_per_file(100);
  std::string filename("/ConcurrentChunkCreationOverlap");
  std::vector<std::thread> threads;
  
  // First create a file
  auto create_file_status(metadata_manager_->CreateFileMetadata(filename));
  EXPECT_TRUE(create_file_status.ok());
  
  // A counter to count how many CreateChunkHandle actually fail 
  // (as expected)
  std::atomic<int> cnt_fail{0};

  // Concurrent creation of chunks for the same file, so some of them 
  // must fail
  for (int i = 0; i < num_of_threads; i++) {
    threads.push_back(std::thread([&, i]() {
      for (int chunk_id = 0; chunk_id < num_of_chunk_per_file; chunk_id++) {
        auto create_chunk_handle_or(
                 metadata_manager_->CreateChunkHandle(filename, chunk_id));
        if (!create_chunk_handle_or.ok()) {
          EXPECT_EQ(create_chunk_handle_or.status().error_code(),
                    google::protobuf::util::error::ALREADY_EXISTS);
          cnt_fail++;
        }
      }
    }));
  }

  JoinAndClearThreads(threads);

  // There are in total num_of_threads * num_of_chunk_per_file chunk creation
  // attempt, but only for chunk 0,...,num_of_chunk_per_file-1 of one file
  // Therefore, only num_of_chunk_per_file attempts succeeded and the rest
  // would fail. 
  EXPECT_EQ(cnt_fail.load(), (num_of_threads-1) * num_of_chunk_per_file);
  
  // Verify that the the expected number of chunks are created for the file
  auto file_metadata_or(metadata_manager_->GetFileMetadata(filename));
  EXPECT_TRUE(file_metadata_or.ok());
  auto file_metadata(file_metadata_or.ValueOrDie());
  EXPECT_EQ(file_metadata->chunk_handles_size(), num_of_chunk_per_file);
}

// Check error messages for a few different cases
// 1) Create a file metadata and then create it again
// 2) Get a a non-existing file metadata
// 3) Create a chunk index for a non-existing file
// 4) Create a chunk index for a file twice
TEST_F(MetadataManagerUnitTest, CheckErrorCases) {
  auto new_filename("/newFile");
  auto create_metadata(metadata_manager_->CreateFileMetadata(new_filename));
  EXPECT_TRUE(create_metadata.ok());
  auto duplicate_create_metadata_or(
      metadata_manager_->CreateFileMetadata(new_filename));
  EXPECT_EQ(duplicate_create_metadata_or.error_code(),
            google::protobuf::util::error::ALREADY_EXISTS);

  auto non_exist_file("/nonExist");
  auto non_exist_metadata_or(
           metadata_manager_->GetFileMetadata(non_exist_file));
  EXPECT_EQ(non_exist_metadata_or.status().error_code(),
            google::protobuf::util::error::NOT_FOUND);

  auto non_exist_chunk_handle(
      metadata_manager_->CreateChunkHandle(non_exist_file, 0));
  EXPECT_EQ(non_exist_chunk_handle.status().error_code(),
            google::protobuf::util::error::NOT_FOUND);

  auto non_exist_file2("/newFile/foo");
  auto non_exist_chunk_handle2(
           metadata_manager_->CreateChunkHandle(non_exist_file2, 0));
  EXPECT_EQ(non_exist_chunk_handle2.status().error_code(),
            google::protobuf::util::error::NOT_FOUND);
}

// Stress test for contentious creation of file metadata. We spawn
// a number of threads, and each threads keeps trying to create
// file metadata for /l0/l1/.../l{threadId}. So if done only once some
// of these creation would fail because the parent directory may not
// exist yet, but keep trying they should eventually succeed. Verify 
// the file name at the end of this test
TEST_F(MetadataManagerUnitTest, ConcurrentFileCreationStressTest) {
  int num_of_threads(24);
  std::vector<std::thread> threads;
  std::string filename_base("l");

  for (int i = 0; i < num_of_threads; i++) {
    threads.push_back(std::thread([&, i]() {
      std::string filename(ComputeNestedFileName(filename_base,i+1));
      
      auto create_file_metadata_status(
               metadata_manager_->CreateFileMetadata(filename));
     
      // Retry if the above is not successful
      while(!create_file_metadata_status.ok()) {
        create_file_metadata_status = 
            metadata_manager_->CreateFileMetadata(filename);
      }
    }));
  }

  JoinAndClearThreads(threads);

  for (int i = 0; i < num_of_threads; i++) {
    std::string filename(ComputeNestedFileName(filename_base,i+1));
    // Construct the file name for each thread
    EXPECT_TRUE(metadata_manager_->ExistFileMetadata(filename)); 
  }
}

// Stress test for contentious creation of chunk handles. First we create
// A series of nested files, i.e. /n0/n1/.../n{num_of_thread-1}.  
// Each thread operates on a nested file, /n0/n1/.../n{thread_id},
// and create a {num_of_chunk_per_file} number of chunks concurrent.
// Verify that all assitned chunk handles are unique, and each file metadata
// contains an expected number of chunk handles
TEST_F(MetadataManagerUnitTest, ConcurrentFileChunkCreationStressTest) {
  int num_of_threads(24);
  int num_of_chunk_per_file(100);
  gfs::common::thread_safe_flat_hash_set<std::string> assigned_chunk_handles;
  std::vector<std::thread> threads;
  std::string filename_base("n");

  // Create nested files
  int nested_level(num_of_threads);
  for (int i = 0; i < nested_level; i++) {
    std::string filename(ComputeNestedFileName(filename_base, i+1));
    auto create_file_metadata_status(
             metadata_manager_->CreateFileMetadata(filename));
    EXPECT_TRUE(create_file_metadata_status.ok());
  }

  for (int i = 0; i < num_of_threads; i++) {
    threads.push_back(std::thread([&, i]() {
      std::string filename(ComputeNestedFileName(filename_base, i+1));
      for (int chunk_id = 0; chunk_id < num_of_chunk_per_file; chunk_id++) {
        auto create_chunk_handle_or(
                 metadata_manager_->CreateChunkHandle(filename, chunk_id));
        EXPECT_TRUE(create_chunk_handle_or.ok());
        std::string chunk_handle(create_chunk_handle_or.ValueOrDie());
        assigned_chunk_handles.insert(chunk_handle);
      }
    }));
  }

  JoinAndClearThreads(threads);

  // Verify that there are num_of_chunk_per_file * num_of_threads number of 
  // chunk handles assigned in this test
  EXPECT_EQ(assigned_chunk_handles.size(), 
            num_of_chunk_per_file * num_of_threads);

  // Verify that each nested file has expected number of chunk handles
  for (int i = 0; i < nested_level; i++) {
    std::string filename(ComputeNestedFileName(filename_base, i+1));
    
    auto file_metadata_or(metadata_manager_->GetFileMetadata(filename));
    EXPECT_TRUE(file_metadata_or.ok());
    auto file_metadata(file_metadata_or.ValueOrDie());
    EXPECT_EQ(file_metadata->chunk_handles_size(), num_of_chunk_per_file);
  }
}
