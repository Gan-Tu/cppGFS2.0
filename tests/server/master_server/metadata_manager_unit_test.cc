#include "gtest/gtest.h"
#include "src/server/master_server/metadata_manager.h"
#include <thread>
#include <set>

using namespace gfs::server;

class MetadataManagerUnitTest : public ::testing::Test {
   protected:
      void SetUp() override {
         metadataManager_ = MetadataManager::GetInstance();
      }

      MetadataManager* metadataManager_;
};

// Helper function to join a colletion of threads and cleanup the container for these
// threads
void joinAndClearThreads(std::vector<std::thread>& threads) {
   for(auto& t : threads) {
      t.join();
   }
   threads.clear();
}

// The simplest case that one creates a file /foo, and add a file chunk
TEST_F(MetadataManagerUnitTest, CreateSingleFileMetadata) {
   auto create_metadata(metadataManager_->CreateFileMetadata("/foo"));
   EXPECT_TRUE(create_metadata.ok());
   
   EXPECT_TRUE(metadataManager_->ExistFileMetadata("/foo"));
   auto foo_metadata_or(metadataManager_->GetFileMetadata("/foo"));
   EXPECT_TRUE(foo_metadata_or.ok());
   auto foo_metadata(foo_metadata_or.ValueOrDie());
   EXPECT_EQ(foo_metadata->filename(), "/foo");
   
   auto first_chunk_handle_or(metadataManager_->CreateChunkHandle("/foo", 0));
   EXPECT_TRUE(first_chunk_handle_or.ok());
   auto first_chunk_handle(first_chunk_handle_or.ValueOrDie());
   EXPECT_EQ(first_chunk_handle,"0");
   EXPECT_EQ(foo_metadata->chunk_handles_size(), (unsigned int)1);
}

// Create multiple files in parallel, the file names are /1, /2, /3...
// and file each file, create a chunkHandle at index 0
TEST_F(MetadataManagerUnitTest, CreateMultiFileMetadataInParallel) {
   auto numOfThreads(10);
   std::vector<std::thread> threads;
   for(int i=0; i<numOfThreads; i++) {
      threads.push_back(std::thread([&,i]() {
        auto file_name("/"+std::to_string(i));
        metadataManager_->CreateFileMetadata(file_name);
        metadataManager_->CreateChunkHandle(file_name, 0);
      }));
   }

  // Join all threads
  joinAndClearThreads(threads);

  std::set<std::string> uniqie_id;
  for(int i=0; i<numOfThreads; i++) {
     auto file_name("/"+std::to_string(i));
     // Ensure that the files are created successfully
     EXPECT_TRUE(metadataManager_->ExistFileMetadata(file_name));
     auto file_metadata_or(metadataManager_->GetFileMetadata(file_name));
     EXPECT_TRUE(file_metadata_or.ok());
     auto file_metadata(file_metadata_or.ValueOrDie());
     EXPECT_EQ(file_metadata->filename(), file_name);
     auto& chunk_handles(*file_metadata->mutable_chunk_handles());
     // Ensure that chunk handle exists for chunk_index 0 for each file
     EXPECT_EQ(chunk_handles.count(0), 1);
     uniqie_id.insert(chunk_handles.at(0));
  }

  // Ensure that there are {numOfThreads} number of unique chunk handles assigned
  EXPECT_EQ(uniqie_id.size(), (unsigned int)numOfThreads);
}

// Multiple threads creating the same file, which is a contentious condition and only
// one of them succeeds. Furthermore, multiple threads adding different chunk index
// and creating new chunk handles in parallel.
TEST_F(MetadataManagerUnitTest, CreateSingleFileMultiChunksInParallel) {
  auto numOfThreads(10);
  std::vector<std::thread> threads;
  auto file_name("/sameFile");
  std::atomic<int> cnts{0};

  // Create the same file concurrently
  for(int i=0; i<numOfThreads; i++) {
     threads.push_back(std::thread([&,i]() {
       auto create_metadata(metadataManager_->CreateFileMetadata(file_name));
       if(create_metadata.ok()) {
         cnts++;
       }
     }));
  }

  // Join all threads and clear up
  joinAndClearThreads(threads);

  // Only one of the create function succeeds because it is the same file
  EXPECT_EQ(cnts.load(), 1);

  // Create disjoint chunk handles concurrently
  for(int i=0; i<numOfThreads; i++) {
     threads.push_back(std::thread([&,i]() {
        metadataManager_->CreateChunkHandle(file_name, i);
     }));
  }

  // Join all threads
  joinAndClearThreads(threads);

  auto file_metadata_or(metadataManager_->GetFileMetadata(file_name));
  EXPECT_TRUE(file_metadata_or.ok());
  auto file_metadata(file_metadata_or.ValueOrDie());
  EXPECT_EQ(file_metadata->filename(), file_name);
  std::set<std::string> uniqie_id;
  EXPECT_EQ(file_metadata->chunk_handles_size(), numOfThreads);
  auto& chunk_handles(*file_metadata->mutable_chunk_handles());
  for(int i=0; i<numOfThreads; i++) {
     uniqie_id.insert(chunk_handles.at(i));
  }

  // Ensure that all chunk handles created are unique
  EXPECT_EQ(uniqie_id.size(), numOfThreads);
}

// Check error messages for a few different cases 
// 1) Create a file metadata and then create it again
// 2) Get a a non-existing file metadata
// 3) Create a chunk index for a non-existing file
// 4) Create a chunk index for a file twice 
TEST_F(MetadataManagerUnitTest, CheckErrorMessages) {
  auto new_file_name("/newFile");
  auto create_metadata(metadataManager_->CreateFileMetadata(new_file_name));
  EXPECT_TRUE(create_metadata.ok());
  auto duplicate_create_metadata_or(
          metadataManager_->CreateFileMetadata(new_file_name));
  // Note: ideally here we should say file metadata already exists. But before
  // creating a file metadata we always first create a lock. If the lock already
  // exsits we return early. Improvement could be done by handling the error returned
  // from the lockManager
  EXPECT_EQ(duplicate_create_metadata_or.error_message(),
            "Lock already exists for /newFile");

  auto non_exist_file("/nonExist");
  auto non_exist_metadata_or(metadataManager_->GetFileMetadata(non_exist_file));
  EXPECT_EQ(non_exist_metadata_or.status().error_message(),
            "File metadata does not exist: /nonExist");
 
  auto non_exist_chunk_handle(
         metadataManager_->CreateChunkHandle(non_exist_file,0));
  EXPECT_EQ(non_exist_chunk_handle.status().error_message(),
            "File metadata does not exist: /nonExist"); 

  auto non_exist_file2("/newFile/foo");
  auto non_exist_chunk_handle2(
         metadataManager_->CreateChunkHandle(non_exist_file2,0));
  EXPECT_EQ(non_exist_chunk_handle2.status().error_message(),
            "File metadata does not exist: /newFile/foo");
} 
