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
   EXPECT_EQ(metadataManager_->CreateFileMetadata("/foo"), true);
   EXPECT_EQ(metadataManager_->ExistFileMetadata("/foo"), true);
   auto fooData(metadataManager_->GetFileMetadata("/foo"));
   EXPECT_EQ(fooData->filename(), "/foo");
   auto firstChunkHandle(metadataManager_->CreateChunkHandle("/foo", 0));
   EXPECT_EQ(firstChunkHandle,"0");
   EXPECT_EQ(fooData->chunk_handles_size(), (unsigned int)1);
}

// Create multiple files in parallel, the file names are /1, /2, /3...
// and file each file, create a chunkHandle at index 0
TEST_F(MetadataManagerUnitTest, CreateMultiFileMetadataInParallel) {
   auto numOfThreads(10);
   std::vector<std::thread> threads;
   for(int i=0; i<numOfThreads; i++) {
      threads.push_back(std::thread([&,i]() {
        auto fileName("/"+std::to_string(i));
        metadataManager_->CreateFileMetadata(fileName);
        metadataManager_->CreateChunkHandle(fileName, 0);
      }));
   }

  // Join all threads
  joinAndClearThreads(threads);

  std::set<std::string> uniqueIds;
  for(int i=0; i<numOfThreads; i++) {
     auto fileName("/"+std::to_string(i));
     // Ensure that the files are created successfully
     EXPECT_EQ(metadataManager_->ExistFileMetadata(fileName),true);
     auto fData(metadataManager_->GetFileMetadata(fileName));
     EXPECT_EQ(fData->filename(), fileName);
     auto& chunk_handles(*fData->mutable_chunk_handles());
     // Ensure that chunk handle exists for chunk_index 0 for each file
     EXPECT_EQ(chunk_handles.count(0), 1);
     uniqueIds.insert(chunk_handles.at(0));
  }

  // Ensure that there are {numOfThreads} number of unique chunk handles assigned
  EXPECT_EQ(uniqueIds.size(), (unsigned int)numOfThreads);
}

// Multiple threads creating the same file, which is a contentious condition and only
// one of them succeeds. Furthermore, multiple threads adding different chunk index
// and creating new chunk handles in parallel.
TEST_F(MetadataManagerUnitTest, CreateSingleFileMultiChunksInParallel) {
  auto numOfThreads(10);
  std::vector<std::thread> threads;
  auto fileName("/sameFile");
  std::atomic<int> cnts{0};

  // Create the same file concurrently
  for(int i=0; i<numOfThreads; i++) {
     threads.push_back(std::thread([&,i]() {
       auto succ(metadataManager_->CreateFileMetadata(fileName));
       if(succ) {
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
        metadataManager_->CreateChunkHandle(fileName, i);
     }));
  }

  // Join all threads
  joinAndClearThreads(threads);

  auto fData(metadataManager_->GetFileMetadata(fileName));
  EXPECT_EQ(fData->filename(), fileName);
  std::set<std::string> uniqueIds;
  EXPECT_EQ(fData->chunk_handles_size(), numOfThreads);
  auto& chunk_handles(*fData->mutable_chunk_handles());
  for(int i=0; i<numOfThreads; i++) {
     uniqueIds.insert(chunk_handles.at(i));
  }

  // Ensure that all chunk handles created are unique
  EXPECT_EQ(uniqueIds.size(), numOfThreads);
}
