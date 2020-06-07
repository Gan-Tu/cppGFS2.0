#include <chrono>
#include <thread>

#include "absl/time/time.h"
#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "gtest/gtest.h"
#include "src/client/gfs_client.h"
#include "src/common/config_manager.h"
#include "src/server/chunk_server/chunk_server_file_service_impl.h"
#include "src/server/chunk_server/chunk_server_impl.h"
#include "src/server/chunk_server/file_chunk_manager.h"
#include "src/server/master_server/master_metadata_service_impl.h"
#include "src/server/master_server/metadata_manager.h"
#include "tests/utils.h"

// Config info for a semi-mocked chunk server
const std::string kTestConfigPath = "tests/server/chunk_server/test_config.yml";
const std::string kTestChunkServerName = "chunk_server_02";
const uint32_t kTestChunkServerPort = 50053;
const std::string kTestChunkServerHostName = "0.0.0.0";
const std::string kTestChunkServerAddress 
    = kTestChunkServerHostName + ":" + std::to_string(kTestChunkServerPort);

// Config info for a semi-mocked master server
const uint32_t kTestMasterServerPort = 50051;
const std::string kTestMasterServerHostName = "0.0.0.0";
const std::string kTestMasterServerName = "master_server_01";
const std::string kTestMasterServerAddress  
    = kTestMasterServerHostName + ":" + std::to_string(kTestMasterServerPort);

// Sample data used for testing
const std::string kSmallDataFileName("/kSmallData");
const std::string kSmallData("Testing short data for read");
const std::string kSmallDataFileChunkHandle("0");
const uint32_t kSmallDataFileChunkVersion(2);

using gfs::common::ConfigManager;
using gfs::server::ChunkServerImpl;
using gfs::server::FileChunkManager;
using gfs::server::MetadataManager;
using gfs::service::MasterMetadataServiceImpl;
using gfs::service::ChunkServerFileServiceImpl;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using grpc::Server;
using grpc::ServerBuilder;

void SeedTestChunkData() {
  auto file_chunk_manager(FileChunkManager::GetInstance());
  file_chunk_manager->Initialize("file_service_test_db", 1000000);
  // Set up a short data for chunk_handle "0"
  const std::string chunk_handle(kSmallDataFileChunkHandle);
  const uint32_t version(kSmallDataFileChunkVersion);
  // Create a chunk with version 2
  ASSERT_TRUE(file_chunk_manager->CreateChunk(chunk_handle, version).ok());
  
  // Write to the chunk
  auto write_result = file_chunk_manager->WriteToChunk(
      chunk_handle, version, /*start_offset=*/0, kSmallData.size(), kSmallData);

  // verify that all the data was written.
  ASSERT_EQ(kSmallData.size(), write_result.ValueOrDie());
}

void SeedTestMetadata() {
  auto metadata_manager(MetadataManager::GetInstance());
  // Create a chunk handle for kSmallData, and this chunk_handle will be "0"
  ASSERT_TRUE(metadata_manager->CreateFileMetadata(kSmallDataFileName).ok());
  ASSERT_TRUE(metadata_manager->CreateChunkHandle(kSmallDataFileName, 0).ok());
  // Bump up the version to 2
  ASSERT_TRUE(metadata_manager->AdvanceChunkVersion(kSmallDataFileChunkHandle)
                  .ok());
  ASSERT_TRUE(metadata_manager->AdvanceChunkVersion(kSmallDataFileChunkHandle)
                  .ok());
  // Set the chunk server location to be the one used in this test
  auto file_chunk_metadata(metadata_manager->GetFileChunkMetadata(
                               kSmallDataFileChunkHandle).ValueOrDie());
  file_chunk_metadata.mutable_locations()->Add(
      tests::ChunkServerLocationBuilder(kTestChunkServerName,
                                        kTestChunkServerPort));
  metadata_manager->SetFileChunkMetadata(file_chunk_metadata);
}

// Start a test chunk server, this gets called from a separate thread
void StartTestChunkServer() {
  ServerBuilder builder;
  auto credentials = grpc::InsecureServerCredentials();
  builder.AddListeningPort(kTestChunkServerAddress, credentials);
  // Register the chunk server file service impl
  StatusOr<ChunkServerImpl*> chunk_server_or =
      ChunkServerImpl::ConstructChunkServerImpl(kTestConfigPath,
                                                kTestChunkServerName);
  ChunkServerImpl* chunk_server = chunk_server_or.ValueOrDie();
  ChunkServerFileServiceImpl file_service(chunk_server);
  builder.RegisterService(&file_service);
  // Initialize test data on the chunk server
  SeedTestChunkData(); 
  // Start the server, and let it run until thread is cancelled
  std::unique_ptr<Server> server(builder.BuildAndStart());
  server->Wait();
}

// Start a test master server, this gets called from a separate thread
void StartTestMasterServer() {
  ServerBuilder builder;
  auto credentials = grpc::InsecureServerCredentials();
  builder.AddListeningPort(kTestMasterServerAddress, credentials);
  // Register the master metadata service impl
  ConfigManager* config = ConfigManager::GetConfig(kTestConfigPath)
                              .ValueOrDie();
  MasterMetadataServiceImpl metadata_service(config);
  builder.RegisterService(&metadata_service);
  // Initialize test data on the master server
  SeedTestMetadata();
  // Start the server, and let it run until thread is cancelled
  std::unique_ptr<Server> server(builder.BuildAndStart());
  server->Wait();
}

// Initialize the client impl and make it ready to call client function
void StartClient() {
  Status init_status(gfs::client::init_client(kTestConfigPath, 
                                              "master_server_01", true));
  EXPECT_TRUE(init_status.ok());
}

// Helper funtion to perform a read request of a piece of data. It performs a
// read request for the filename {data_filename}, at {offset} for {length}
// of data.       
void SingleClientReadData(const std::string& data_filename,
                          const std::string& data, size_t offset, 
                          size_t length) {
  size_t actual_read_bytes;
  // Compute the actual bytes that we will read here. The reason of adding 
  // an explicit check below is because data.size() and offset are unsigned int
  // and directly minus may result in overflow and incorrect result.
  // One could use the following expression for short (less reasable though):
  //   actual_read_bytes = offset >= data.size() ? 0 : 
  //                           std::min(data.size() - offset, length);
  if (offset >= data.size()) {
    actual_read_bytes = 0;
  } else {
    actual_read_bytes = std::min(data.size() - offset, length);
  }

  auto read_result(gfs::client::read(data_filename.c_str(), offset, length));
  EXPECT_TRUE(read_result.ok()) << "Failed to read: " << read_result.status();
  // Make sure that the read data is expected
  auto read_data(read_result.ValueOrDie());
  EXPECT_EQ(read_data.bytes_read, actual_read_bytes);
  EXPECT_EQ(memcmp(read_data.buffer, data.c_str() + offset, 
                   read_data.bytes_read), 0);
  // Cleanup to prevent memory leak
  free(read_data.buffer);
}

void SingleClientReadShortData() {
  size_t data_len(kSmallData.size());
  // Read full length
  SingleClientReadData(kSmallDataFileName, kSmallData, 0, data_len);
  // Read second half length
  SingleClientReadData(kSmallDataFileName, kSmallData, data_len / 2, 
                       data_len - data_len / 2);  
  // Start from last quarter but try to read full length
  SingleClientReadData(kSmallDataFileName, kSmallData, data_len - data_len / 4,
                       data_len);
}

// TODO(Gan): refactor / rewrite this test for more cases 
class ChunkServerFileServiceTest : public ::testing::Test {
};

// Test a single client makes a read request of small data
TEST_F(ChunkServerFileServiceTest, SimpleClientReadShortDataTest) {
  StartClient();
  SingleClientReadShortData();
}

// Test multiple clients conrrently make read request of small data
TEST_F(ChunkServerFileServiceTest, ConcurrentClientReadShortDataTest) {
  auto num_of_threads(20);
  std::vector<std::thread> threads;
  for (int i = 0; i < num_of_threads; i++) {
    threads.push_back(std::thread([]() {
        StartClient();
        SingleClientReadShortData();
    }));
  }

  tests::JoinAndClearThreads(threads);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Start a chunk server for file service handler in the background, 
  std::thread chunk_server_thread = std::thread(StartTestChunkServer);
  
  // Start a master server file metadata service handler in the background
  std::thread master_server_thread = std::thread(StartTestMasterServer);

  // Wait some time for the server to successfully start in background
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Run tests
  int exit_code = RUN_ALL_TESTS();

  // Clean up background server
  pthread_cancel(chunk_server_thread.native_handle());
  pthread_cancel(master_server_thread.native_handle());

  chunk_server_thread.join();
  master_server_thread.join();

  return exit_code;
}
