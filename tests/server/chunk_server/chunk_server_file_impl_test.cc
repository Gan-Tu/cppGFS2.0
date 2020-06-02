#include <chrono>
#include <thread>

#include "absl/time/time.h"
#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "gtest/gtest.h"
#include "src/common/protocol_client/chunk_server_service_gfs_client.h"
#include "src/common/protocol_client/chunk_server_service_server_client.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"
#include "src/protos/grpc/chunk_server_lease_service.grpc.pb.h"
#include "src/server/chunk_server/chunk_server_file_service_impl.h"
#include "src/server/chunk_server/chunk_server_impl.h"
#include "src/server/chunk_server/chunk_server_lease_service_impl.h"
#include "src/server/chunk_server/file_chunk_manager.h"

using gfs::server::ChunkServerImpl;
using gfs::server::FileChunkManager;
using gfs::service::ChunkServerFileServiceImpl;
using gfs::service::ChunkServerLeaseServiceImpl;
using gfs::service::ChunkServerServiceChunkServerClient;
using gfs::service::ChunkServerServiceGfsClient;
using gfs::service::ChunkServerServiceMasterServerClient;
using google::protobuf::util::StatusOr;
using grpc::Server;
using grpc::ServerBuilder;
using protos::grpc::AdvanceFileChunkVersionReply;
using protos::grpc::AdvanceFileChunkVersionRequest;
using protos::grpc::InitFileChunkReply;
using protos::grpc::InitFileChunkRequest;
using protos::grpc::ReadFileChunkReply;
using protos::grpc::ReadFileChunkRequest;

const std::string kTestConfigPath = "tests/server/chunk_server/test_config.yml";
// Initialize this test on port 50052, so it doesn't conflict with lease test
// when running both using Bazel at the same time
const std::string kTestServerName = "chunk_server_03";
const std::string kTestServerAddress = "0.0.0.0:50054";

// Since we share the same test chunk server in the background, we let differnet
// unit tests issue requests to *different* file handles, so the mutations
// created by unit tests themselves don't interfere each other
//
// This is indeed not ideal, but does ther job for now
const std::string kTestFileHandle = "9d2a2342-97f9-11ea";
const std::string kTestFileHandle_Advanceable = "bb37-0242ac130002";
const uint32_t kTestFileVersion = 2;
const std::string kTestData = "Hello, World! This is a dope test";

namespace {
void SeedTestData(ChunkServerImpl* chunk_server) {
  // TODO(tugan): chunk_server is not used for now, but will be added in future
  // pull request for seeding lease service's test data; I leave it here for
  // ease of integration later

  // initial chunks
  FileChunkManager* test_file_manager = FileChunkManager::GetInstance();
  test_file_manager->CreateChunk(kTestFileHandle, kTestFileVersion);
  test_file_manager->WriteToChunk(kTestFileHandle, kTestFileVersion,
                                  /*start_offset=*/0, kTestData.length(),
                                  kTestData);
  test_file_manager->CreateChunk(kTestFileHandle_Advanceable, kTestFileVersion);
  test_file_manager->WriteToChunk(kTestFileHandle_Advanceable, kTestFileVersion,
                                  /*start_offset=*/0, kTestData.length(),
                                  kTestData);
}

void StartTestServer() {
  // Initialize the test chunk server database
  FileChunkManager::GetInstance()->Initialize("chunk_server_file_impl_test_db",
                                              /*max_chunk_size_bytes=*/1024);

  ServerBuilder builder;
  auto credentials = grpc::InsecureServerCredentials();
  builder.AddListeningPort(kTestServerAddress, credentials);
  StatusOr<ChunkServerImpl*> chunk_server_or =
      ChunkServerImpl::ConstructChunkServerImpl(kTestConfigPath,
                                                kTestServerName);
  ChunkServerImpl* chunk_server = chunk_server_or.ValueOrDie();
  SeedTestData(chunk_server);  // seed test data
  ChunkServerLeaseServiceImpl lease_service(chunk_server);
  builder.RegisterService(&lease_service);
  ChunkServerFileServiceImpl file_service(chunk_server);
  builder.RegisterService(&file_service);
  // Start the server, and let it run until thread is cancelled
  std::unique_ptr<Server> server(builder.BuildAndStart());
  server->Wait();
}
}  // namespace

class ChunkServerFileImplTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialize a test specific client
    master_server_client_ =
        std::make_shared<ChunkServerServiceMasterServerClient>(
            grpc::CreateChannel(kTestServerAddress,
                                grpc::InsecureChannelCredentials()));
    chunk_server_client_ =
        std::make_shared<ChunkServerServiceChunkServerClient>(
            grpc::CreateChannel(kTestServerAddress,
                                grpc::InsecureChannelCredentials()));
    gfs_client_ =
        std::make_shared<ChunkServerServiceGfsClient>(grpc::CreateChannel(
            kTestServerAddress, grpc::InsecureChannelCredentials()));
  }

  ReadFileChunkRequest MakeValidReadFileChunkRequest() {
    ReadFileChunkRequest req;
    req.set_chunk_handle(kTestFileHandle);
    req.set_chunk_version(kTestFileVersion);
    req.set_offset_start(0);
    req.set_length(kTestData.length());
    return req;
  }

  AdvanceFileChunkVersionRequest MakeValidAdvanceFileChunkVersionRequest() {
    AdvanceFileChunkVersionRequest req;
    req.set_chunk_handle(kTestFileHandle);
    req.set_new_chunk_version(kTestFileVersion + 1);
    return req;
  }

  std::shared_ptr<ChunkServerServiceMasterServerClient> master_server_client_;
  std::shared_ptr<ChunkServerServiceChunkServerClient> chunk_server_client_;
  std::shared_ptr<ChunkServerServiceGfsClient> gfs_client_;
};

//
// TEST - Initialize File Chunks
//

TEST_F(ChunkServerFileImplTest, InitNewFileChunk) {
  InitFileChunkRequest req;
  req.set_chunk_handle("init-new-chunk-test-handle");

  // The chunk handle doesn't exist yet, so it should succeed
  grpc::ClientContext client_context;
  auto reply_or = master_server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(), InitFileChunkReply::CREATED);
}

TEST_F(ChunkServerFileImplTest, InitExistingFileChunk) {
  InitFileChunkRequest req;
  req.set_chunk_handle(kTestFileHandle);

  // The chunk already exists, so re-initialize it should error
  grpc::ClientContext client_context;
  auto reply_or = master_server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(), InitFileChunkReply::ALREADY_EXISTS);
}

TEST_F(ChunkServerFileImplTest, ReadFileChunkOK) {
  ReadFileChunkRequest req = MakeValidReadFileChunkRequest();

  grpc::ClientContext client_context;
  auto reply_or = gfs_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(), ReadFileChunkReply::OK);
  EXPECT_EQ(reply_or.ValueOrDie().data(), kTestData);
  EXPECT_EQ(reply_or.ValueOrDie().bytes_read(), kTestData.length());
}

//
// TEST - Read File Chunks
//

TEST_F(ChunkServerFileImplTest, ReadFileChunkFullRead) {
  ReadFileChunkRequest req = MakeValidReadFileChunkRequest();
  const uint32_t read_length = kTestData.length() - 3;
  req.set_length(read_length);

  grpc::ClientContext client_context;
  auto reply_or = gfs_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(), ReadFileChunkReply::OK);
  EXPECT_EQ(reply_or.ValueOrDie().data(), kTestData.substr(0, read_length));
  EXPECT_EQ(reply_or.ValueOrDie().bytes_read(), read_length);
}

TEST_F(ChunkServerFileImplTest, ReadFileChunkPartialRead) {
  ReadFileChunkRequest req = MakeValidReadFileChunkRequest();
  req.set_length(kTestData.length() + 3);

  grpc::ClientContext client_context;
  auto reply_or = gfs_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(), ReadFileChunkReply::OK);

  // We will reach end of file before we read the requested number of bytes, so
  // this should be a partial read
  EXPECT_EQ(reply_or.ValueOrDie().data(), kTestData);
  EXPECT_EQ(reply_or.ValueOrDie().bytes_read(), kTestData.length());
}

TEST_F(ChunkServerFileImplTest, ReadFileChunkFullReadWithOffset) {
  ReadFileChunkRequest req = MakeValidReadFileChunkRequest();
  const uint32_t offset_start = 5;
  const uint32_t read_length = kTestData.length() - offset_start;
  req.set_offset_start(offset_start);
  req.set_length(read_length);

  grpc::ClientContext client_context;
  auto reply_or = gfs_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(), ReadFileChunkReply::OK);
  EXPECT_EQ(reply_or.ValueOrDie().data(),
            kTestData.substr(offset_start, offset_start + read_length));
  EXPECT_EQ(reply_or.ValueOrDie().bytes_read(), read_length);
}

TEST_F(ChunkServerFileImplTest, ReadFileChunkPartialReadWithOffset) {
  ReadFileChunkRequest req = MakeValidReadFileChunkRequest();
  const uint32_t offset_start = 5;
  req.set_offset_start(offset_start);
  req.set_length(kTestData.length());

  grpc::ClientContext client_context;
  auto reply_or = gfs_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(), ReadFileChunkReply::OK);
  EXPECT_EQ(reply_or.ValueOrDie().data(), kTestData.substr(offset_start));
  EXPECT_EQ(reply_or.ValueOrDie().bytes_read(),
            kTestData.length() - offset_start);
}

TEST_F(ChunkServerFileImplTest, ReadFileChunkNoFileChunk) {
  ReadFileChunkRequest req = MakeValidReadFileChunkRequest();
  req.set_chunk_handle("no-such-handle");

  grpc::ClientContext client_context;
  auto reply_or = gfs_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(),
            ReadFileChunkReply::FAILED_NOT_FOUND);
}

TEST_F(ChunkServerFileImplTest, ReadFileChunkNoChunkVersion) {
  ReadFileChunkRequest req = MakeValidReadFileChunkRequest();

  req.set_chunk_version(kTestFileVersion + 1);
  grpc::ClientContext client_context;
  StatusOr<ReadFileChunkReply> reply_or =
      gfs_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(),
            ReadFileChunkReply::FAILED_VERSION_OUT_OF_SYNC);

  req.set_chunk_version(kTestFileVersion - 1);
  grpc::ClientContext client_context2;
  reply_or = gfs_client_->SendRequest(req, client_context2);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(),
            ReadFileChunkReply::FAILED_VERSION_OUT_OF_SYNC);
}

TEST_F(ChunkServerFileImplTest, ReadFileChunkOKOutOfRange) {
  ReadFileChunkRequest req = MakeValidReadFileChunkRequest();
  req.set_offset_start(kTestData.length() + 2);

  grpc::ClientContext client_context;
  auto reply_or = gfs_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(),
            ReadFileChunkReply::FAILED_OUT_OF_RANGE);
}

//
// TEST - Advance File Chunk Versions
//

TEST_F(ChunkServerFileImplTest, AdvanceFileChunkVersionOK) {
  AdvanceFileChunkVersionRequest req =
      MakeValidAdvanceFileChunkVersionRequest();
  // this test should succeed, so we manipulate a different file handle to
  // not interfere other advance tests
  req.set_chunk_handle(kTestFileHandle_Advanceable);

  grpc::ClientContext client_context;
  auto reply_or = master_server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(), AdvanceFileChunkVersionReply::OK);
  EXPECT_EQ(reply_or.ValueOrDie().chunk_version(), req.new_chunk_version());
}

TEST_F(ChunkServerFileImplTest, AdvanceFileChunkVersionNoFileChunk) {
  AdvanceFileChunkVersionRequest req =
      MakeValidAdvanceFileChunkVersionRequest();
  req.set_chunk_handle("no-such-handle");

  grpc::ClientContext client_context;
  auto reply_or = master_server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(),
            AdvanceFileChunkVersionReply::FAILED_NOT_FOUND);
}

TEST_F(ChunkServerFileImplTest, AdvanceFileChunkVersionOutOfSyncVersionError) {
  AdvanceFileChunkVersionRequest req =
      MakeValidAdvanceFileChunkVersionRequest();

  req.set_new_chunk_version(kTestFileVersion + 2);
  grpc::ClientContext client_context;
  StatusOr<AdvanceFileChunkVersionReply> reply_or =
      master_server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(),
            AdvanceFileChunkVersionReply::FAILED_VERSION_OUT_OF_SYNC);

  req.set_new_chunk_version(kTestFileVersion);
  grpc::ClientContext client_context2;
  reply_or = master_server_client_->SendRequest(req, client_context2);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(),
            AdvanceFileChunkVersionReply::FAILED_VERSION_OUT_OF_SYNC);

  req.set_new_chunk_version(kTestFileVersion - 1);
  grpc::ClientContext client_context3;
  reply_or = master_server_client_->SendRequest(req, client_context3);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(),
            AdvanceFileChunkVersionReply::FAILED_VERSION_OUT_OF_SYNC);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Start a server for lease gRPC handler in the background, and wait some time
  // for the server to successfully start in background
  std::thread server_thread = std::thread(StartTestServer);
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Run tests
  int exit_code = RUN_ALL_TESTS();

  // Clean up background server
  pthread_cancel(server_thread.native_handle());
  server_thread.join();

  return exit_code;
}