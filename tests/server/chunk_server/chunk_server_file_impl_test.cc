#include <chrono>
#include <thread>

#include "absl/time/time.h"
#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "gtest/gtest.h"
#include "src/common/config_manager.h"
#include "src/common/protocol_client/chunk_server_service_gfs_client.h"
#include "src/common/protocol_client/chunk_server_service_server_client.h"
#include "src/common/utils.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"
#include "src/protos/grpc/chunk_server_lease_service.grpc.pb.h"
#include "src/server/chunk_server/chunk_server_file_service_impl.h"
#include "src/server/chunk_server/chunk_server_impl.h"
#include "src/server/chunk_server/chunk_server_lease_service_impl.h"
#include "src/server/chunk_server/file_chunk_manager.h"

using gfs::common::ConfigManager;
using namespace gfs::common::utils;
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
using protos::grpc::FileChunkMutationStatus;
using protos::grpc::GrantLeaseReply;
using protos::grpc::GrantLeaseRequest;
using protos::grpc::InitFileChunkReply;
using protos::grpc::InitFileChunkRequest;
using protos::grpc::ReadFileChunkReply;
using protos::grpc::ReadFileChunkRequest;
using protos::grpc::SendChunkDataReply;
using protos::grpc::SendChunkDataRequest;
using protos::grpc::WriteFileChunkReply;
using protos::grpc::WriteFileChunkRequest;

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

const uint64_t kTestLeaseExpirationUnixSeconds =
    absl::ToUnixSeconds(absl::Now() + absl::Hours(1));

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

void StartTestServer(const std::string& server_address,
                     const std::string& server_name,
                     const bool initialize_file_chunk_mgr,
                     const bool seed_data) {
  if (initialize_file_chunk_mgr) {
    // Initialize the test chunk server database
    FileChunkManager::GetInstance()->Initialize(
        "chunk_server_file_impl_test_db",
        /*max_chunk_size_bytes=*/1024);
  }

  ServerBuilder builder;
  auto credentials = grpc::InsecureServerCredentials();
  builder.AddListeningPort(server_address, credentials);
  StatusOr<ChunkServerImpl*> chunk_server_or =
      ChunkServerImpl::ConstructChunkServerImpl(kTestConfigPath, server_name,
                                                /*resolve_hostname=*/true);
  ChunkServerImpl* chunk_server = chunk_server_or.value();

  if (seed_data) {
    // seed test data
    SeedTestData(chunk_server);
  }

  ChunkServerLeaseServiceImpl lease_service(chunk_server);
  builder.RegisterService(&lease_service);
  ChunkServerFileServiceImpl file_service(
      chunk_server, /*clear_cached_data_after_write=*/false);
  builder.RegisterService(&file_service);
  // Start the server, and let it run until thread is cancelled
  std::unique_ptr<Server> server(builder.BuildAndStart());
  server->Wait();
}

StatusOr<SendChunkDataReply> SendDataToChunkServer(
    const std::string& server_address, const std::string& data,
    const std::string& checksum) {
  ChunkServerServiceGfsClient client(
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));

  SendChunkDataRequest request;
  request.set_data(data);
  request.set_checksum(checksum);
  return client.SendRequest(request);
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
    config_mgr_ = std::shared_ptr<ConfigManager>(
        ConfigManager::GetConfig(kTestConfigPath).value());
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
  std::shared_ptr<ConfigManager> config_mgr_;
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
  EXPECT_EQ(reply_or.value().status(), InitFileChunkReply::CREATED);
}

TEST_F(ChunkServerFileImplTest, InitExistingFileChunk) {
  InitFileChunkRequest req;
  req.set_chunk_handle(kTestFileHandle);

  // The chunk already exists, so re-initialize it should error
  grpc::ClientContext client_context;
  auto reply_or = master_server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.value().status(), InitFileChunkReply::ALREADY_EXISTS);
}

TEST_F(ChunkServerFileImplTest, ReadFileChunkOK) {
  ReadFileChunkRequest req = MakeValidReadFileChunkRequest();

  grpc::ClientContext client_context;
  auto reply_or = gfs_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.value().status(), ReadFileChunkReply::OK);
  EXPECT_EQ(reply_or.value().data(), kTestData);
  EXPECT_EQ(reply_or.value().bytes_read(), kTestData.length());
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
  EXPECT_EQ(reply_or.value().status(), ReadFileChunkReply::OK);
  EXPECT_EQ(reply_or.value().data(), kTestData.substr(0, read_length));
  EXPECT_EQ(reply_or.value().bytes_read(), read_length);
}

TEST_F(ChunkServerFileImplTest, ReadFileChunkPartialRead) {
  ReadFileChunkRequest req = MakeValidReadFileChunkRequest();
  req.set_length(kTestData.length() + 3);

  grpc::ClientContext client_context;
  auto reply_or = gfs_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.value().status(), ReadFileChunkReply::OK);

  // We will reach end of file before we read the requested number of bytes, so
  // this should be a partial read
  EXPECT_EQ(reply_or.value().data(), kTestData);
  EXPECT_EQ(reply_or.value().bytes_read(), kTestData.length());
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
  EXPECT_EQ(reply_or.value().status(), ReadFileChunkReply::OK);
  EXPECT_EQ(reply_or.value().data(),
            kTestData.substr(offset_start, offset_start + read_length));
  EXPECT_EQ(reply_or.value().bytes_read(), read_length);
}

TEST_F(ChunkServerFileImplTest, ReadFileChunkPartialReadWithOffset) {
  ReadFileChunkRequest req = MakeValidReadFileChunkRequest();
  const uint32_t offset_start = 5;
  req.set_offset_start(offset_start);
  req.set_length(kTestData.length());

  grpc::ClientContext client_context;
  auto reply_or = gfs_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.value().status(), ReadFileChunkReply::OK);
  EXPECT_EQ(reply_or.value().data(), kTestData.substr(offset_start));
  EXPECT_EQ(reply_or.value().bytes_read(),
            kTestData.length() - offset_start);
}

TEST_F(ChunkServerFileImplTest, ReadFileChunkNoFileChunk) {
  ReadFileChunkRequest req = MakeValidReadFileChunkRequest();
  req.set_chunk_handle("no-such-handle");

  grpc::ClientContext client_context;
  auto reply_or = gfs_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.value().status(),
            ReadFileChunkReply::FAILED_NOT_FOUND);
}

TEST_F(ChunkServerFileImplTest, ReadFileChunkNoChunkVersion) {
  ReadFileChunkRequest req = MakeValidReadFileChunkRequest();

  req.set_chunk_version(kTestFileVersion + 1);
  grpc::ClientContext client_context;
  StatusOr<ReadFileChunkReply> reply_or =
      gfs_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.value().status(),
            ReadFileChunkReply::FAILED_VERSION_OUT_OF_SYNC);

  req.set_chunk_version(kTestFileVersion - 1);
  grpc::ClientContext client_context2;
  reply_or = gfs_client_->SendRequest(req, client_context2);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.value().status(),
            ReadFileChunkReply::FAILED_VERSION_OUT_OF_SYNC);
}

TEST_F(ChunkServerFileImplTest, ReadFileChunkOKOutOfRange) {
  ReadFileChunkRequest req = MakeValidReadFileChunkRequest();
  req.set_offset_start(kTestData.length() + 2);

  grpc::ClientContext client_context;
  auto reply_or = gfs_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.value().status(),
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
  EXPECT_EQ(reply_or.value().status(), AdvanceFileChunkVersionReply::OK);
  EXPECT_EQ(reply_or.value().chunk_version(), req.new_chunk_version());
}

TEST_F(ChunkServerFileImplTest, AdvanceFileChunkVersionNoFileChunk) {
  AdvanceFileChunkVersionRequest req =
      MakeValidAdvanceFileChunkVersionRequest();
  req.set_chunk_handle("no-such-handle");

  grpc::ClientContext client_context;
  auto reply_or = master_server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.value().status(),
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
  EXPECT_EQ(reply_or.value().status(),
            AdvanceFileChunkVersionReply::FAILED_VERSION_OUT_OF_SYNC);

  req.set_new_chunk_version(kTestFileVersion);
  grpc::ClientContext client_context2;
  reply_or = master_server_client_->SendRequest(req, client_context2);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.value().status(),
            AdvanceFileChunkVersionReply::FAILED_VERSION_OUT_OF_SYNC);

  req.set_new_chunk_version(kTestFileVersion - 1);
  grpc::ClientContext client_context3;
  reply_or = master_server_client_->SendRequest(req, client_context3);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.value().status(),
            AdvanceFileChunkVersionReply::FAILED_VERSION_OUT_OF_SYNC);
}

//
// TEST - Write file chunk and apply mutation
//

// This tests the write request and apply mutation workflow. Creates multiple
// chunkservers and write to one as primary and let primary send apply mutation
// to the replica chunk servers. Test different scenarios such as good write,
// bad offset, bad version etc.
TEST_F(ChunkServerFileImplTest, WriteReplicatedFileChunk) {
  std::string chunk_handle = "WriteReplicatedFileChunk";
  std::string write_data = "Data for WriteReplicatedFileChunk";
  std::string write_data_checksum = calc_checksum(write_data);

  // Seed this data in the file chunk manager. Since we are running the test
  // chunk servers as threads, they'll share the same file chunk manager.
  FileChunkManager* test_file_manager = FileChunkManager::GetInstance();
  test_file_manager->CreateChunk(chunk_handle, kTestFileVersion);
  test_file_manager->WriteToChunk(chunk_handle, kTestFileVersion,
                                  /*start_offset=*/0, kTestData.length(),
                                  kTestData);

  std::string primary_chunk_server_name = kTestServerName;

  std::vector<std::string> replica_chunk_server_names = {
      "write_chunk_server_01", "write_chunk_server_02"};

  std::vector<std::string> all_chunk_server_names(replica_chunk_server_names);
  all_chunk_server_names.push_back(primary_chunk_server_name);

  std::vector<std::thread> replica_chunk_server_threads;

  // Start replica servers, primary is already running from test main.
  for (auto& server_name : replica_chunk_server_names) {
    replica_chunk_server_threads.push_back(std::thread(
        StartTestServer,
        config_mgr_->GetServerAddress(server_name, /*resolve_hostname=*/true),
        server_name,
        /*initialize_file_chunk_mgr=*/false, /*seed_data=*/false));
  }

  // Wait for servers to fully initialize
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Send data to all servers, trivial no need to parallelize
  for (auto& server_name : all_chunk_server_names) {
    auto reply = SendDataToChunkServer(
        config_mgr_->GetServerAddress(server_name, /*resolve_hostname=*/true),
        write_data, write_data_checksum);

    EXPECT_TRUE(reply.ok());
    EXPECT_EQ(reply.value().status(), SendChunkDataReply::OK);
  }

  // Grant primary write lease on file chunk
  GrantLeaseRequest lease_request;
  lease_request.set_chunk_handle(chunk_handle);
  lease_request.set_chunk_version(kTestFileVersion);
  lease_request.mutable_lease_expiration_time()->set_seconds(
      kTestLeaseExpirationUnixSeconds);

  auto lease_reply = master_server_client_->SendRequest(lease_request);

  EXPECT_TRUE(lease_reply.ok());
  EXPECT_EQ(lease_reply.value().status(), GrantLeaseReply::ACCEPTED);

  // Case 1:
  // Now ask primary to write data, once primary is done it will tell replicas
  // to write too. This should succeed.

  ChunkServerServiceGfsClient primary_client(grpc::CreateChannel(
      config_mgr_->GetServerAddress(primary_chunk_server_name,
                                    /*resolve_hostname=*/true),
      grpc::InsecureChannelCredentials()));

  // prepare write request
  WriteFileChunkRequest write_request;
  auto header = write_request.mutable_header();
  header->set_chunk_handle(chunk_handle);
  header->set_chunk_version(kTestFileVersion);
  header->set_offset_start(0);
  header->set_length(write_data.size());
  header->set_data_checksum(write_data_checksum);

  // Add replica locations to write request
  for (auto& server_name : replica_chunk_server_names) {
    auto location = write_request.add_replica_locations();
    location->set_server_hostname(server_name);
    location->set_server_port(config_mgr_->GetServerPort(server_name));
  }

  auto write_reply_or = primary_client.SendRequest(write_request);

  EXPECT_TRUE(write_reply_or.ok());

  auto write_reply = write_reply_or.value();

  EXPECT_EQ(write_reply.status(), FileChunkMutationStatus::OK);

  // Check that all replica servers responded
  EXPECT_EQ(replica_chunk_server_names.size(),
            write_reply.replica_status_size());

  for (auto& replica : write_reply.replica_status()) {
    EXPECT_EQ(replica.status(), FileChunkMutationStatus::OK);
  }

  // Case 2:
  // Now try to write wrong version. This should fail.

  // Again send data to chunk servers, since cache is cleared after Write.
  // Send data to all servers, trivial no need to parallelize
  for (auto& server_name : all_chunk_server_names) {
    auto reply = SendDataToChunkServer(
        config_mgr_->GetServerAddress(server_name, /*resolve_hostname=*/true),
        write_data, write_data_checksum);

    EXPECT_TRUE(reply.ok());
    EXPECT_EQ(reply.value().status(), SendChunkDataReply::OK);
  }

  write_request.mutable_header()->set_chunk_version(kTestFileVersion + 1);

  write_reply_or = primary_client.SendRequest(write_request);

  EXPECT_TRUE(write_reply_or.ok());

  write_reply = write_reply_or.value();

  EXPECT_EQ(write_reply.status(),
            FileChunkMutationStatus::FAILED_STALE_VERSION);

  // No replica status, since it failed in primary
  EXPECT_EQ(0, write_reply.replica_status_size());

  // Case 3:
  // Now try to write wrong offset. This should fail.

  // Again send data to chunk servers, since cache is cleared after Write.
  // Send data to all servers, trivial no need to parallelize
  for (auto& server_name : all_chunk_server_names) {
    auto reply = SendDataToChunkServer(
        config_mgr_->GetServerAddress(server_name, /*resolve_hostname=*/true),
        write_data, write_data_checksum);

    EXPECT_TRUE(reply.ok());
    EXPECT_EQ(reply.value().status(), SendChunkDataReply::OK);
  }

  // Set correct version
  write_request.mutable_header()->set_chunk_version(kTestFileVersion);
  // Set bad start offset
  write_request.mutable_header()->set_offset_start(write_data.size() + 5);

  write_reply_or = primary_client.SendRequest(write_request);

  EXPECT_TRUE(write_reply_or.ok());

  write_reply = write_reply_or.value();

  EXPECT_EQ(write_reply.status(), FileChunkMutationStatus::FAILED_OUT_OF_RANGE);

  // No replica status, since it failed in primary
  EXPECT_EQ(0, write_reply.replica_status_size());

  // Case 4:
  // Now try to write without sending the data first. This should fail, since
  // data won't be in cache.

  std::string new_write_data =
      "Now try to write without sending the data first";
  std::string new_write_data_checksum = calc_checksum(new_write_data);

  write_request.mutable_header()->set_length(new_write_data.size());
  write_request.mutable_header()->set_data_checksum(new_write_data_checksum);
  write_request.mutable_header()->set_offset_start(0);

  write_reply_or = primary_client.SendRequest(write_request);

  EXPECT_TRUE(write_reply_or.ok());

  write_reply = write_reply_or.value();

  EXPECT_EQ(write_reply.status(),
            FileChunkMutationStatus::FAILED_DATA_NOT_FOUND);

  // No replica status, since it failed in primary
  EXPECT_EQ(0, write_reply.replica_status_size());

  // kill the replica servers
  for (std::size_t i = 0; i < replica_chunk_server_threads.size(); ++i) {
    pthread_cancel(replica_chunk_server_threads[i].native_handle());
    replica_chunk_server_threads[i].join();
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Start a server for lease gRPC handler in the background, and wait some time
  // for the server to successfully start in background
  std::thread server_thread =
      std::thread(StartTestServer, kTestServerAddress, kTestServerName,
                  /*initialize_file_chunk_mgr=*/true,
                  /*const bool seed_data =*/true);
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Run tests
  int exit_code = RUN_ALL_TESTS();

  // Clean up background server
  pthread_cancel(server_thread.native_handle());
  server_thread.join();

  return exit_code;
}