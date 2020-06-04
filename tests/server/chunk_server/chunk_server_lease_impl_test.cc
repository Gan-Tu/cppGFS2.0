#include <chrono>
#include <thread>

#include "absl/time/time.h"
#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "gtest/gtest.h"
#include "src/common/protocol_client/chunk_server_service_server_client.h"
#include "src/protos/grpc/chunk_server_lease_service.grpc.pb.h"
#include "src/server/chunk_server/chunk_server_impl.h"
#include "src/server/chunk_server/chunk_server_lease_service_impl.h"
#include "src/server/chunk_server/file_chunk_manager.h"

using gfs::server::ChunkServerImpl;
using gfs::server::FileChunkManager;
using gfs::service::ChunkServerLeaseServiceImpl;
using gfs::service::ChunkServerServiceMasterServerClient;
using google::protobuf::util::StatusOr;
using grpc::Server;
using grpc::ServerBuilder;
using protos::grpc::GrantLeaseReply;
using protos::grpc::GrantLeaseRequest;
using protos::grpc::RevokeLeaseReply;
using protos::grpc::RevokeLeaseRequest;

const std::string kTestConfigPath = "tests/server/chunk_server/test_config.yml";
const std::string kTestServerName = "chunk_server_01";
const std::string kTestServerAddress = "0.0.0.0:50052";

// Since we share the same test chunk server in the background, we let grant
// and revoke lease unit tests issue requests to *differnet* file handles, so
// the mutations created by unit tests themselves don't interfere each other
//
// This is indeed not ideal, but does ther job for now
const std::string kTestGrantLeaseChunkHandle = "9d2a2342-97f9-11ea";
const std::string kTestRevokeLeaseChunkHandle_PERSIST = "bb37-0242ac130002";
const std::string kTestRevokeLeaseChunkHandle_REVOKABLE = "0fd8e43c-a2e5-11ea";
const uint32_t kTestFileVersion = 2;
const uint64_t kTestExpirationUnixSeconds =
    absl::ToUnixSeconds(absl::Now() + absl::Hours(1));

namespace {
void SeedTestData(ChunkServerImpl* chunk_server) {
  // initial chunks
  FileChunkManager::GetInstance()->CreateChunk(kTestGrantLeaseChunkHandle,
                                               kTestFileVersion);
  FileChunkManager::GetInstance()->CreateChunk(
      kTestRevokeLeaseChunkHandle_PERSIST, kTestFileVersion);
  FileChunkManager::GetInstance()->CreateChunk(
      kTestRevokeLeaseChunkHandle_REVOKABLE, kTestFileVersion);
  // initial lease
  chunk_server->AddOrUpdateLease(kTestRevokeLeaseChunkHandle_PERSIST,
                                 kTestExpirationUnixSeconds);
  chunk_server->AddOrUpdateLease(kTestRevokeLeaseChunkHandle_REVOKABLE,
                                 kTestExpirationUnixSeconds);
}

void StartTestServer() {
  // Initialize the test chunk server database
  FileChunkManager::GetInstance()->Initialize("chunk_server_lease_impl_test_db",
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
  // Start the server, and let it run until thread is cancelled
  std::unique_ptr<Server> server(builder.BuildAndStart());
  server->Wait();
}
}  // namespace

class ChunkServerLeaseImplTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialize a test specific client
    server_client_ = std::make_shared<ChunkServerServiceMasterServerClient>(
        grpc::CreateChannel(kTestServerAddress,
                            grpc::InsecureChannelCredentials()));
  }

  GrantLeaseRequest MakeValidGrantLeaseRequest() {
    GrantLeaseRequest req;
    req.set_chunk_handle(kTestGrantLeaseChunkHandle);
    req.set_chunk_version(kTestFileVersion);
    req.mutable_lease_expiration_time()->set_seconds(
        kTestExpirationUnixSeconds);
    return req;
  }

  RevokeLeaseRequest MakeValidRevokeLeaseRequest() {
    RevokeLeaseRequest req;
    req.set_chunk_handle(kTestRevokeLeaseChunkHandle_PERSIST);
    req.mutable_original_lease_expiration_time()->set_seconds(
        kTestExpirationUnixSeconds);
    return req;
  }

  std::shared_ptr<ChunkServerServiceMasterServerClient> server_client_;
};

TEST_F(ChunkServerLeaseImplTest, GrantLeaseValidRequest) {
  GrantLeaseRequest req = MakeValidGrantLeaseRequest();
  grpc::ClientContext client_context;
  auto reply_or = server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(), GrantLeaseReply::ACCEPTED);
}

TEST_F(ChunkServerLeaseImplTest, GrantLeaseNoChunkHandle) {
  GrantLeaseRequest req = MakeValidGrantLeaseRequest();
  req.set_chunk_handle("no-such-handle");  // chunk server doesn't have this

  grpc::ClientContext client_context;
  auto reply_or = server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(),
            GrantLeaseReply::REJECTED_NOT_FOUND);
}

TEST_F(ChunkServerLeaseImplTest, GrantLeaseStaleVersionOnServer) {
  GrantLeaseRequest req = MakeValidGrantLeaseRequest();
  req.set_chunk_version(kTestFileVersion + 1);

  grpc::ClientContext client_context;
  auto reply_or = server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(),
            GrantLeaseReply::REJECTED_STALE_VERSION);
}

TEST_F(ChunkServerLeaseImplTest, GrantLeaseStaleVersionInRequest) {
  GrantLeaseRequest req = MakeValidGrantLeaseRequest();
  req.set_chunk_version(kTestFileVersion - 1);

  grpc::ClientContext client_context;
  auto reply_or = server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(), GrantLeaseReply::UNKNOWN);
}

TEST_F(ChunkServerLeaseImplTest, GrantLeaseExpiredLease) {
  GrantLeaseRequest req = MakeValidGrantLeaseRequest();
  req.mutable_lease_expiration_time()->set_seconds(
      absl::ToUnixSeconds(absl::Now() - absl::Minutes(1)));

  grpc::ClientContext client_context;
  auto reply_or = server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(),
            GrantLeaseReply::IGNORED_EXPIRED_LEASE);
}

TEST_F(ChunkServerLeaseImplTest, RevokeLeaseValidRequest) {
  RevokeLeaseRequest req = MakeValidRevokeLeaseRequest();
  req.set_chunk_handle(kTestRevokeLeaseChunkHandle_REVOKABLE);

  grpc::ClientContext client_context;
  auto reply_or = server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(), RevokeLeaseReply::REVOKED);
}

TEST_F(ChunkServerLeaseImplTest, RevokeLeaseNewerLeaseAlreadyExists) {
  RevokeLeaseRequest req = MakeValidRevokeLeaseRequest();
  req.mutable_original_lease_expiration_time()->set_seconds(
      kTestExpirationUnixSeconds - 1000);

  grpc::ClientContext client_context;
  auto reply_or = server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(),
            RevokeLeaseReply::IGNORED_HAS_NEWER_LEASE);
}

TEST_F(ChunkServerLeaseImplTest, RevokeLeaseNoChunkHandle) {
  RevokeLeaseRequest req = MakeValidRevokeLeaseRequest();
  req.set_chunk_handle("no-such-handle");

  grpc::ClientContext client_context;
  auto reply_or = server_client_->SendRequest(req, client_context);
  EXPECT_TRUE(reply_or.ok());
  EXPECT_EQ(reply_or.ValueOrDie().status(),
            RevokeLeaseReply::REJECTED_NOT_FOUND);
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
