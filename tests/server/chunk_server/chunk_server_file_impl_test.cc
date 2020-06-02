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
using protos::grpc::GrantLeaseRequest;
using protos::grpc::RevokeLeaseRequest;

const std::string kTestConfigPath = "tests/server/chunk_server/test_config.yml";
const std::string kTestServerName = "chunk_server_01";
const std::string kTestServerAddress = "0.0.0.0:50052";

// Since we share the same test chunk server in the background, we let grant
// and revoke lease unit tests issue requests to *differnet* file handles, so
// the mutations created by unit tests themselves don't interfere each other
//
// This is indeed not ideal, but does ther job for now
// const std::string kTestGrantLeaseChunkHandle = "9d2a2342-97f9-11ea";
// const std::string kTestRevokeLeaseChunkHandle_PERSIST = "bb37-0242ac130002";
// const std::string kTestRevokeLeaseChunkHandle_REVOKABLE =
// "0fd8e43c-a2e5-11ea"; const uint32_t kTestFileVersion = 2; const uint64_t
// kTestExpirationUnixSeconds =
//     absl::ToUnixSeconds(absl::Now() + absl::Hours(1));

namespace {
void SeedTestData(ChunkServerImpl* chunk_server) {
  // // initial chunks
  // FileChunkManager::GetInstance()->CreateChunk(kTestGrantLeaseChunkHandle,
  //                                              kTestFileVersion);
  // // initial lease
  // chunk_server->AddOrUpdateLease(kTestRevokeLeaseChunkHandle_PERSIST,
  //                                kTestExpirationUnixSeconds);
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

  std::shared_ptr<ChunkServerServiceMasterServerClient> master_server_client_;
  std::shared_ptr<ChunkServerServiceChunkServerClient> chunk_server_client_;
  std::shared_ptr<ChunkServerServiceGfsClient> gfs_client_;
};

TEST_F(ChunkServerFileImplTest, Basic) { EXPECT_TRUE(true); }

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