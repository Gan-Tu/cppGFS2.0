#include "src/server/chunk_server/chunk_server_impl.h"

#include "absl/time/time.h"
#include "google/protobuf/stubs/statusor.h"
#include "gtest/gtest.h"
#include "src/server/chunk_server/file_chunk_manager.h"

using gfs::server::ChunkServerImpl;
using gfs::server::FileChunkManager;
using google::protobuf::util::StatusOr;

class ChunkServerImplTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    FileChunkManager::GetInstance()->Initialize("chunk_server_impl_test_db",
                                                /*max_chunk_size_bytes=*/1024);
  }
  void SetUp() override {
    StatusOr<ChunkServerImpl*> chunk_server_or =
        ChunkServerImpl::ConstructChunkServerImpl(
            "tests/server/chunk_server/test_config.yml",
            /*chunk_server_name=*/"chunk_server_01",
            /*resolve_hostname=*/false);
    chunk_server_ = chunk_server_or.ValueOrDie();
  }
  ChunkServerImpl* chunk_server_;
};

TEST_F(ChunkServerImplTest, AddOrUpdateLease) {
  const std::string file_handle = "/test/handle";

  // Should have no lease yet
  EXPECT_FALSE(chunk_server_->HasWriteLease(file_handle));
  EXPECT_FALSE(chunk_server_->GetLeaseExpirationTime(file_handle).ok());

  // Add a lease and now it should have the lease
  absl::Time expected_expiration = absl::Now() + absl::Hours(1);
  uint64_t expected_expiration_sec = absl::ToUnixSeconds(expected_expiration);
  chunk_server_->AddOrUpdateLease(file_handle, expected_expiration_sec);
  StatusOr<absl::Time> actual_expiration_or =
      chunk_server_->GetLeaseExpirationTime(file_handle);
  EXPECT_TRUE(actual_expiration_or.ok());
  EXPECT_EQ(absl::ToUnixSeconds(actual_expiration_or.ValueOrDie()),
            expected_expiration_sec);
  EXPECT_TRUE(chunk_server_->HasWriteLease(file_handle));

  // Update the lease expiration time
  expected_expiration = absl::Now();
  expected_expiration_sec = absl::ToUnixSeconds(expected_expiration);
  chunk_server_->AddOrUpdateLease(file_handle, expected_expiration_sec);
  actual_expiration_or = chunk_server_->GetLeaseExpirationTime(file_handle);
  EXPECT_TRUE(actual_expiration_or.ok());
  EXPECT_EQ(absl::ToUnixSeconds(actual_expiration_or.ValueOrDie()),
            expected_expiration_sec);
  // lease should have expired, as we set the expiration to NOW at update time
  EXPECT_FALSE(chunk_server_->HasWriteLease(file_handle));
}

TEST_F(ChunkServerImplTest, RemoveLease) {
  const std::string file_handle = "/test/handle";
  EXPECT_FALSE(chunk_server_->HasWriteLease(file_handle));

  // Add a lease and now it should have the lease
  uint64_t expected_expiration_sec =
      absl::ToUnixSeconds(absl::Now() + absl::Hours(1));
  chunk_server_->AddOrUpdateLease(file_handle, expected_expiration_sec);
  EXPECT_TRUE(chunk_server_->HasWriteLease(file_handle));
  EXPECT_TRUE(chunk_server_->GetLeaseExpirationTime(file_handle).ok());

  // Remove lease
  chunk_server_->RemoveLease(file_handle);
  EXPECT_FALSE(chunk_server_->HasWriteLease(file_handle));
  EXPECT_FALSE(chunk_server_->GetLeaseExpirationTime(file_handle).ok());
}

TEST_F(ChunkServerImplTest, ChunkVersionGetterSetter) {
  const std::string file_handle = "/test/handle";

  // No chunk version is set yet, so can't get the version
  EXPECT_FALSE(chunk_server_->GetChunkVersion(file_handle).ok());

  // Set chunk version
  uint32_t expected_chunk_version = 10;
  // Now we should get the same chunk version back
  FileChunkManager::GetInstance()->CreateChunk(file_handle,
                                               expected_chunk_version);
  StatusOr<uint32_t> actual_chunk_version_or =
      chunk_server_->GetChunkVersion(file_handle);
  EXPECT_TRUE(actual_chunk_version_or.ok());
  EXPECT_EQ(actual_chunk_version_or.ValueOrDie(), expected_chunk_version);
}

TEST_F(ChunkServerImplTest, GetMasterProtocolClient) {
  auto master_client = chunk_server_->GetMasterProtocolClient("0.0.0.0:80");
  // Should reuse the client, if listening at same server address
  EXPECT_EQ(chunk_server_->GetMasterProtocolClient("0.0.0.0:80"),
            master_client);
  // Should create new client, if listening at different server address
  EXPECT_NE(chunk_server_->GetMasterProtocolClient("0.0.0.0:50051"),
            master_client);
}