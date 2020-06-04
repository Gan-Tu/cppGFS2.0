#include "src/common/config_manager.h"

#include "gtest/gtest.h"

using gfs::common::ConfigManager;

class ConfigManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_ =
        ConfigManager::GetConfig("./tests/common/test_config.yml").ValueOrDie();
  }
  ConfigManager* config_ = nullptr;
};

TEST_F(ConfigManagerTest, HasServerTest) {
  EXPECT_TRUE(config_->HasMasterServer("test_master"));
  EXPECT_FALSE(config_->HasChunkServer("test_master"));

  EXPECT_TRUE(config_->HasChunkServer("test_chunk_server_01"));
  EXPECT_FALSE(config_->HasMasterServer("test_chunk_server_01"));

  EXPECT_TRUE(config_->HasChunkServer("test_chunk_server_02"));
  EXPECT_FALSE(config_->HasMasterServer("test_chunk_server_02"));
}

TEST_F(ConfigManagerTest, GetServerHostnameTest) {
  EXPECT_EQ(config_->GetServerHostname("test_master"), "test_master_hostname");
  EXPECT_EQ(config_->GetServerHostname("test_chunk_server_01"),
            "test_chunk_server_01_hostname");
  EXPECT_EQ(config_->GetServerHostname("test_chunk_server_02"),
            "test_chunk_server_02_hostname");
}

TEST_F(ConfigManagerTest, GetServerPortTest) {
  EXPECT_EQ(config_->GetServerPort("test_master"), 10002);
  EXPECT_EQ(config_->GetServerPort("test_chunk_server_01"), 8080);
  EXPECT_EQ(config_->GetServerPort("test_chunk_server_02"), 50051);
}

TEST_F(ConfigManagerTest, ResolveHostnameTest) {
  EXPECT_EQ(config_->ResolveHostname("test_master_hostname"), "0.0.0.0");
  EXPECT_EQ(config_->ResolveHostname("test_chunk_server_01_hostname"),
            "255.255.255.0");
  EXPECT_EQ(config_->ResolveHostname("test_chunk_server_02_hostname"),
            "92.0.2.1");
}

TEST_F(ConfigManagerTest, GetServerAddressTest) {
  EXPECT_EQ(config_->GetServerAddress("test_master"),
            "test_master_hostname:10002");
  EXPECT_EQ(config_->GetServerAddress("test_chunk_server_01"),
            "test_chunk_server_01_hostname:8080");
  EXPECT_EQ(config_->GetServerAddress("test_chunk_server_02"),
            "test_chunk_server_02_hostname:50051");

  EXPECT_EQ(config_->GetServerAddress("test_master", /*resolve_hostname=*/true),
            "0.0.0.0:10002");
  EXPECT_EQ(config_->GetServerAddress("test_chunk_server_01",
                                      /*resolve_hostname=*/true),
            "255.255.255.0:8080");
  EXPECT_EQ(config_->GetServerAddress("test_chunk_server_02",
                                      /*resolve_hostname=*/true),
            "92.0.2.1:50051");
}

TEST_F(ConfigManagerTest, GetDatabaseNameTest) {
  EXPECT_EQ(config_->GetDatabaseName("test_chunk_server_01"),
            "data/gfs_db_test_chunk_server_01");
  EXPECT_EQ(config_->GetDatabaseName("test_chunk_server_02"),
            "data/gfs_db_test_chunk_server_02");
}

TEST_F(ConfigManagerTest, GetDiskMetadataTest) {
  EXPECT_EQ(config_->GetFileChunkBlockSize(), 64);
  EXPECT_EQ(config_->GetRequiredDiskSpaceToMaintain(), 100);
}

TEST_F(ConfigManagerTest, GetTimeoutTest) {
  EXPECT_EQ(config_->GetGrpcDeadline(), absl::Seconds(10));
  EXPECT_EQ(config_->GetWriteLeaseTimeout(), absl::Minutes(1));
  EXPECT_EQ(config_->GetHeartBeatTaskSleepDuration(), absl::Seconds(30));
  EXPECT_EQ(config_->GetClientCacheTimeout(), absl::Hours(1));
}