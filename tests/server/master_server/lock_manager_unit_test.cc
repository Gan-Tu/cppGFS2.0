#include <stack>
#include <thread>

#include "gtest/gtest.h"
#include "src/server/master_server/lock_manager.h"
#include "tests/utils.h"

using namespace gfs::server;
using namespace tests;

class LockManagerUnitTest : public ::testing::Test {
 protected:
  void SetUp() override { lockManager_ = LockManager::GetInstance(); }

  LockManager* lockManager_;
};

// Add lock step by step and make sure the exist
TEST_F(LockManagerUnitTest, AddLock) {
  EXPECT_EQ(lockManager_->Exist("/foo"), false);
  // Successfully add a lock for /foo
  auto foo_lock_or(lockManager_->CreateLock("/foo"));
  EXPECT_TRUE(foo_lock_or.ok());
  EXPECT_NE(foo_lock_or.ValueOrDie(), nullptr);
  // Successfully add a lock for /foo/bar
  auto bar_lock_or(lockManager_->CreateLock("/foo/bar"));
  EXPECT_TRUE(bar_lock_or.ok());
  EXPECT_NE(bar_lock_or.ValueOrDie(), nullptr);
}

// Add locks in parallel for the case that they share nothing
// create /0, /1, ... /10
TEST_F(LockManagerUnitTest, AddLockInParallel) {
  auto numOfThreads(10);
  std::vector<std::thread> threads;
  for (int i = 0; i < numOfThreads; i++) {
    threads.push_back(std::thread(
        [&, i]() { lockManager_->CreateLock("/" + std::to_string(i)); }));
  }

  JoinAndClearThreads(threads);

  // Check that all locks exist
  for (int i = 0; i < numOfThreads; i++) {
    EXPECT_TRUE(lockManager_->Exist("/" + std::to_string(i)));
  }
}

// Add the same lock in parallel, ensure that only one succeeds
TEST_F(LockManagerUnitTest, AddSameLockInParallel) {
  auto numOfThreads(10);
  std::atomic<int> cnt(0);

  std::vector<std::thread> threads;
  for (int i = 0; i < numOfThreads; i++) {
    threads.push_back(std::thread([&, i]() {
      auto path_lock_or(lockManager_->CreateLock("/samePath"));
      if (path_lock_or.ok()) {
        cnt++;
      }
    }));
  }

  JoinAndClearThreads(threads);

  // Only one of the above calls returns successfully
  EXPECT_EQ(cnt.load(), 1);
}

// Add lock for /a, /a/b and /a/b/c and test that the ParentLocksAnchor
// works as expected by checking the stack size
TEST_F(LockManagerUnitTest, AcquireLockForParentDir) {
  auto a_lock_or(lockManager_->CreateLock("/a"));
  EXPECT_NE(a_lock_or.ValueOrDie(), nullptr);
  auto b_lock_or(lockManager_->CreateLock("/a/b"));
  EXPECT_NE(b_lock_or.ValueOrDie(), nullptr);
  auto c_lock_or(lockManager_->CreateLock("/a/b/c"));
  EXPECT_NE(c_lock_or.ValueOrDie(), nullptr);

  ParentLocksAnchor anchor(lockManager_, "/a/b/c");
  EXPECT_TRUE(anchor.ok());
  EXPECT_EQ(anchor.lock_size(), (unsigned int)2);
}

// 1. Create a lock, then try to create it again, this should result in an error
// saying "Key already exists"
// 2. Fetch a non-existing lock, this should result in an error saying "Key does
// not exist"
// 3. Use ParentLocksAnchor to lock a path where some intermediate path does not
// exist, this should result in an error saying "Lock for path does not exist"
TEST_F(LockManagerUnitTest, CheckErrorCases) {
  auto create_lock_or(lockManager_->CreateLock("/duplicate"));
  EXPECT_TRUE(create_lock_or.ok());
  auto duplicate_create_lock_or(lockManager_->CreateLock("/duplicate"));
  EXPECT_EQ(duplicate_create_lock_or.status().error_code(),
            google::protobuf::util::error::ALREADY_EXISTS);

  auto non_exist_lock_or(lockManager_->FetchLock("/nonExist"));
  EXPECT_EQ(non_exist_lock_or.ok(), false);
  EXPECT_EQ(non_exist_lock_or.status().error_code(),
            google::protobuf::util::error::NOT_FOUND);

  auto non_exist_path_lock_or("/aa/bb/cc");
  ParentLocksAnchor anchor(lockManager_, non_exist_path_lock_or);
  EXPECT_EQ(anchor.ok(), false);
  EXPECT_EQ(anchor.status().error_code(),
            google::protobuf::util::error::NOT_FOUND);
}
