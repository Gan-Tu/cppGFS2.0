#include <stack>
#include <thread>

#include "gtest/gtest.h"
#include "src/server/master_server/lock_manager.h"

using namespace gfs::server;

class LockManagerUnitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    lockManager_ = LockManager::GetInstance();
  }

  LockManager* lockManager_;
};

// Add lock step by step and make sure the exist
TEST_F(LockManagerUnitTest, AddLock) {
  EXPECT_EQ(lockManager_->Exist("/foo"), false);
  // Successfully add a lock for /foo
  auto fooRes(lockManager_->CreateLock("/foo"));
  EXPECT_TRUE(fooRes.ok());
  EXPECT_NE(fooRes.ValueOrDie(), nullptr);
  // Successfully add a lock for /foo/bar
  auto barRes(lockManager_->CreateLock("/foo/bar"));
  EXPECT_TRUE(barRes.ok());
  EXPECT_NE(barRes.ValueOrDie(), nullptr);
}

// Add locks in parallel for the case that they share nothing
// create /0, /1, ... /10
TEST_F(LockManagerUnitTest, AddLockInParallel) {
  auto numOfThreads(10);
  std::vector<std::thread> threads;
  for (int i = 0; i < numOfThreads; i++) {
    threads.push_back(std::thread([&, i]() {
      lockManager_->CreateLock("/" + std::to_string(i));
    }));
  }

  // Join all threads
  for (int i = 0; i < numOfThreads; i++) {
    threads[i].join();
  }

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
      auto lkRes(lockManager_->CreateLock("/samePath"));
      if (lkRes.ok()) {
        cnt++;
      }
    }));
  }

  // Join all threads
  for (int i = 0; i < numOfThreads; i++) {
    threads[i].join();
  }

  // Only one of the above calls returns successfully
  EXPECT_EQ(cnt.load(), 1);
}

// Add lock for /a, /a/b and /a/b/c and test that the ParentLocksAnchor 
// works as expected by checking the stack size
TEST_F(LockManagerUnitTest, AcquireLockForParentDir) {
  auto aRes(lockManager_->CreateLock("/a"));
  EXPECT_NE(aRes.ValueOrDie(), nullptr);
  auto bRes(lockManager_->CreateLock("/a/b"));
  EXPECT_NE(bRes.ValueOrDie(), nullptr);
  auto cRes(lockManager_->CreateLock("/a/b/c"));
  EXPECT_NE(cRes.ValueOrDie(), nullptr);

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
TEST_F(LockManagerUnitTest, CheckErrorMessageForLock) {
  auto createRes(lockManager_->CreateLock("/duplicate"));
  EXPECT_TRUE(createRes.ok());
  auto dupCreateRes(lockManager_->CreateLock("/duplicate"));
  EXPECT_EQ(dupCreateRes.status().error_message(), 
            "Lock already exists for /duplicate");

  auto nonExistRes(lockManager_->FetchLock("/nonExist"));
  EXPECT_EQ(nonExistRes.ok(), false);
  EXPECT_EQ(nonExistRes.status().error_message(),
            "Lock does not exist for /nonExist");

  auto nonExistPath("/aa/bb/cc");
  ParentLocksAnchor anchor(lockManager_, nonExistPath);
  EXPECT_EQ(anchor.ok(), false);
  EXPECT_EQ(anchor.status().error_message(),
            "Lock for /aa does not exist");
}
