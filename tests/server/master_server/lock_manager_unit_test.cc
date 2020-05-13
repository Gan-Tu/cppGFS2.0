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

TEST_F(LockManagerUnitTest, AddLock) {
  EXPECT_EQ(lockManager_->Exist("/foo"), false);
  // Successfully add a lock for /foo
  auto fooRes(lockManager_->CreateLock("/foo"));
  EXPECT_EQ(fooRes.ok(), true);
  EXPECT_NE(fooRes.ValueOrDie(), nullptr);
  // Successfully add a lock for /foo/bar
  auto barRes(lockManager_->CreateLock("/foo/bar"));
  EXPECT_EQ(barRes.ok(), true);
  EXPECT_NE(barRes.ValueOrDie(), nullptr);
}

TEST_F(LockManagerUnitTest, AddLockInParallel) {
  // Add locks in parallel for the case that they share nothing
  // create /0, /1, ... /10
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
    EXPECT_EQ(lockManager_->Exist("/" + std::to_string(i)), true);
  }
}

TEST_F(LockManagerUnitTest, AddSameLockInParallel) {
  // Add the same lock in parallel, ensure that only one succeeds
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

TEST_F(LockManagerUnitTest, AcquireLockForParentDir) {
  // Add lock for /a, /a/b and /a/b/c and test that the AcquireLockForParentDir
  // method works as expected by checking the stack size
  auto aRes(lockManager_->CreateLock("/a"));
  EXPECT_NE(aRes.ValueOrDie(), nullptr);
  auto bRes(lockManager_->CreateLock("/a/b"));
  EXPECT_NE(bRes.ValueOrDie(), nullptr);
  auto cRes(lockManager_->CreateLock("/a/b/c"));
  EXPECT_NE(cRes.ValueOrDie(), nullptr);

  ParentLocksAnchor anchor(lockManager_, "/a/b/c");
  EXPECT_EQ(anchor.ok(), true);
  EXPECT_EQ(anchor.lock_size(), (unsigned int)2);
}
