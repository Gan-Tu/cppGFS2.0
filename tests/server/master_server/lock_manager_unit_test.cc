#include <stack>
#include <thread>

#include "gtest/gtest.h"
#include "src/server/master_server/lock_manager.h"

class LockManagerUnitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    lockManager_ = gfs::server::LockManager::GetInstance();
  }

  gfs::server::LockManager* lockManager_;
};

TEST_F(LockManagerUnitTest, AddLock) {
  EXPECT_EQ(lockManager_->Exist("/foo"), false);
  // Successfully add a lock for /foo
  auto foo(lockManager_->AddLockIfNonExist("/foo"));
  EXPECT_NE(foo, nullptr);
  // Successfully add a lock for /foo/bar
  auto bar(lockManager_->AddLockIfNonExist("/foo/bar"));
  EXPECT_NE(bar, nullptr);
}

TEST_F(LockManagerUnitTest, AddLockInParallel) {
  // Add locks in parallel for the case that they share nothing
  // create /0, /1, ... /10
  auto numOfThreads(10);
  std::vector<std::thread> threads;
  for (int i = 0; i < numOfThreads; i++) {
    threads.push_back(std::thread([&, i]() {
      lockManager_->AddLockIfNonExist("/" + std::to_string(i));
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
      auto lk(lockManager_->AddLockIfNonExist("/samePath"));
      if (lk) {
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
  auto a(lockManager_->AddLockIfNonExist("/a"));
  EXPECT_NE(a, nullptr);
  auto b(lockManager_->AddLockIfNonExist("/a/b"));
  EXPECT_NE(b, nullptr);
  auto c(lockManager_->AddLockIfNonExist("/a/b/c"));
  EXPECT_NE(c, nullptr);

  std::stack<absl::Mutex*> locks;
  lockManager_->AcquireLockForParentDir("/a/b/c", locks);
  EXPECT_EQ(locks.size(), (unsigned int)2);

  lockManager_->ReleaseLockForParentDir(locks);
  EXPECT_EQ(locks.size(), 0);
}
