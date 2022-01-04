#include "src/server/master_server/lock_manager.h"

#include <thread>

using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;

namespace gfs {
namespace server {

bool LockManager::Exist(const std::string& filename) {
  return file_path_locks_.Contains(filename);
}

google::protobuf::util::StatusOr<absl::Mutex*> LockManager::CreateLock(
    const std::string& filename) {
  std::shared_ptr<absl::Mutex> new_lock(new absl::Mutex());

  // If the filename exists, that means another thread has created this
  // lock, or this thread has been created previously by the current
  // thread
  if (!file_path_locks_.TryInsert(filename, new_lock)) {
    return Status(google::protobuf::util::kAlreadyExists,
                  "Lock already exists for " + filename);
  }

  // TODO: We have not finalized the plan regarding to the removal of locks.
  // The simplest approach is to say we don't remove locks (note that not
  // removing locks doesn't equate to not removing files) so we won't have
  // an issue that a removal takes place between the above and code below.
  // This may seem too restricted, but dealing with concurrent creation and
  // deletion is a pain and having a full-support of this may be rather
  // an overkill. What we can do instead is to defer the deletion of
  // lock resources (they are relatively small comparing to file metadata)
  // and say, let's remove the lock resources only after the actual chunk
  // resources got garbage collected. Let's see how far we can go with this
  // project and see if we get a chance to improve the design here.
  return new_lock.get();
}

google::protobuf::util::StatusOr<absl::Mutex*> LockManager::FetchLock(
    const std::string& filename) {
  // The second item of the TryGetValue return indicates whether the item
  // exists in the collection, and the first item corresponds to the item
  // the second item is true
  auto try_get_lock(file_path_locks_.TryGetValue(filename));
  bool lock_exist(try_get_lock.second);

  if (!lock_exist) {
    return Status(google::protobuf::util::kNotFound,
                  "Lock does not exist for " + filename);
  }
  return try_get_lock.first.get();
}

LockManager* LockManager::GetInstance() {
  static LockManager* manager = new LockManager();
  return manager;
}

/* Acquire readerLock for the parent paths of a given filename, i.e. if
 * filename is "/foo/bar/baz", the lock manager acquires reader lock for
 * "/foo" and "/foo/bar", and store these locks in a stack. */
ParentLocksAnchor::ParentLocksAnchor(LockManager* lock_manager,
                                     const std::string& filename) {
  auto slashPos(filename.find('/', 1));
  while (slashPos != std::string::npos) {
    auto dir(filename.substr(0, slashPos));
    // If some of these intermediate path does not exist, return false
    // Otherwise, grab the reader lock for dir and push it to the stack
    StatusOr<absl::Mutex*> path_lock_or(lock_manager->FetchLock(dir));
    if (!path_lock_or.ok()) {
      status_ = Status(google::protobuf::util::kNotFound,
                       "Lock for " + dir + " does not exist");
      return;
    }

    auto path_lock(path_lock_or.ValueOrDie());
    path_lock->ReaderLock();
    locks_.push(path_lock);
    slashPos = filename.find('/', slashPos + 1);
  }
  status_ = google::protobuf::util::Status::OK;
}

bool ParentLocksAnchor::ok() const { return status_.ok(); }

google::protobuf::util::Status ParentLocksAnchor::status() const {
  return status_;
}

size_t ParentLocksAnchor::lock_size() const { return locks_.size(); }

ParentLocksAnchor::~ParentLocksAnchor() {
  while (!locks_.empty()) {
    // Unlock the reader locks in a reverse sequence
    auto lock(locks_.top());
    locks_.pop();
    lock->ReaderUnlock();
  }
}

}  // namespace server
}  // namespace gfs
