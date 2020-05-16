#include "src/server/master_server/lock_manager.h"

#include <thread>

#include "absl/hash/hash.h"

using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;

namespace gfs {
namespace server {

bool LockManager::Exist(const std::string& filename) const {
  return file_path_locks_.contains(filename);
}

google::protobuf::util::StatusOr<absl::Mutex*> LockManager::CreateLock(
    const std::string& filename) {
  // The emplace method returns a pair, where the first field corresponds 
  // to the item for this key, and the second field is true if this 
  // call successfully inserts an item, and is false if there
  // is already an item existed for the given key. We use the second field
  // to detect whether a lock has been created already (or there is another
  // racing thread that inserted one before this call). 
  auto new_lock(std::shared_ptr<absl::Mutex>(new absl::Mutex()));
  auto lock_and_if_took_place(file_path_locks_.emplace(filename,new_lock));
  bool has_insert_taken_place(lock_and_if_took_place.second);
  if (!has_insert_taken_place) {
    return Status(google::protobuf::util::error::ALREADY_EXISTS,
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
    const std::string& filename) const {
  // Again, note that we do not fully support crazy schemes like 
  // concurrent create and delete locks. Otherwise, the following pattern
  // may seem to have a time-of-check-time-of-use bug, namely the lock
  // got removed in the two lines between. Furthermore, it may seem
  // tempting to just use the operator [] and a null check to 
  // accomplish this, but the problem is that it makes this function
  // invasive and can insert a default item underneath, this can 
  // interact with the CreateLock function above and cause an empty
  // share_ptr ended up for a lock, which is obviously not great. 
  if (!Exist(filename)) {
    return Status(google::protobuf::util::error::NOT_FOUND,
                  "Lock does not exist for " + filename);
  }
  return file_path_locks_.at(filename).get();
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
      status_ = Status(google::protobuf::util::error::NOT_FOUND,
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
