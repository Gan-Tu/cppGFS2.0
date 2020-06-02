#ifndef GFS_COMMON_UTILS_H_
#define GFS_COMMON_UTILS_H_

#include "absl/synchronization/mutex.h"
#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "parallel_hashmap/phmap.h"
#include "yaml-cpp/yaml.h"

namespace gfs {
namespace common {

// Constant values that are frequently referred to in the impls
size_t const bytesPerMb = 1024 * 1024;

// Define an intrinsically thread-safe flat hash map by parallel hashmap
// The default definition assumes no internal lock and requires users
// to pragmatically synchronize concurrent read and write to the parallel
// hashmap. By passing a lock type, e.g. absl::Mutex, the parallel
// hashmap is intrinsically thread safe.
//
// The example below follows the pattern defined in:
// https://greg7mdp.github.io/parallel-hashmap/
// https://github.com/greg7mdp/parallel-hashmap/blob/master/examples/bench.cc
template <class K, class V,
          class Hash = phmap::container_internal::hash_default_hash<K>>
class thread_safe_flat_hash_map
    : public phmap::parallel_flat_hash_map<
          K, V, Hash, phmap::container_internal::hash_default_eq<K>,
          std::allocator<std::pair<const K, V>>, /*submaps=*/4, absl::Mutex> {};

// Define a customized parallel hashmap that builds on a default
// parallel_flat_hash_map to provide atomic operations such as "return the
// item if the key exists or return false". As the development move forward we
// realize that a pure thread-safe hashmap has limitations when it comes to
// sophisticated operations, and we'd really like to gain control of the
// locks on submaps if possible. For this purpose, we build a customized
// parallel hashmap with a control of the submap locks (the default one
// does not have locks, and we allocate locks manually).
//
// Example 1: You can try to access a value from a key by
//   auto try_value(hmap.TryGetValue(key));
//   if (!try_value.second) {
//     // Handle the non-existing case
//   }
//   // Handle the normal case
//
// Example 2: You can try to insert a value from a key by
//   auto try_value_insert(hmap.TryInsert(key, value));
//   if (!try_value.second) {
//     // Handle the case that the key already exists
//   }
//   // Handle the normal case
//
// The benefits of having these methods is that you avoid time-of-check-
// time-of-use type of bugs.
//
// Caveat: DO NOT call the build-in method / operator on top of this data
// structure if you are running in multi-threaded environment. This is because
// there is no protection on submaps provided by the base class, and you'd run
// into race condition if you do so. Always use the added methods below
template <class Key, class Value>
class parallel_hash_map : public phmap::parallel_flat_hash_map<Key, Value> {
 public:
  // Constructor initializes the lock array
  parallel_hash_map() {
    locks_ = std::vector<std::shared_ptr<absl::Mutex>>(
        this->subcnt(), std::shared_ptr<absl::Mutex>(new absl::Mutex()));
  }

  // Safely return if a key exists in the map
  bool Contains(const Key& key) {
    absl::Mutex* lock(FetchLock(key));
    absl::ReaderMutexLock lock_guard(lock);
    return this->contains(key);
  }

  // Return a pair where the second item corresponds to whether the key
  // exists. If the second item is true, then the first item corresponds
  // to the value of the item
  std::pair<Value, bool> TryGetValue(const Key& key) {
    absl::Mutex* lock(FetchLock(key));
    absl::ReaderMutexLock lock_guard(lock);
    if (!this->contains(key)) {
      return std::make_pair(Value(), false);
    }
    return std::make_pair(this->at(key), true);
  }

  // Return a bool where the second item corresponds to whether the insertion
  // takes place, i.e. this is a new key. If the key already exists,
  // do nothing and return false
  bool TryInsert(const Key& key, const Value& value) {
    absl::Mutex* lock(FetchLock(key));
    absl::WriterMutexLock lock_guard(lock);
    if (this->contains(key)) {
      return false;
    }
    (*this)[key] = value;
    return true;
  }

  // Set value for a key, regardless of whether the key exists of not
  void SetValue(const Key& key, const Value& value) {
    absl::Mutex* lock(FetchLock(key));
    absl::WriterMutexLock lock_guard(lock);
    (*this)[key] = value;
  }

 private:
  std::vector<std::shared_ptr<absl::Mutex>> locks_;
  absl::Mutex* FetchLock(const Key& key) {
    // Note that the "hash" method is somehow non-const (for no good
    // reason). An update of the library may resolve this, but for now
    // let's live with what we have. This causes methods defined above
    // to be non-const.
    size_t hash_val = this->hash(key);
    size_t idx = this->subidx(hash_val);
    return locks_[idx].get();
  }
};

// Similar as above, define an intrinsically thread-safe flat hash set
template <class V, class Hash = phmap::container_internal::hash_default_hash<V>>
class thread_safe_flat_hash_set
    : public phmap::parallel_flat_hash_set<
          V, Hash, phmap::container_internal::hash_default_eq<V>,
          std::allocator<V>, /*submaps=*/4, absl::Mutex> {};

namespace utils {

// Convert a grpc::Status to protocol buffer's Status, so it's compatible with
// protocol buffer's StatusOr.
google::protobuf::util::Status ConvertGrpcStatusToProtobufStatus(
    const grpc::Status& status);

// Convert a protocol buffer's Status to grpc::Status. This occurs typically
// on the server side when converting an internal state represented by 
// protocol buffer's Status or StatusOr to a grpc::Status would would be 
// sent to the client
grpc::Status ConvertProtobufStatusToGrpcStatus(
    const google::protobuf::util::Status& status);

// Check the validity of a given Filename. By validity we mean that a pathname
// must follow the format /comp1/comp2/.../compn. Specifically a path should
// have the following properties
//
// 1. Cannot be empty
// 2. Cannot be relative, i.e. not starting with a root "/"
// 3. Cannot have trailing slash. (this might seem to restrictive but would
// offer convenience).
// 4. Cannot have consecutive slash.
// [TBD] other constraints if applicable
google::protobuf::util::Status CheckFilenameValidity(
    const std::string& filename);

// Return a StatusOr with |value| if the |status| is OK; otherwise, convert the
// gRPC |status| to protobuf status, so it can be used in the returned StatusOr.
template <typename T>
inline google::protobuf::util::StatusOr<T> ReturnStatusOrFromGrpcStatus(
    T value, grpc::Status status) {
  if (status.ok()) {
    return value;
  } else {
    return ConvertGrpcStatusToProtobufStatus(status);
  }
}

// Validate a parsed configuration YAML node for required fields/schema.
// Return Status::OK, if successful; otherwise, any validation error.
google::protobuf::util::Status ValidateConfigFile(const YAML::Node& node);

}  // namespace utils
}  // namespace common
}  // namespace gfs

#endif  // GFS_COMMON_UTILS_H_
