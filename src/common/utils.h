#ifndef GFS_COMMON_UTILS_H_
#define GFS_COMMON_UTILS_H_

#include "absl/synchronization/mutex.h"
#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "parallel_hashmap/phmap.h"

namespace gfs {
namespace common {

// Define an intrinsically thread-safe flat hash map by parallel hashmap
// The default definition assumes no internal lock and requires users
// to pragmatically synchronize concurrent read and write to the parallel
// hashmap. By passing a lock type, e.g. absl::Mutex, the parallel
// hashmap is intrinsically thread safe.
//
// The example below follows the pattern defined in:
// https://greg7mdp.github.io/parallel-hashmap/
// https://github.com/greg7mdp/parallel-hashmap/blob/master/examples/bench.cc
template <class K, class V>
class thread_safe_flat_hash_map
    : public phmap::parallel_flat_hash_map<
          K, V, phmap::container_internal::hash_default_hash<K>,
          phmap::container_internal::hash_default_eq<K>,
          std::allocator<std::pair<const K, V>>, /*submaps=*/4, absl::Mutex> {
};

// Similar as above, define an intrinsically thread-safe flat hash set
template <class V>
class thread_safe_flat_hash_set
    : public phmap::parallel_flat_hash_set<
          V, phmap::container_internal::hash_default_hash<V>,
          phmap::container_internal::hash_default_eq<V>,
          std::allocator<V>, /*submaps=*/4, absl::Mutex> {
};

namespace utils {

// Convert a grpc::Status to protocol buffer's Status, so it's compatible with
// protocol buffer's StatusOr.
google::protobuf::util::Status ConvertGrpcStatusToProtobufStatus(
    const grpc::Status& status);

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

}  // namespace utils
}  // namespace common
}  // namespace gfs

#endif  // GFS_COMMON_UTILS_H_
