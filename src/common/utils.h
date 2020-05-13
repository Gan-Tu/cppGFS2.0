#ifndef GFS_COMMON_UTILS_H_
#define GFS_COMMON_UTILS_H_

#include "google/protobuf/stubs/status.h"
#include "grpcpp/grpcpp.h"

namespace gfs {
namespace common {
namespace utils {

// Convert a grpc::Status to protocol buffer's Status, so it's compatible with
// protocol buffer's StatusOr.
google::protobuf::util::Status ConvertGrpcStatusToProtobufStatus(
    const grpc::Status& status);

// Check the validity of a given pathanem. By validity we mean that a pathname must
// follow the format /comp1/comp2/.../compn. Specifically a path should have the
// following properties
// 
// 1. Cannot be empty
// 2. Cannot be relative, i.e. not starting with a root "/"
// 3. Cannot have trailing slash. (this might seem to restrictive but would offer
// convenience).
// 4. Cannot have consecutive slash.
google::protobuf::util::Status checkPathnameValidity(const std::string& pathname); 

}  // namespace utils
}  // namespace common

}  // namespace gfs

#endif  // GFS_COMMON_UTILS_H_
