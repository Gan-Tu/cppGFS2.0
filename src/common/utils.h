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

}  // namespace utils
}  // namespace common
}  // namespace gfs

#endif  // GFS_COMMON_UTILS_H_