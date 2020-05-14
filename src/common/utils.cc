#include "src/common/utils.h"

#include "google/protobuf/stubs/status.h"
#include "grpcpp/grpcpp.h"

namespace gfs {
namespace common {
namespace utils {

google::protobuf::util::Status ConvertGrpcStatusToProtobufStatus(
    const grpc::Status& status) {
  google::protobuf::util::error::Code error_code;
  switch (status.error_code()) {
    case grpc::StatusCode::OK:
      error_code = google::protobuf::util::error::OK;
      break;
    case grpc::StatusCode::CANCELLED:
      error_code = google::protobuf::util::error::CANCELLED;
      break;
    case grpc::StatusCode::INVALID_ARGUMENT:
      error_code = google::protobuf::util::error::INVALID_ARGUMENT;
      break;
    case grpc::StatusCode::DEADLINE_EXCEEDED:
      error_code = google::protobuf::util::error::DEADLINE_EXCEEDED;
      break;
    case grpc::StatusCode::NOT_FOUND:
      error_code = google::protobuf::util::error::NOT_FOUND;
      break;
    case grpc::StatusCode::ALREADY_EXISTS:
      error_code = google::protobuf::util::error::ALREADY_EXISTS;
      break;
    case grpc::StatusCode::PERMISSION_DENIED:
      error_code = google::protobuf::util::error::PERMISSION_DENIED;
      break;
    case grpc::StatusCode::UNAUTHENTICATED:
      error_code = google::protobuf::util::error::UNAUTHENTICATED;
      break;
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
      error_code = google::protobuf::util::error::RESOURCE_EXHAUSTED;
      break;
    case grpc::StatusCode::FAILED_PRECONDITION:
      error_code = google::protobuf::util::error::FAILED_PRECONDITION;
      break;
    case grpc::StatusCode::ABORTED:
      error_code = google::protobuf::util::error::ABORTED;
      break;
    case grpc::StatusCode::OUT_OF_RANGE:
      error_code = google::protobuf::util::error::OUT_OF_RANGE;
      break;
    case grpc::StatusCode::UNIMPLEMENTED:
      error_code = google::protobuf::util::error::UNIMPLEMENTED;
      break;
    case grpc::StatusCode::INTERNAL:
      error_code = google::protobuf::util::error::INTERNAL;
      break;
    case grpc::StatusCode::UNAVAILABLE:
      error_code = google::protobuf::util::error::UNAVAILABLE;
      break;
    case grpc::StatusCode::DATA_LOSS:
      error_code = google::protobuf::util::error::DATA_LOSS;
      break;
    default:
      error_code = google::protobuf::util::error::UNKNOWN;
      break;
  }
  return google::protobuf::util::Status(error_code, status.error_message());
}

google::protobuf::util::Status CheckFilenameValidity(
  const std::string& filename) {
  if(filename.empty()) {
    return google::protobuf::util::Status(
             google::protobuf::util::error::INVALID_ARGUMENT,
             "Empty filename is not allowed");
  }

  if(filename[0]!='/') {
    return google::protobuf::util::Status(
             google::protobuf::util::error::INVALID_ARGUMENT,
             "Relative path is not allowed");
  }

  if(filename.back()=='/') {
    return google::protobuf::util::Status(
             google::protobuf::util::error::INVALID_ARGUMENT,
             "Trailing slash is not allowed");
  }

  if(filename.find("//")!=std::string::npos) {
    return google::protobuf::util::Status(
             google::protobuf::util::error::INVALID_ARGUMENT,
             "Consecutive slash is not allowed");
  }

  return google::protobuf::util::Status::OK;
}

}  // namespace utils
}  // namespace common
}  // namespace gfs
