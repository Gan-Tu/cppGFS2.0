#include "src/common/utils.h"

#include <openssl/md5.h>

#include "google/protobuf/stubs/status.h"
#include "grpcpp/grpcpp.h"

namespace gfs {
namespace common {
namespace utils {

using google::protobuf::util::AbortedError;
using google::protobuf::util::AlreadyExistsError;
using google::protobuf::util::CancelledError;
using google::protobuf::util::DataLossError;
using google::protobuf::util::DeadlineExceededError;
using google::protobuf::util::FailedPreconditionError;
using google::protobuf::util::InternalError;
using google::protobuf::util::InvalidArgumentError;
using google::protobuf::util::IsAborted;
using google::protobuf::util::IsAlreadyExists;
using google::protobuf::util::IsCancelled;
using google::protobuf::util::IsDataLoss;
using google::protobuf::util::IsDeadlineExceeded;
using google::protobuf::util::IsFailedPrecondition;
using google::protobuf::util::IsInternal;
using google::protobuf::util::IsInvalidArgument;
using google::protobuf::util::IsNotFound;
using google::protobuf::util::IsOutOfRange;
using google::protobuf::util::IsPermissionDenied;
using google::protobuf::util::IsResourceExhausted;
using google::protobuf::util::IsUnauthenticated;
using google::protobuf::util::IsUnavailable;
using google::protobuf::util::IsUnimplemented;
using google::protobuf::util::NotFoundError;
using google::protobuf::util::OkStatus;
using google::protobuf::util::OutOfRangeError;
using google::protobuf::util::PermissionDeniedError;
using google::protobuf::util::ResourceExhaustedError;
using google::protobuf::util::UnauthenticatedError;
using google::protobuf::util::UnavailableError;
using google::protobuf::util::UnimplementedError;
using google::protobuf::util::UnknownError;

google::protobuf::util::Status ConvertGrpcStatusToProtobufStatus(
    const grpc::Status& status) {
  const auto msg = status.error_message();
  switch (status.error_code()) {
    case grpc::StatusCode::OK:
      return OkStatus();
    case grpc::StatusCode::CANCELLED:
      return CancelledError(msg);
    case grpc::StatusCode::INVALID_ARGUMENT:
      return InvalidArgumentError(msg);
    case grpc::StatusCode::DEADLINE_EXCEEDED:
      return DeadlineExceededError(msg);
    case grpc::StatusCode::NOT_FOUND:
      return NotFoundError(msg);
    case grpc::StatusCode::ALREADY_EXISTS:
      return AlreadyExistsError(msg);
    case grpc::StatusCode::PERMISSION_DENIED:
      return PermissionDeniedError(msg);
    case grpc::StatusCode::UNAUTHENTICATED:
      return UnauthenticatedError(msg);
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
      return ResourceExhaustedError(msg);
    case grpc::StatusCode::FAILED_PRECONDITION:
      return FailedPreconditionError(msg);
    case grpc::StatusCode::ABORTED:
      return AbortedError(msg);
    case grpc::StatusCode::OUT_OF_RANGE:
      return OutOfRangeError(msg);
    case grpc::StatusCode::UNIMPLEMENTED:
      return UnimplementedError(msg);
    case grpc::StatusCode::INTERNAL:
      return InternalError(msg);
    case grpc::StatusCode::UNAVAILABLE:
      return UnavailableError(msg);
    case grpc::StatusCode::DATA_LOSS:
      return DataLossError(msg);
  }
  return InternalError("Unknown error message code");
}

grpc::Status ConvertProtobufStatusToGrpcStatus(
    const google::protobuf::util::Status& status) {
  const std::string msg = status.message().as_string();
  if (status.ok()) {
    return grpc::Status(grpc::StatusCode::OK, msg);
  }
  if (IsCancelled(status)) {
    return grpc::Status(grpc::StatusCode::CANCELLED, msg);
  }
  if (IsInvalidArgument(status)) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, msg);
  }
  if (IsDeadlineExceeded(status)) {
    return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, msg);
  }
  if (IsNotFound(status)) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, msg);
  }
  if (IsAlreadyExists(status)) {
    return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, msg);
  }
  if (IsPermissionDenied(status)) {
    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, msg);
  }
  if (IsUnauthenticated(status)) {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, msg);
  }
  if (IsResourceExhausted(status)) {
    return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, msg);
  }
  if (IsFailedPrecondition(status)) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, msg);
  }
  if (IsAborted(status)) {
    return grpc::Status(grpc::StatusCode::ABORTED, msg);
  }
  if (IsOutOfRange(status)) {
    return grpc::Status(grpc::StatusCode::OUT_OF_RANGE, msg);
  }
  if (IsUnimplemented(status)) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, msg);
  }
  if (IsInternal(status)) {
    return grpc::Status(grpc::StatusCode::INTERNAL, msg);
  }
  if (IsUnavailable(status)) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, msg);
  }
  if (IsDataLoss(status)) {
    return grpc::Status(grpc::StatusCode::DATA_LOSS, msg);
  }
  return grpc::Status(grpc::StatusCode::UNKNOWN, "Unrecognized status code");
}

google::protobuf::util::Status CheckFilenameValidity(
    const std::string& filename) {
  if (filename.empty()) {
    return InvalidArgumentError("Empty filename is not allowed");
  }

  if (filename[0] != '/') {
    return InvalidArgumentError("Relative path is not allowed");
  }

  if (filename.back() == '/') {
    return InvalidArgumentError("Trailing slash is not allowed");
  }

  if (filename.find("//") != std::string::npos) {
    return InvalidArgumentError("Consecutive slash is not allowed");
  }

  return OkStatus();
}

google::protobuf::util::Status ValidateConfigFile(const YAML::Node& node) {
  if (!node.IsDefined()) {
    return InvalidArgumentError("empty config");
  } else if (!node["servers"].IsDefined()) {
    return InvalidArgumentError("missing: servers");
  } else if (!node["network"].IsDefined()) {
    return InvalidArgumentError("missing: network");
  } else if (!node["disk"].IsDefined()) {
    return InvalidArgumentError("missing: disk");
  } else if (!node["timeout"].IsDefined()) {
    return InvalidArgumentError("missing: timeout");
  } else if (!node["servers"]["master_servers"].IsDefined()) {
    return InvalidArgumentError("missing: servers.master_servers");
  } else if (!node["servers"]["chunk_servers"].IsDefined()) {
    return InvalidArgumentError("missing: servers.chunk_servers");
  } else if (!node["network"]["dns_lookup_table"].IsDefined()) {
    return InvalidArgumentError("missing: network.dns_lookup_table");
  } else if (!node["disk"]["block_size_mb"].IsDefined()) {
    return InvalidArgumentError("missing: disk.block_size_mb");
  } else if (!node["disk"]["min_free_disk_space_mb"].IsDefined()) {
    return InvalidArgumentError("missing: disk.min_free_disk_space_mb");
  } else if (!node["disk"]["leveldb"].IsDefined()) {
    return InvalidArgumentError("missing: disk.leveldb");
  } else if (!node["timeout"]["grpc"].IsDefined()) {
    return InvalidArgumentError("missing: timeout.grpc");
  } else if (!node["timeout"]["lease"].IsDefined()) {
    return InvalidArgumentError("missing: timeout.lease");
  } else if (!node["timeout"]["heartbeat"].IsDefined()) {
    return InvalidArgumentError("missing: timeout.heartbeat");
  } else if (!node["timeout"]["client_cache"].IsDefined()) {
    return InvalidArgumentError("missing: timeout.client_cache");
  }

  std::vector<std::string> server_types = {"master_servers", "chunk_servers"};
  for (std::string& server_type : server_types) {
    for (size_t i = 0; i < node["servers"][server_type].size(); i++) {
      std::string server_name =
          node["servers"][server_type][i].as<std::string>();
      if (!node["network"][server_name].IsDefined()) {
        return InvalidArgumentError("missing: network definition for " +
                                    server_name);
      } else if (!node["network"][server_name]["hostname"].IsDefined()) {
        return InvalidArgumentError("missing: hostname for " + server_name);
      } else if (!node["network"][server_name]["port"].IsDefined()) {
        return InvalidArgumentError("missing: port for " + server_name);
      } else if (server_type == "chunk_servers" &&
                 !node["disk"]["leveldb"][server_name].IsDefined()) {
        return InvalidArgumentError("missing: leveldb database name for " +
                                    server_name);
      }
      std::string hostname =
          node["network"][server_name]["hostname"].as<std::string>();
      if (!node["network"]["dns_lookup_table"][hostname].IsDefined()) {
        return InvalidArgumentError("missing: dns lookup for " + hostname);
      }
    }
  }
  return OkStatus();
}

const std::string calc_checksum(const std::string& data) {
  std::string checksum_string(MD5_DIGEST_LENGTH, ' ');
  // The casting below is necessary to fit with openssl's MD5 function
  // signature, which is a pretty ugly interface
  MD5((const unsigned char*)&data[0], data.size(),
      (unsigned char*)&checksum_string[0]);
  return checksum_string;
}

}  // namespace utils
}  // namespace common
}  // namespace gfs
