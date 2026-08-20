#include "src/common/utils.h"

#include "absl/crc/crc32c.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "grpcpp/grpcpp.h"

namespace gfs {
namespace common {
namespace utils {

using absl::AbortedError;
using absl::AlreadyExistsError;
using absl::CancelledError;
using absl::DataLossError;
using absl::DeadlineExceededError;
using absl::FailedPreconditionError;
using absl::InternalError;
using absl::InvalidArgumentError;
using absl::IsAborted;
using absl::IsAlreadyExists;
using absl::IsCancelled;
using absl::IsDataLoss;
using absl::IsDeadlineExceeded;
using absl::IsFailedPrecondition;
using absl::IsInternal;
using absl::IsInvalidArgument;
using absl::IsNotFound;
using absl::IsOutOfRange;
using absl::IsPermissionDenied;
using absl::IsResourceExhausted;
using absl::IsUnauthenticated;
using absl::IsUnavailable;
using absl::IsUnimplemented;
using absl::NotFoundError;
using absl::OkStatus;
using absl::OutOfRangeError;
using absl::PermissionDeniedError;
using absl::ResourceExhaustedError;
using absl::UnauthenticatedError;
using absl::UnavailableError;
using absl::UnimplementedError;
using absl::UnknownError;

absl::Status ConvertGrpcStatusToProtobufStatus(
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
    const absl::Status& status) {
  const std::string msg = std::string(status.message());
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

absl::Status CheckFilenameValidity(
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

absl::Status ValidateConfigFile(const YAML::Node& node) {
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
  // Also encode the data length in the identifier to further reduce the
  // (already small) chance that two distinct in-flight payloads collide in
  // the chunk servers' data cache.
  return absl::StrFormat(
      "%08x-%x", static_cast<uint32_t>(absl::ComputeCrc32c(data)), data.size());
}

uint32_t calc_crc32c(const char* data, size_t length) {
  return static_cast<uint32_t>(
      absl::ComputeCrc32c(absl::string_view(data, length)));
}

}  // namespace utils
}  // namespace common
}  // namespace gfs
