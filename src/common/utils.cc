#include <openssl/md5.h>
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

grpc::Status ConvertProtobufStatusToGrpcStatus(
    const google::protobuf::util::Status& status) {
  grpc::StatusCode error_code;
  switch (status.error_code()) {
    case google::protobuf::util::error::OK:
      error_code = grpc::StatusCode::OK;
      break;
    case google::protobuf::util::error::CANCELLED:
      error_code = grpc::StatusCode::CANCELLED;
      break;
    case google::protobuf::util::error::INVALID_ARGUMENT:
      error_code = grpc::StatusCode::INVALID_ARGUMENT;
      break;
    case google::protobuf::util::error::DEADLINE_EXCEEDED:
      error_code = grpc::StatusCode::DEADLINE_EXCEEDED;
      break;
    case google::protobuf::util::error::NOT_FOUND:
      error_code = grpc::StatusCode::NOT_FOUND;
      break;
    case google::protobuf::util::error::ALREADY_EXISTS:
      error_code = grpc::StatusCode::ALREADY_EXISTS;
      break;
    case google::protobuf::util::error::PERMISSION_DENIED:
      error_code = grpc::StatusCode::PERMISSION_DENIED;
      break;
    case google::protobuf::util::error::UNAUTHENTICATED:
      error_code = grpc::StatusCode::UNAUTHENTICATED;
      break;
    case google::protobuf::util::error::RESOURCE_EXHAUSTED:
      error_code = grpc::StatusCode::RESOURCE_EXHAUSTED;
      break;
    case google::protobuf::util::error::FAILED_PRECONDITION:
      error_code = grpc::StatusCode::FAILED_PRECONDITION;
      break;
    case google::protobuf::util::error::ABORTED:
      error_code = grpc::StatusCode::ABORTED;
      break;
    case google::protobuf::util::error::OUT_OF_RANGE:
      error_code = grpc::StatusCode::OUT_OF_RANGE;
      break;
    case google::protobuf::util::error::UNIMPLEMENTED:
      error_code = grpc::StatusCode::UNIMPLEMENTED;
      break;
    case google::protobuf::util::error::INTERNAL:
      error_code = grpc::StatusCode::INTERNAL;
      break;
    case google::protobuf::util::error::UNAVAILABLE:
      error_code = grpc::StatusCode::UNAVAILABLE;
      break;
    case google::protobuf::util::error::DATA_LOSS:
      error_code = grpc::StatusCode::DATA_LOSS;
      break;
    default:
      error_code = grpc::StatusCode::UNKNOWN;
      break;
  }
  return grpc::Status(error_code, status.error_message());
}

google::protobuf::util::Status CheckFilenameValidity(
    const std::string& filename) {
  if (filename.empty()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::INVALID_ARGUMENT,
        "Empty filename is not allowed");
  }

  if (filename[0] != '/') {
    return google::protobuf::util::Status(
        google::protobuf::util::error::INVALID_ARGUMENT,
        "Relative path is not allowed");
  }

  if (filename.back() == '/') {
    return google::protobuf::util::Status(
        google::protobuf::util::error::INVALID_ARGUMENT,
        "Trailing slash is not allowed");
  }

  if (filename.find("//") != std::string::npos) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::INVALID_ARGUMENT,
        "Consecutive slash is not allowed");
  }

  return google::protobuf::util::Status::OK;
}

google::protobuf::util::Status ValidateConfigFile(const YAML::Node& node) {
  if (!node.IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT, "empty config");
  } else if (!node["servers"].IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT,
        "missing: servers");
  } else if (!node["network"].IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT,
        "missing: network");
  } else if (!node["disk"].IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT, "missing: disk");
  } else if (!node["timeout"].IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT,
        "missing: timeout");
  } else if (!node["servers"]["master_servers"].IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT,
        "missing: servers.master_servers");
  } else if (!node["servers"]["chunk_servers"].IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT,
        "missing: servers.chunk_servers");
  } else if (!node["network"]["dns_lookup_table"].IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT,
        "missing: network.dns_lookup_table");
  } else if (!node["disk"]["block_size_mb"].IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT,
        "missing: disk.block_size_mb");
  } else if (!node["disk"]["min_free_disk_space_mb"].IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT,
        "missing: disk.min_free_disk_space_mb");
  } else if (!node["disk"]["leveldb"].IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT,
        "missing: disk.leveldb");
  } else if (!node["timeout"]["grpc"].IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT,
        "missing: timeout.grpc");
  } else if (!node["timeout"]["lease"].IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT,
        "missing: timeout.lease");
  } else if (!node["timeout"]["heartbeat"].IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT,
        "missing: timeout.heartbeat");
  } else if (!node["timeout"]["client_cache"].IsDefined()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::Code::INVALID_ARGUMENT,
        "missing: timeout.client_cache");
  }

  std::vector<std::string> server_types = {"master_servers", "chunk_servers"};
  for (std::string& server_type : server_types) {
    for (size_t i = 0; i < node["servers"][server_type].size(); i++) {
      std::string server_name =
          node["servers"][server_type][i].as<std::string>();
      if (!node["network"][server_name].IsDefined()) {
        return google::protobuf::util::Status(
            google::protobuf::util::error::Code::INVALID_ARGUMENT,
            "missing: network definition for " + server_name);
      } else if (!node["network"][server_name]["hostname"].IsDefined()) {
        return google::protobuf::util::Status(
            google::protobuf::util::error::Code::INVALID_ARGUMENT,
            "missing: hostname for " + server_name);
      } else if (!node["network"][server_name]["port"].IsDefined()) {
        return google::protobuf::util::Status(
            google::protobuf::util::error::Code::INVALID_ARGUMENT,
            "missing: port for " + server_name);
      } else if (server_type == "chunk_servers" &&
                 !node["disk"]["leveldb"][server_name].IsDefined()) {
        return google::protobuf::util::Status(
            google::protobuf::util::error::Code::INVALID_ARGUMENT,
            "missing: leveldb database name for " + server_name);
      }
      std::string hostname =
          node["network"][server_name]["hostname"].as<std::string>();
      if (!node["network"]["dns_lookup_table"][hostname].IsDefined()) {
        return google::protobuf::util::Status(
            google::protobuf::util::error::Code::INVALID_ARGUMENT,
            "missing: dns lookup for " + hostname);
      }
    }
  }
  return google::protobuf::util::Status::OK;
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
