#include "src/client/gfs_client.h"

#include "src/client/client_impl.h"
#include "src/common/utils.h"

namespace gfs {
namespace client {

// A per-thread object that serves as the interface for the impl code to access
// and issue calls to internal units such as cache manager, service clients to
// master and chunk servers
static thread_local ClientImpl* client_impl_ = nullptr;

// Validate a given open flag, currently we only support four modes, namely
// read, write, create and create | write
bool ValidateOpenFlag(unsigned int flags) {
  return flags == OpenFlag::Read || flags == OpenFlag::Write ||
         flags == OpenFlag::Create ||
         flags == (OpenFlag::Write | OpenFlag::Create);
}

google::protobuf::util::Status init_client(const std::string& config_filename,
                                           const std::string& master_name,
                                           const bool resolve_hostname) {
  if (client_impl_) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::ALREADY_EXISTS,
        "ClientImpl has already been initialized successfully");
  }

  // Instantiate the client impl object
  auto client_impl_or(ClientImpl::ConstructClientImpl(
      config_filename, master_name, resolve_hostname));
  if (!client_impl_or.ok()) {
    return client_impl_or.status();
  }

  client_impl_ = client_impl_or.ValueOrDie();
  return google::protobuf::util::Status::OK;
}

void reset_client() {
  delete client_impl_;
  client_impl_ = nullptr;
}

google::protobuf::util::Status open(const char* filename, unsigned int flags) {
  // Make sure that init_client is called as a pre-condition
  if (!client_impl_) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::FAILED_PRECONDITION,
        "init_client must be called before calling client APIs");
  }

  // Check and validate the flags, e.g. it doesn't make sense to
  // open with both read and write flags
  if (!ValidateOpenFlag(flags)) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::INVALID_ARGUMENT,
        "Invalid open flag : " + std::to_string(flags));
  }

  // Check and validate the filename
  auto check_filename_status(common::utils::CheckFilenameValidity(filename));
  if (!check_filename_status.ok()) {
    return check_filename_status;
  }

  // Creation mode, this is true when flags = OpenFlag::Create or
  // Open::Create | Open::Write
  if (flags == OpenFlag::Create) {
    auto create_status(client_impl_->CreateFile(filename));
    if (!create_status.ok()) {
      return create_status;
    }
  } else if (flags == (OpenFlag::Create | OpenFlag::Write)) {
    auto create_status(client_impl_->CreateFile(filename));
    if (!create_status.ok() &&
        create_status.code() != google::protobuf::util::error::ALREADY_EXISTS) {
      return create_status;
    }
  }

  // Note that real GFS does permission check when a read or write open flags
  // is passed to the open call. Beause we do not worry about permission
  // in this work, we simply do nothing here if a read / write flag is given
  return google::protobuf::util::Status::OK;
}

google::protobuf::util::Status close(const char* filename) {
  return google::protobuf::util::Status::OK;
}

google::protobuf::util::StatusOr<Data> read(const char* filename, size_t offset,
                                            size_t nbytes) {
  // Make sure that init_client is called as a pre-condition
  if (!client_impl_) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::FAILED_PRECONDITION,
        "init_client must be called before calling client APIs");
  }

  // Check and validate the filename
  auto check_filename_status(common::utils::CheckFilenameValidity(filename));
  if (!check_filename_status.ok()) {
    return check_filename_status;
  }

  auto read_data_or(client_impl_->ReadFile(filename, offset, nbytes));
  // Return error status if read fails
  if (!read_data_or.ok()) {
    return read_data_or.status();
  }

  return Data(read_data_or.ValueOrDie().first,
              read_data_or.ValueOrDie().second);
}

google::protobuf::util::Status write(const char* filename, void* buffer,
                                     size_t offset, size_t nbytes) {
  // Make sure that init_client is called as a pre-condition
  if (!client_impl_) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::FAILED_PRECONDITION,
        "init_client must be called before calling client APIs");
  }

  // Check and validate the filename
  auto check_filename_status(common::utils::CheckFilenameValidity(filename));
  if (!check_filename_status.ok()) {
    return check_filename_status;
  }

  auto write_data_status(
      client_impl_->WriteFile(filename, buffer, offset, nbytes));
  return write_data_status;
}

google::protobuf::util::Status remove(const char* filename) {
  return google::protobuf::util::Status::OK;
}

}  // namespace client
}  // namespace gfs
