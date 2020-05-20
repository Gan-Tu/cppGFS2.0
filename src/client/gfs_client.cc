#include "src/client/gfs_client.h"

namespace gfs {
namespace client {

google::protobuf::util::Status open(const char* pathname, unsigned int flags) {
  // Step 1. Check and validate the flags, e.g. it doesn't make sense to 
  // open with both read and write flags

  // Creation mode
  if (flags | OpenFlag::Create) {
  }

  // Read mode
  if (flags | OpenFlag::Read) {
  }

  // Write mode
  if (flags | OpenFlag::Read) {
  }

  return google::protobuf::util::Status::OK;
}

google::protobuf::util::Status close(const char* pathname) {
  return google::protobuf::util::Status::OK;
}

google::protobuf::util::StatusOr<Data> read(const char* pathname, size_t offset,
                                            size_t nbytes) {
  return Data();
}

google::protobuf::util::Status write(const char* path, void* buffer,
                                     size_t offset, size_t nbytes) {
  return google::protobuf::util::Status::OK;
}

google::protobuf::util::Status remove(const char* pathname) {
  return google::protobuf::util::Status::OK;
}

}  // namespace client
}  // namespace gfs
