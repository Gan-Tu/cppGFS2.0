#ifndef GFS_CLIENT_GFS_CLIENT_H_
#define GFS_CLIENT_GFS_CLIENT_H_

#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"

namespace gfs {

// Define open flag type using enum with bitmask
enum OpenFlag {
  Read = 0x1,
  Write = 0x2,
  Create = 0x4
};

namespace client {

// Data type for the return of read operation. This includes how many bytes
// have been read and a pointer of the buffer
struct Data {
  int bytesRead;
  void* buffer;
};

// Initialization function that a user should call before using any of the 
// APIs below. This function takes a configuration file path, a master node
// name, and an optional bool to indicate whether to resolve host name. 
// It will initialize the internal units (e.g. cacheManager) according to the 
// configuration.  
google::protobuf::util::Status init_client(const std::string& config_filename,
    const std::string& master_name, const bool resolve_hostname = false);

// We support the following mode: Read Mode | Write Mode | Create Mode
// when opening a file. The only possible combination is Write | Create. 
// The flags is an unsigned integer (similar to the open system call) which
// is essentially a bit set representing a combination of OpenFlags. 
// For read mode, flags = gfs::OpenFlags::Read 
// For write mode, flags = gfs::OpenFlags::Write
// For create mode, flags = gfs::OpenFlags::Create
// For write + create mode, flags = (gfs::OpenFlags::Write |
//                                   gfs::OpenFlags::Create)
// You should not use integer literals when setting these flags as they are
// error prone. Simply use the enum names shown as above. 
google::protobuf::util::Status open(const char* filename, unsigned int flags);

google::protobuf::util::Status close(const char* filename);

google::protobuf::util::StatusOr<Data> read(
    const char* filename, size_t offset, size_t nbytes);

google::protobuf::util::Status write(const char* path, void* buffer,
    size_t offset, size_t nbytes);

google::protobuf::util::Status remove(const char* filename);

}  // namespace client
}  // namespace gfs

#endif  // GFS_CLIENT_GFS_CLIENT_H_
