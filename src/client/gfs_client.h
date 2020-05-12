#ifndef GFS_CLIENT_GFS_CLIENT_H_
#define GFS_CLIENT_GFS_CLIENT_H_

#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"

namespace gfs {
namespace client {

/* Data type for the return of read operation. This includes how many bytes 
 * have been read and a pointer of the buffer */
struct Data {
  int bytesRead;
  void* buffer; 
};

/* Let's plan to support the following open mode:
 * Read Mode | Write Mode | Create Mode | Is Directory
 * Possible combination: 
 * Create | Write - create and write to a file
 * Create | Is Directory - Create a direcotry 
 */
google::protobuf::util::Status open(const char* pathname, int flags);
google::protobuf::util::Status close(const char* pathname);

google::protobuf::util::StatusOr<Data> read(const char* pathname, 
                                            size_t offset, size_t nbytes);
google::protobuf::util::Status write(const char* path, 
                                     void* buffer, size_t offset, size_t nbytes);

google::protobuf::util::Status remove(const char* pathname); 

} // namespace client
} // namespace gfs 

#endif // GFS_CLIENT_GFS_CLIENT_H_
