#ifndef CLIENT_GFS_CLIENT_H
#define CLIENT_GFS_CLIENT_H

#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"

namespace gfs {
namespace client {

// Use the Status and Statusor objects provided by protobuf
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;

/* Data type for the return of read operation. This includes how many bytes have been
 * read and a pointer of the buffer */
struct Data {
   int bytesRead;
   void* buffer; 
};

// Provide an interface function to let user to initialize client and configure
// parameters such as master's url. A user should call this function before
// performing any of the following functions as initializations are needed
void configClient(const char* masterUrl);

Status create(const char* pathname);
Status remove(const char* pathname); 
// TODO: Add open mode if needed
Status open(const char* pathname);
Status close(const char* pathname);

StatusOr<Data> read(const char* pathname, size_t offset, size_t nbytes);
Status write(const char* path, void* buffer, size_t offset, size_t nbytes);

} /* namespace client */
} /* namespace gfs */

#endif /* CLIENT_GFS_CLIENT_H */
