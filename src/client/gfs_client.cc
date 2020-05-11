#include "gfs_client.h"

using namespace gfs;
using namespace client;

namespace gfs {
namespace client {

// This function should initialize objects such as the ClientStubs for between the
// client and the master, and client and the chunk server. The param list needs to be
// extended to include chunk server's information as well. 
void configClient(const char* masterUrl) {
}

Status create(const char* pathname) {
   return Status();
}

Status remove(const char* pathname) {
   return Status();
}

Status open(const char* pathname) {
   return Status();
}

Status close(const char* pathname) {
   return Status();
}

StatusOr<Data> read(const char* pathname, size_t offset, size_t nbytes) {
   return StatusOr<Data>();
}

Status write(const char* path, void* buffer, size_t offset, size_t nbytes) {
   return Status();
}

} /* namespace client */
} /* namespace gfs */
