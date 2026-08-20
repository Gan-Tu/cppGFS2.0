#ifndef GFS_COMMON_PROTOS_COMMON_H_
#define GFS_COMMON_PROTOS_COMMON_H_

#include <functional>
#include <string>

#include "src/protos/chunk_server.pb.h"

// Shared helpers for the common wire protos. Location identity in particular
// is compared in several places (the master's chunk server registry, the
// client excluding the primary from the forward list, the primary excluding
// itself); defining equality once keeps those comparisons from drifting.
namespace protos {

inline bool operator==(const protos::ChunkServerLocation& lhs,
                       const protos::ChunkServerLocation& rhs) {
  return lhs.server_hostname() == rhs.server_hostname() &&
         lhs.server_port() == rhs.server_port();
}

inline bool operator!=(const protos::ChunkServerLocation& lhs,
                       const protos::ChunkServerLocation& rhs) {
  return !(lhs == rhs);
}

inline bool operator==(const protos::ChunkServer& lhs,
                       const protos::ChunkServer& rhs) {
  return lhs.location() == rhs.location();
}

}  // namespace protos

namespace gfs {
namespace common {

// For generating the hash value of a ChunkServerLocation, for use in hash
// sets/maps where the location is the chunk server's unique identifier.
class ChunkServerLocationHash {
 public:
  std::size_t operator()(const protos::ChunkServerLocation& location) const
      noexcept {
    std::size_t h1 = std::hash<std::string>{}(location.server_hostname());
    std::size_t h2 = std::hash<uint32_t>{}(location.server_port());
    return h1 ^ (h2 << 1);
  }
};

}  // namespace common
}  // namespace gfs

#endif  // GFS_COMMON_PROTOS_COMMON_H_
