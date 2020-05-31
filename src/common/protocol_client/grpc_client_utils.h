#ifndef GFS_COMMON_PROTOCOL_CLIENT_GRPC_CLIENT_UTILS_H_
#define GFS_COMMON_PROTOCOL_CLIENT_GRPC_CLIENT_UTILS_H_

#include "grpcpp/grpcpp.h"
#include "src/common/config_manager.h"

namespace gfs {
namespace common {

// Helper function to set deadline for a client context object using the 
// timeout configuration obtained from the config manager
void SetClientContextDeadline(grpc::ClientContext& client_context,
                              ConfigManager* config_manager); 

} // namespace common
} // namespace gfs 

#endif // GFS_COMMON_PROTOCOL_CLIENT_GRPC_CLIENT_UTILS_H_
