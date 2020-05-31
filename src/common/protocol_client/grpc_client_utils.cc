#include "absl/time/time.h"
#include "src/common/protocol_client/grpc_client_utils.h"

namespace gfs {
namespace common {

void SetClientContextDeadline(grpc::ClientContext& client_context,
                              ConfigManager* config_manager) {
  absl::Duration grpc_timeout(config_manager->GetGrpcDeadline());
  client_context.set_deadline(std::chrono::system_clock::now() + 
      std::chrono::milliseconds(absl::ToInt64Milliseconds(grpc_timeout)));
}

} // namespace common
} // namespace gfs
