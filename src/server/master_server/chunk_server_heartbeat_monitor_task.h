#ifndef GFS_SERVER_MASTER_SERVER_CHUNK_SERVER_HEARTBEAT_MONITOR_TASK_H_
#define GFS_SERVER_MASTER_SERVER_CHUNK_SERVER_HEARTBEAT_MONITOR_TASK_H_

#include <future>
#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "src/common/config_manager.h"
#include "src/common/protocol_client/chunk_server_control_service_client.h"

namespace gfs {

namespace server {

// This is a singleton class that represents the chunk server heartbeat
// monitoring task running periodically in the background. Uses a background
// thread to monitor the hearbeat of chunk servers registered with the
// chunkserver manager (i.e. active chunk servers). This allows us to know when
// a chunk server is down and to avoid using that chunk server for client
// requests.
// This periodically (as configured) checks the chunkservers currently
// registered with the chunk server manager, and sends heartbeat message to
// them. A chunkserver is declared to be unavailable when it doesn't respond
// after a number attempts. This server is then unregistered from the
// chunkserver manager, to avoid it being used for future allocations or client
// requests. If the chunkserver comes back up, it will report itself to the
// chunkserver manager and be re-registered. See
// master_chunk_server_manager_service_impl.h for how that is done.
// This task is started on master server startup and terminated on shutdown.
//
class ChunkServerHeartBeatMonitorTask {
 public:
  // The singleton instance
  static ChunkServerHeartBeatMonitorTask* GetInstance();

  // To prevent copying the ChunkServerHeartBeatMonitorTask
  ChunkServerHeartBeatMonitorTask(const ChunkServerHeartBeatMonitorTask&) =
      delete;

  void operator=(const ChunkServerHeartBeatMonitorTask&) = delete;

  // This triggers the task to start running on a separate background thread, if
  // not already running and returns without blocking the current thread. Uses
  // the config file to create config manager to read required configurations.
  // Does nothing if task is already started.
  void Start(const std::string& config_file);

  // This sends a signal to the thread running in background to terminate and
  // waits till termination is complete. We would only terminate when this is
  // called or when an exception occurs. The background thread checks for
  // termination before doing any intensive work. This terminate would be called
  // from a different thread and not the background thread. Does nothing if task
  // is already terminated.
  void Terminate();

 private:
  ChunkServerHeartBeatMonitorTask();

  // Calls terminate if task hasn't been terminated previously.
  ~ChunkServerHeartBeatMonitorTask();

  // The actual work running in the background thread. This periodically (as
  // configured) checks the chunkservers currently registered with the chunk
  // server manager, and send heartbeat message to them. A chunkserver is
  // declared to be unavailable when it doesn't respond after a number attempts.
  // This server is then unregistered from the chunkserver manager, to avoid it
  // being used for future allocations or client requests. If the chunkserver
  // comes back up, it will report itself to the chunkserver manager and be
  // re-registered. See master_chunk_server_manager_service_impl.h for how that
  // is done.
  void MonitorHeartBeat();

  // This checks if the termination promise has been set. The promise is used to
  // signal termination. Returns true if termination has been signalled, hence
  // thread should terminate, or false if termination signal isn't set.
  bool IsTerminateSignalled();

  // Return the protocol client for talking to the chunk server at
  // |server_address|. If the connection is already established, reuse the
  // connection. Otherwise, initialize and return a new protocol client
  // connecting to |server_address|.
  std::shared_ptr<gfs::service::ChunkServerControlServiceClient>
  GetOrCreateChunkServerControlClient(const std::string& server_address);

  // The background thread used for running this task.
  std::unique_ptr<std::thread> thread_;

  // This promise is used to signal termination to the background thread, when
  // terminate is called from another thread. Set the promise value when
  // terminate is called.
  std::unique_ptr<std::promise<void>> terminate_promise_;

  // Future used to check if the terminate promise has been set.
  std::unique_ptr<std::future<void>> terminate_future_;

  // Maps the server_address to the protocol client for communication.
  absl::flat_hash_map<
      std::string,
      std::shared_ptr<gfs::service::ChunkServerControlServiceClient>>
      chunk_server_control_clients_;

  std::unique_ptr<gfs::common::ConfigManager> config_mgr_;
};

}  // namespace server
}  // namespace gfs

#endif  // GFS_SERVER_MASTER_SERVER_CHUNK_SERVER_HEARTBEAT_MONITOR_TASK_H_
