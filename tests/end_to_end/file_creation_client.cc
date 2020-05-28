#include <thread>
#include <vector>
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "src/client/gfs_client.h"
#include "src/common/system_logger.h"

// TODO(Xi): Refactor the following and init() into a util.h as many tests
// may reuse this piece
ABSL_FLAG(std::string, config_path, "file_creation_test/config.yaml", 
          "/path/to/config.yml");
ABSL_FLAG(std::string, master_name, "master_server_01",
          "master name for this client to talk to");

// Init function to initialize the client
void init() {
  const std::string config_path = absl::GetFlag(FLAGS_config_path);
  const std::string master_name = absl::GetFlag(FLAGS_master_name);
  const bool resolve_hostname(true);

  LOG(INFO) << "Calling init_client...";
  auto init_status(gfs::client::init_client(config_path, master_name, 
                                            resolve_hostname));
  if (!init_status.ok()) {
    LOG(ERROR) << "Client initialiation failed with error: " 
               << init_status.error_message();
    exit(-1);
  }
}

// Create a single file
void singleFileCreation(const std::string& filename) {
  init();
  auto create_foo_status(gfs::client::open(filename.c_str(), 
                                           gfs::OpenFlag::Create));
  if (!create_foo_status.ok()) {
    LOG(ERROR) << "Open to create " + filename + " failed with error: "
               << create_foo_status.error_message();
    exit(1);
  } else {
    LOG(INFO) << "Open to create " + filename +  " succeeded";
  }
}

// Launch multiple threads and have each to create a file concurrently
void concurrentFileCreation(const unsigned int num_of_threads) {
  std::vector<std::thread> threads;
  for (unsigned int i = 0; i < num_of_threads; i++) {
    threads.push_back(std::thread(singleFileCreation, "/foo" 
                                      + std::to_string(i))); 
  }
  
  for (auto& t : threads) {
    t.join();
  }
}

int main(int argc, char** argv) {
  // Initialize system log and parse command line options
  gfs::common::SystemLogger::GetInstance().Initialize(argv[0]);
  absl::ParseCommandLine(argc, argv);
  
  // Simple test case, create a single file
  singleFileCreation("/foo"); 

  // Conrurrent file creation
  concurrentFileCreation(50);
  return 0;
}
