#include <thread>
#include <vector>
#include "src/client/gfs_client.h"
#include "src/common/system_logger.h"

// Initialization, including setting up system logging, config file and master
// name. This function may be refactored into a utils call later
void init() {
  const std::string config_filename("file_creation_test/config.yaml");
  const std::string master_name("master_server_01");
  // Currently data/config.yml does not have real hostname so we set 
  // resolve_hostname to enable a manual lookup
  const bool resolve_hostname(true);

  LOG(INFO) << "Calling init_client...";
  auto init_status(gfs::client::init_client(config_filename, master_name, 
                                            resolve_hostname));
  if (!init_status.ok()) {
    LOG(ERROR) << "Client initialiation failed with error: " 
               << init_status.error_message();
    exit(-1);
  }
}

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
  gfs::common::SystemLogger::GetInstance().Initialize(argv[0]);
  
  // Simple test case, create a single file
  singleFileCreation("/foo"); 

  // Conrurrent file creation
  concurrentFileCreation(50);
  return 0;
}
