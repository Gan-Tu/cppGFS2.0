#include "src/client/gfs_client.h"
#include "src/common/system_logger.h"

int main(int argc, char** argv) {
  gfs::common::SystemLogger::GetInstance().Initialize(/*program_name=*/argv[0]);
  const std::string config_filename("data/config.yml");
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
    return 1;
  }
 
  auto create_foo_status(gfs::client::open("/foo", gfs::OpenFlag::Create));
  if (!create_foo_status.ok()) {
    LOG(ERROR) << "Open to create /foo failed with error: "
               << create_foo_status.error_message();
  } else {
    LOG(INFO) << "Open to create /foo succeeded";
  }

  return 0;
}
