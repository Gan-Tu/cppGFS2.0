#include <iostream>
#include <memory>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "src/client/gfs_client.h"
#include "src/common/system_logger.h"

using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;

// Configurations
ABSL_FLAG(std::string, config_path, "data/config.yml", "/path/to/config.yml");
ABSL_FLAG(std::string, master_name, "master_server_01",
          "connect to the given master server, as defined in the config");
ABSL_FLAG(bool, use_docker_dns_server, false, "use docker's DNS server");

// CRUD Flag
ABSL_FLAG(std::string, mode, "",
          "CRUD mode (create, read, write, write_no_create, remove)");
ABSL_FLAG(std::string, filename, "", "/filename");

ABSL_FLAG(size_t, offset, 0, "Offset to read/write from; default: 0");
ABSL_FLAG(size_t, nbytes, 0, "number of bytes to read; default: 0");
ABSL_FLAG(std::string, data, "", "data to write; only applicable to writes");

unsigned int ParseOpenFlag(std::string mode) {
  unsigned int open_flag;
  const std::string open_mode = absl::GetFlag(FLAGS_mode);
  if (open_mode == "create") {
    open_flag = gfs::OpenFlag::Create;
  } else if (open_mode == "read") {
    open_flag = gfs::OpenFlag::Read;
  } else if (open_mode == "write") {
    open_flag = gfs::OpenFlag::Write | gfs::OpenFlag::Create;
  } else if (open_mode == "write_no_create") {
    open_flag = gfs::OpenFlag::Write;
  } else if (open_mode == "remove") {
    // remove is not implemented right now, but we still open with read mode
    open_flag = gfs::OpenFlag::Read;
  } else {
    LOG(FATAL) << "Unrecognized mode: " << absl::GetFlag(FLAGS_mode);
  }
  return open_flag;
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  gfs::common::SystemLogger::GetInstance().Initialize(
      /*program_name=*/argv[0], /*use_failure_signal_handler=*/false);

  const std::string mode = absl::GetFlag(FLAGS_mode);
  const std::string filename = absl::GetFlag(FLAGS_filename);
  if (filename.empty()) {
    LOG(ERROR) << "filename cannot be empty";
    return 1;
  } else if (mode.empty()) {
    LOG(ERROR) << "mode cannot be empty";
    return 1;
  }

  // Initialize GFS Client
  Status init_status = gfs::client::init_client(
      /*config_path=*/absl::GetFlag(FLAGS_config_path),
      /*master_name=*/absl::GetFlag(FLAGS_master_name),
      /*resolve_hostname=*/!absl::GetFlag(FLAGS_use_docker_dns_server));
  if (!init_status.ok()) {
    LOG(ERROR) << "Failed initializing GFS client: " << init_status;
    return 1;
  }

  // Open file
  Status open_status = gfs::client::open(filename.c_str(), ParseOpenFlag(mode));
  if (!open_status.ok()) {
    LOG(ERROR) << "Cannot open file: " << open_status;
    return 1;
  } else if (mode == "create") {
    LOG(INFO) << "File created " << filename;
    return 0;
  } else {
    LOG(INFO) << "File opened: " << filename << " in " << mode << " mode.";
  }

  const size_t offset = absl::GetFlag(FLAGS_offset);
  const size_t nbytes = absl::GetFlag(FLAGS_nbytes);
  const std::string data = absl::GetFlag(FLAGS_data);
  if (mode == "read") {
    StatusOr<gfs::client::Data> read_result =
        gfs::client::read(filename.c_str(), offset, nbytes);
    if (read_result.ok()) {
      gfs::client::Data data = read_result.value();
      LOG(INFO) << "Read " << data.bytes_read << " bytes of data from "
                << filename;
      LOG(INFO) << "Data read: '" << (char*)data.buffer << "'";
    } else {
      LOG(ERROR) << "Failed to read: " << read_result.status();
      return 1;
    }
  } else if (mode == "write" || mode == "write_no_create") {
    Status write_status = gfs::client::write(
        filename.c_str(), (void*)data.c_str(), offset, data.size());
    if (write_status.ok()) {
      LOG(INFO) << "Data written successfully";
    } else {
      LOG(ERROR) << "Failed to write: " << write_status;
      return 1;
    }
  } else if (mode == "remove") {
    Status remove_status = gfs::client::remove(filename.c_str());
    if (remove_status.ok()) {
      LOG(INFO) << "File removed";
    } else {
      LOG(ERROR) << "Failed to remove: " << remove_status;
      return 1;
    }
  }

  return 0;
}
