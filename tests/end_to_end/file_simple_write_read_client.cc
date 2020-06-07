#include <thread>
#include <vector>
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "src/client/gfs_client.h"
#include "src/common/system_logger.h"

ABSL_FLAG(std::string, config_path, "file_simple_write_read_test/config.yaml", 
          "/path/to/config.yml");
ABSL_FLAG(std::string, master_name, "master_server_01",
          "master name for this client to talk to");
// We have binary mode for this client, if this flag is set true, then it issues
// write based on the data defined below
ABSL_FLAG(bool, is_writer, false, "whether to issue write to files");

// Initialize data that the writer will use to write and reader will use to verify
// the content
const std::vector<std::string> InitializeData() {
  std::vector<std::string> data;
  for (int i = 0; i < 100; i++) {
    std::string data_item("This is a small amount of data for the " + 
                              std::to_string(i) + "-th slot");
    data.push_back(data_item);
  }
  return data;
}

const std::vector<std::string> kGlobalFileData = InitializeData();

// Init function to initialize the client
void init() {
  const std::string config_path = absl::GetFlag(FLAGS_config_path);
  const std::string master_name = absl::GetFlag(FLAGS_master_name);
  const bool resolve_hostname(true);

  LOG(INFO) << "Calling init_client...";
  auto init_status(gfs::client::init_client(config_path, master_name, 
                                            resolve_hostname));
  if (!init_status.ok()) {
    LOG(ERROR) << "Client initialization failed with error: " 
              << init_status.error_message();
    exit(-1);
  }
}

// Create and write to a single file
void singleFileWrite(const std::string& filename_base, ushort id) {
  init();
  const std::string filename(filename_base + std::to_string(id));
  auto create_file_status(gfs::client::open(filename.c_str(),
                                            gfs::OpenFlag::Create));
  if (!create_file_status.ok()) {
    LOG(ERROR) << "Open to create " + filename + " failed with error: "
              << create_file_status;
    exit(1);
  } else {
    LOG(INFO) << "Open to create " + filename + " succeeded";
  }

  auto write_status(gfs::client::write(
      filename.c_str(), (void*)kGlobalFileData[id].c_str(), 0, 
      kGlobalFileData[id].size()));

  if (!write_status.ok()) {
    LOG(ERROR) << "Write request in the " + std::to_string(id) 
              << "-th thread failed due to: " << write_status;
    exit(1);
  } else {
    LOG(INFO) << "Write request in the " + std::to_string(id) << " succeeded"; 
  }
}

// Read a single file
void singleFileRead(const std::string& filename_base, ushort id) {
  init();
  const std::string filename(filename_base + std::to_string(id));
  auto open_file_status(gfs::client::open(filename.c_str(),
                                          gfs::OpenFlag::Read));
  if (!open_file_status.ok()) {
    LOG(ERROR) << "Open to read " + filename + " failed with error: "
              << open_file_status;
    exit(1);
  } else {
    LOG(INFO) << "Open to read " + filename + " succeeded";
  }

  auto read_data_or(gfs::client::read(
      filename.c_str(), 0, kGlobalFileData[id].size()));

  if (!read_data_or.ok()) {
    LOG(ERROR) << "Read request in the " + std::to_string(id) 
              << "-th thread failed due to: " << read_data_or.status();
    exit(1);
  } else {
    LOG(INFO) << "Read request in the " + std::to_string(id) << " succeeded"; 
  }

  auto read_data(read_data_or.ValueOrDie());
  if (memcmp(read_data.buffer, kGlobalFileData[id].c_str(), 
            kGlobalFileData[id].size()) != 0) {
    LOG(ERROR) << "Read request in the " + std::to_string(id) 
              << " receives incorrect data";
    exit(1);
  } else {
    LOG(INFO) << "Read request in the " + std::to_string(id)
              << " receives correct data";
  }
}

// Launch a number of threads and each to create and write to a different file
void parallelFileWrite(const std::string& filename_base,
                      const ushort num_of_threads) {
  std::vector<std::thread> threads;
  for (auto i = 0; i < num_of_threads; i++) {
    threads.push_back(std::thread(singleFileWrite, filename_base, i));
  }

  for(auto& t : threads) {
    t.join();
  }
}

// Launch a number of threads and each to read a different file
void parallelFileRead(const std::string& filename_base,
                      const ushort num_of_threads) {
  std::vector<std::thread> threads;
  for (auto i = 0; i < num_of_threads; i++) {
    threads.push_back(std::thread(singleFileRead, filename_base, i));
  }

  for(auto& t : threads) {
    t.join();
  }
} 

int main(int argc, char** argv) {
  // Initialize system log and parse command line options
  gfs::common::SystemLogger::GetInstance().Initialize(argv[0]);
  absl::ParseCommandLine(argc, argv);

  // If the client is executed in writer's mode we write, otherwise we read and check
  // results
  if (absl::GetFlag(FLAGS_is_writer)) {
    parallelFileWrite("/bar", 50);
  } else {
    parallelFileRead("/bar", 50);
  }

  return 0;
}
