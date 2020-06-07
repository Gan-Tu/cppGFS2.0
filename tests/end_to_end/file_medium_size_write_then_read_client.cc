#include <thread>
#include <vector>
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "src/client/gfs_client.h"
#include "src/common/system_logger.h"

ABSL_FLAG(std::string, config_path, "file_medium_size_write_then_read_test/config.yaml", 
          "/path/to/config.yml");
ABSL_FLAG(std::string, master_name, "master_server_01",
          "master name for this client to talk to");

// We write 100MB data, which causes multiple (2) chunks to be created for a file
const size_t kGlobalFileDataSize = 100 * 1024 * 1024;

// Initialize the data by randomly generation
const std::string InitializeData() {
  static const char alphanum[] 
      = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  std::string data;
  data.resize(kGlobalFileDataSize);
  
  for (size_t i = 0; i < kGlobalFileDataSize; ++i) {
    data[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  return data;
}

const std::string kGlobalFileData = InitializeData();

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

// Read a file, verify its content
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
      filename.c_str(), 0, kGlobalFileData.size()));

  if (!read_data_or.ok()) {
    LOG(ERROR) << "Read request in the " + std::to_string(id) 
               << "-th thread failed due to: " << read_data_or.status();
    exit(1);
  } else {
    LOG(INFO) << "Read request in the " + std::to_string(id) << " succeeded"; 
  }

  auto read_data(read_data_or.ValueOrDie());
  if (memcmp(read_data.buffer, kGlobalFileData.c_str(), 
             kGlobalFileData.size()) != 0) {
    LOG(ERROR) << "Read request in the " + std::to_string(id) 
               << " receives incorrect data";
    exit(1);
  } else {
    LOG(INFO) << "Read request in the " + std::to_string(id)
              << " receives correct data";
  }
}

// Write a file and then read it from a different thread (so acting as a 
// different client), verify the read
void singleFileWriteThenRead(const std::string& filename_base, ushort id) {
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
      filename.c_str(), (void*)kGlobalFileData.c_str(), 0, 
      kGlobalFileData.size()));

  if (!write_status.ok()) {
    LOG(ERROR) << "Write request in the " + std::to_string(id) 
               << "-th thread failed due to: " << write_status;
    exit(1);
  } else {
    LOG(INFO) << "Write request in the " + std::to_string(id) << " succeeded"; 
  }

  // After successful write, spawn a reader thread and read to verify the write
  std::thread reader_thread(singleFileRead, filename_base, id);
  reader_thread.join();
}

// Launch a number of threads, and each thread first write a medium-size file 
// (kGlobalFileData), and then spawn a separate thread to read the file and 
// verify the data.
void parallelFileWriteThenRead(const std::string& filename_base,
                               const ushort num_of_threads) {
  std::vector<std::thread> threads;
  for (auto i = 0; i < num_of_threads; i++) {
    threads.push_back(std::thread(singleFileWriteThenRead, filename_base, i));
  }

  for(auto& t : threads) {
    t.join();
  }
}

int main(int argc, char** argv) {
  // Initialize system log and parse command line options
  gfs::common::SystemLogger::GetInstance().Initialize(argv[0]);
  absl::ParseCommandLine(argc, argv);

  parallelFileWriteThenRead("/baz", 3);

  return 0;
}
