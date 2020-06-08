#include "absl/strings/str_cat.h"
#include "benchmark/benchmark.h"
#include "glog/logging.h"
#include "src/client/gfs_client.h"

const char kConfigFileName[] = "data/config.yml";
const char kMasterName[] = "master_server_01";
const bool kResolveHostname = true;

static const char kAlphaEnum[] =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

// Initialize the data by randomly generation const std::string
static std::string GenerateData(size_t n_kb) {
  std::string data;
  size_t nbytes = n_kb * 1024;
  data.resize(nbytes);
  for (size_t i = 0; i < nbytes; ++i) {
    data[i] = kAlphaEnum[rand() % (sizeof(kAlphaEnum) - 1)];
  }
  return data;
}

static std::string GenerateRandomFileName() {
  // Make it as random as possible for multi-threaded create tests
  // Not ideal, but works to avoid collisions among benchmark iterations
  return absl::StrCat("/", rand(), "_", rand());
}

static void BM_WRITE_NEW(benchmark::State& state) {
  auto init_status(
      gfs::client::init_client(kConfigFileName, kMasterName, kResolveHostname));
  uint64_t ok = 0;
  uint64_t failed = 0;
  for (auto _ : state) {
    state.PauseTiming();
    void* data = (void*)(GenerateData(state.range(0)).c_str());
    uint64_t offset = 0;
    std::string filename =
        absl::StrCat(GenerateRandomFileName(), "_t", state.thread_index);
    state.ResumeTiming();

    auto open_status_or = gfs::client::open(
        filename.c_str(), (gfs::OpenFlag::Write | gfs::OpenFlag::Create));
    state.PauseTiming();
    if (!open_status_or.ok()) {
      failed++;
      state.ResumeTiming();
      continue;
    }
    state.ResumeTiming();
    auto write_status_or =
        gfs::client::write(filename.c_str(), data, offset, state.range(0));
    state.PauseTiming();
    if (!write_status_or.ok()) {
      failed++;
    } else {
      ok++;
    }
    state.ResumeTiming();
  }
  state.counters["ok"] = ok;
  state.counters["failed"] = failed;
}

static void BM_WRITE_EXISTING(benchmark::State& state) {
  auto init_status(
      gfs::client::init_client(kConfigFileName, kMasterName, kResolveHostname));
  // Setup
  void* data = (void*)(GenerateData(10 * 1024).c_str());  // 10MB file
  uint64_t offset = 0;
  std::string filename =
      absl::StrCat(GenerateRandomFileName(), "_t", state.thread_index);
  auto open_status_or = gfs::client::open(
      filename.c_str(), (gfs::OpenFlag::Write | gfs::OpenFlag::Create));
  if (!open_status_or.ok()) {
    return;
  }
  uint64_t ok = 0;
  uint64_t failed = 0;
  for (auto _ : state) {
    auto write_status_or =
        gfs::client::write(filename.c_str(), data, offset, state.range(0));
    state.PauseTiming();
    if (!write_status_or.ok()) {
      failed++;
    } else {
      ok++;
    }
    state.ResumeTiming();
  }
  state.counters["ok"] = ok;
  state.counters["failed"] = failed;
}

// Register the function as a benchmark

// BENCHMARK(BM_WRITE_NEW)
//     ->Iterations(500)
//     ->DenseRange(1, 10 * 1000 + 1, 1000);  // 1KB to 10MB

// BENCHMARK(BM_WRITE_EXISTING)
//     ->Iterations(500)
//     ->DenseRange(1, 10 * 1000 + 1, 1000);  // 1KB to 10MB

// BENCHMARK(BM_WRITE_NEW)
//     ->Iterations(100)
//     ->ThreadRange(/*min_threads=*/1, /*max_threads=*/100)
//     ->Range(1, 1000);  // 1KB to 1MB

// BENCHMARK(BM_WRITE_EXISTING)
//     ->Iterations(100)
//     ->ThreadRange(/*min_threads=*/1, /*max_threads=*/100)
//     ->Range(1, 1000);  // 1KB to 1MB

// Instead of using BENCHMARK_MAIN, we manually write the main to allows
// use initialize the google logging, to surpress the INFO log being logged
// to stderr by default when glog is not explicitly initialized
int main(int argc, char** argv) {
  google::InitGoogleLogging(/*program_name*/ argv[0]);
  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  ::benchmark::RunSpecifiedBenchmarks();
}
