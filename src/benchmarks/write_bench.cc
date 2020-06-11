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
  uint64_t ok = 0;
  uint64_t failed = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto init_status(gfs::client::init_client(kConfigFileName, kMasterName,
                                              kResolveHostname));
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
    gfs::client::reset_client();
    state.ResumeTiming();
  }
  state.counters["ok"] = ok;
  state.counters["failed"] = failed;
}

static void BM_WRITE_EXISTING(benchmark::State& state) {
  // Setup
  void* data = (void*)(GenerateData(10 * 1024).c_str());  // 10MB file
  uint64_t offset = 0;
  std::string filename =
      absl::StrCat(GenerateRandomFileName(), "_t", state.thread_index);
  uint64_t ok = 0;
  uint64_t failed = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto init_status(
      gfs::client::init_client(kConfigFileName, kMasterName, kResolveHostname));
    auto open_status_or = gfs::client::open(
        filename.c_str(), (gfs::OpenFlag::Write | gfs::OpenFlag::Create));
    if (!open_status_or.ok()) {
      return;
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
    gfs::client::reset_client();
    state.ResumeTiming();
  }
  state.counters["ok"] = ok;
  state.counters["failed"] = failed;
}

// The benchmark may fail if you run all of them at the same time, because
// the GFS server cluster may have a limit on its disk size, with a max number
// of chunks available for allocation. Thus, when google benchmark run thousands
// of iterations, the GFS will quickly become unable to handle read/write
//
// It can also fail due to change of random filename collide and we prohibit
// re-createing the same filename. Thus, use with caution

BENCHMARK(BM_WRITE_NEW)
    ->Iterations(500)
    ->DenseRange(1, 10 * 1000 + 1, 1000);  // 1KB to 10MB

BENCHMARK(BM_WRITE_EXISTING)
    ->Iterations(500)
    ->DenseRange(1, 10 * 1000 + 1, 1000);  // 1KB to 10MB

BENCHMARK(BM_WRITE_NEW)
    ->Iterations(100)
    ->ThreadRange(/*min_threads=*/1, /*max_threads=*/100)
    ->Range(1, 1000);  // 1KB to 1MB

BENCHMARK(BM_WRITE_EXISTING)
    ->Iterations(100)
    ->ThreadRange(/*min_threads=*/1, /*max_threads=*/100)
    ->Range(1, 1000);  // 1KB to 1MB

// Instead of using BENCHMARK_MAIN, we manually write the main to allows
// use initialize the google logging, to surpress the INFO log being logged
// to stderr by default when glog is not explicitly initialized
int main(int argc, char** argv) {
  google::InitGoogleLogging(/*program_name*/ argv[0]);
  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  ::benchmark::RunSpecifiedBenchmarks();
}
