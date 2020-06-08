#include "absl/strings/str_cat.h"
#include "benchmark/benchmark.h"
#include "glog/logging.h"
#include "src/client/gfs_client.h"

const char kConfigFileName[] = "data/config.yml";
const char kMasterName[] = "master_server_01";
const bool kResolveHostname = true;
const char kFileName[] = "/test-file-name";

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

static void BM_READ(benchmark::State& state) {
  uint64_t ok = 0;
  uint64_t failed = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto init_status(gfs::client::init_client(kConfigFileName, kMasterName,
                                              kResolveHostname));
    auto open_status_or = gfs::client::open(kFileName, (gfs::OpenFlag::Read));
    state.ResumeTiming();
    auto read_status_or = gfs::client::read(kFileName, 0, state.range(0));
    state.PauseTiming();
    if (!read_status_or.ok()) {
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

// // The benchmark may fail if you run all of them at the same time, because
// // the GFS server cluster may have a limit on its disk size, with a max number
// // of chunks available for allocation. Thus, when google benchmark run thousands
// // of iterations, the GFS will quickly become unable to handle read/write
// // Note:
// // Concurrent thread read will introduce cache on server side, and reduce 
// // latency being benchmarked, so it should do random read instead
// BENCHMARK(BM_READ)
//     ->Iterations(2000)
//     ->ThreadRange(/*min_threads=*/1, /*max_threads=*/100)
//     ->DenseRange(1, 20001, 1000);  // 1KB to 10MB

BENCHMARK(BM_READ)->DenseRange(1, 100 * 1000 + 1, 1000);  // 1KB to 10MB

// Instead of using BENCHMARK_MAIN, we manually write the main to allows
// use initialize the google logging, to surpress the INFO log being logged
// to stderr by default when glog is not explicitly initialized
int main(int argc, char** argv) {
  google::InitGoogleLogging(/*program_name*/ argv[0]);

  // Seed Data
  auto init_status(
      gfs::client::init_client(kConfigFileName, kMasterName, kResolveHostname));

  // Setup
  void* data = (void*)(GenerateData(100 * 1000).c_str());  // 10MB file
  uint64_t offset = 0;
  auto open_status_or = gfs::client::open(
      kFileName, (gfs::OpenFlag::Write | gfs::OpenFlag::Create));
  if (!open_status_or.ok()) {
    return 1;
  }
  auto write_status_or =
      gfs::client::write(kFileName, data, offset, 100 * 1000);
  if (!write_status_or.ok()) {
    return 1;
  }

  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  ::benchmark::RunSpecifiedBenchmarks();
}
