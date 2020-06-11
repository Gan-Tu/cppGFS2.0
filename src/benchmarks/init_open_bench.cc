#include "absl/strings/str_cat.h"
#include "benchmark/benchmark.h"
#include "glog/logging.h"
#include "src/client/gfs_client.h"

const char kConfigFileName[] = "data/config.yml";
const char kMasterName[] = "master_server_01";
const bool kResolveHostname = true;

static std::string rand_string() {
  // Make it as random as possible for multi-threaded create tests
  // Not ideal, but works to avoid collisions among benchmark iterations
  return absl::StrCat(rand(), "-", rand());
}

static void BM_INIT_CLIENT(benchmark::State& state) {
  // Benchmark the init_client function
  for (auto _ : state) {
    auto init_status(gfs::client::init_client(kConfigFileName, kMasterName,
                                              kResolveHostname));
    // delete the cached client impl, so we can run it again
    state.PauseTiming();
    gfs::client::reset_client();
    state.ResumeTiming();
  }
}

static void BM_OPEN_WITH_READ_MODE(benchmark::State& state) {
  // Benchmark the init_client function
  uint64_t ok = 0;
  uint64_t failed = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto init_status(
      gfs::client::init_client(kConfigFileName, kMasterName, kResolveHostname));
    state.ResumeTiming();
    auto status_or = gfs::client::open("/open_with_read", gfs::OpenFlag::Read);
    state.PauseTiming();
    if (status_or.ok()) {
      ok++;
    } else {
      failed++;
    }
    gfs::client::reset_client();
    state.ResumeTiming();
  }
  state.counters["ok"] = ok;
  state.counters["failed"] = failed;
}

static void BM_OPEN_WITH_WRITE_MODE(benchmark::State& state) {
  // Benchmark the init_client function
  uint64_t ok = 0;
  uint64_t failed = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto init_status(
      gfs::client::init_client(kConfigFileName, kMasterName, kResolveHostname));
    state.ResumeTiming();
    auto status_or =
        gfs::client::open("/open_with_write", gfs::OpenFlag::Write);
    state.PauseTiming();
    if (status_or.ok()) {
      ok++;
    } else {
      failed++;
    }
    gfs::client::reset_client();
    state.ResumeTiming();
  }
  state.counters["ok"] = ok;
  state.counters["failed"] = failed;
}

static void BM_OPEN_WITH_CREATE_MODE_SINGLE_THREADED(benchmark::State& state) {
  // Benchmark the init_client function
  uint64_t ok = 0;
  uint64_t failed = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto init_status(
      gfs::client::init_client(kConfigFileName, kMasterName, kResolveHostname));
    state.ResumeTiming();
    auto status_or = gfs::client::open(
        absl::StrCat("/open_with_create_one_thread_", rand_string()).c_str(),
        gfs::OpenFlag::Create);
    state.PauseTiming();
    if (status_or.ok()) {
      ok++;
    } else {
      failed++;
    }
    gfs::client::reset_client();
    state.ResumeTiming();
  }
  state.counters["ok"] = ok;
  state.counters["failed"] = failed;
}

static void BM_OPEN_WITH_CREATE_MODE_MULTI_THREADED(benchmark::State& state) {
  // Benchmark the init_client function
  uint64_t ok = 0;
  uint64_t failed = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto init_status(
      gfs::client::init_client(kConfigFileName, kMasterName, kResolveHostname));
    state.ResumeTiming();
    auto status_or =
        gfs::client::open(absl::StrCat("/open_with_create_t_",
                                       state.thread_index, "_", rand_string())
                              .c_str(),
                          gfs::OpenFlag::Create);
    state.PauseTiming();
    if (status_or.ok()) {
      ok++;
    } else {
      failed++;
    }
    gfs::client::reset_client();
    state.ResumeTiming();
  }
  state.counters["ok"] = ok;
  state.counters["failed"] = failed;
}

// Register the function as a benchmark
BENCHMARK(BM_INIT_CLIENT);
BENCHMARK(BM_OPEN_WITH_READ_MODE);
BENCHMARK(BM_OPEN_WITH_WRITE_MODE);
BENCHMARK(BM_OPEN_WITH_CREATE_MODE_SINGLE_THREADED);
BENCHMARK(BM_OPEN_WITH_CREATE_MODE_MULTI_THREADED)
    ->ThreadRange(/*min_threads=*/2, /*max_threads=*/100);

// Instead of using BENCHMARK_MAIN, we manually write the main to allows
// use initialize the google logging, to surpress the INFO log being logged
// to stderr by default when glog is not explicitly initialized
int main(int argc, char** argv) {
  google::InitGoogleLogging(/*program_name*/ argv[0]);
  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  ::benchmark::RunSpecifiedBenchmarks();
}
