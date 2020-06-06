#include "benchmark/benchmark.h"
#include "glog/logging.h"
#include "src/client/gfs_client.h"

const char kConfigFileName[] = "data/config.yml";
const char kMasterName[] = "master_server_01";
const bool kResolveHostname = true;

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
  auto init_status(
      gfs::client::init_client(kConfigFileName, kMasterName, kResolveHostname));
  // Benchmark the init_client function
  state.counters["ok"] = 0;
  state.counters["failed"] = 0;
  for (auto _ : state) {
    auto status_or = gfs::client::open("/open_with_read", gfs::OpenFlag::Read);
    state.PauseTiming();
    if (status_or.ok()) {
      state.counters["ok"]++;
    } else {
      state.counters["failed"]++;
    }
    state.ResumeTiming();
  }
}

static void BM_OPEN_WITH_WRITE_MODE(benchmark::State& state) {
  auto init_status(
      gfs::client::init_client(kConfigFileName, kMasterName, kResolveHostname));
  // Benchmark the init_client function
  state.counters["ok"] = 0;
  state.counters["failed"] = 0;
  for (auto _ : state) {
    auto status_or =
        gfs::client::open("/open_with_write", gfs::OpenFlag::Write);
    state.PauseTiming();
    if (status_or.ok()) {
      state.counters["ok"]++;
    } else {
      state.counters["failed"]++;
    }
    state.ResumeTiming();
  }
}

static void BM_OPEN_WITH_CREATE_MODE(benchmark::State& state) {
  auto init_status(
      gfs::client::init_client(kConfigFileName, kMasterName, kResolveHostname));
  // Benchmark the init_client function
  state.counters["ok"] = 0;
  state.counters["failed"] = 0;
  for (auto _ : state) {
    auto status_or =
        gfs::client::open("/open_with_create", gfs::OpenFlag::Create);
    state.PauseTiming();
    if (status_or.ok()) {
      state.counters["ok"]++;
    } else {
      state.counters["failed"]++;
    }
    state.ResumeTiming();
  }
}

// Register the function as a benchmark
BENCHMARK(BM_INIT_CLIENT);
BENCHMARK(BM_OPEN_WITH_READ_MODE);
BENCHMARK(BM_OPEN_WITH_WRITE_MODE);
BENCHMARK(BM_OPEN_WITH_CREATE_MODE);

// Instead of using BENCHMARK_MAIN, we manually write the main to allows
// use initialize the google logging, to surpress the INFO log being logged
// to stderr by default when glog is not explicitly initialized
int main(int argc, char** argv) {
  google::InitGoogleLogging(/*program_name*/ argv[0]);
  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  ::benchmark::RunSpecifiedBenchmarks();
}
