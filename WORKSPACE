load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Google Protocol Buffers.
http_archive(
    name = "rules_proto",
    sha256 = "c22cfcb3f22a0ae2e684801ea8dfed070ba5bed25e73f73580564f250475e72d",
    strip_prefix = "rules_proto-4.0.0-3.19.2",
    urls = [
        "https://github.com/bazelbuild/rules_proto/archive/refs/tags/4.0.0-3.19.2.tar.gz",
    ],
)

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")
rules_proto_dependencies()
rules_proto_toolchains()

# Google Abseil C++ Library
# Example Useage: #include "absl/string"
http_archive(
    name = "com_google_absl",
    sha256 = "dcf71b9cba8dc0ca9940c4b316a0c796be8fab42b070bb6b7cab62b48f0e66c4",
    strip_prefix = "abseil-cpp-20211102.0",
    urls = [
        "https://github.com/abseil/abseil-cpp/archive/refs/tags/20211102.0.tar.gz",
    ],
)

# GoogleTest
# Example Useage: #include "gtest/gtest.h"
http_archive(
    name = "com_google_test",
    sha256 = "b4870bf121ff7795ba20d20bcdd8627b8e088f2d1dab299a031c1034eddc93d5",
    strip_prefix = "googletest-release-1.11.0",
    urls = [
        "https://github.com/google/googletest/archive/refs/tags/release-1.11.0.tar.gz",
    ],
)

# gRPC
http_archive(
    name = "com_github_grpc_grpc",
    sha256 = "46ee3d12c6d42a4ded46ba025c8ba09850425312c710c6ee6e084bd808ebc1e8",
    strip_prefix = "grpc-1.45.1",
    urls = [
        "https://github.com/grpc/grpc/archive/refs/tags/v1.45.1.tar.gz",
    ],
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()

# LevelDb
# Example dependency: "@com_google_leveldb//:leveldb"
http_archive(
    name = "com_google_leveldb",
    build_file = "//src/third_party/build_rules:leveldb.BUILD",
    sha256 = "55423cac9e3306f4a9502c738a001e4a339d1a38ffbee7572d4a07d5d63949b2",
    strip_prefix = "leveldb-1.22",
    urls = ["https://github.com/google/leveldb/archive/1.22.tar.gz"],
)

# Parallel Hashmap
# https://greg7mdp.github.io/parallel-hashmap/
http_archive(
    name = "parallel_hashmap",
    build_file = "//src/third_party/build_rules:parallel_hashmap.BUILD",
    sha256 = "1826589041140837f91ab0e3421abbf7e9184454bb83403b5a0359a0bf87bd68",
    strip_prefix = "parallel-hashmap-1.31",
    urls = ["https://github.com/greg7mdp/parallel-hashmap/archive/1.31.tar.gz"],
)

# Google gFlags
http_archive(
    name = "com_github_gflags_gflags",
    sha256 = "34af2f15cf7367513b352bdcd2493ab14ce43692d2dcd9dfc499492966c64dcf",
    strip_prefix = "gflags-2.2.2",
    urls = [
        "https://github.com/gflags/gflags/archive/refs/tags/v2.2.2.tar.gz",
    ],
)

# Google glog C++ logging library
# Note: Don't use directly in your code,
#       see examples/system_logger_main.cc for how to log.
# Example Usage: #include "glog/logging.h"
# Example dependency: "@com_google_glog//:glog"
http_archive(
    name = "com_google_glog",
    sha256 = "eede71f28371bf39aa69b45de23b329d37214016e2055269b3b5e7cfd40b59f5",
    strip_prefix = "glog-0.5.0",
    urls = [
        "https://github.com/google/glog/archive/refs/tags/v0.5.0.tar.gz",
    ],
)

# YAML Parser
# https://github.com/jbeder/yaml-cpp
http_archive(
    name = "yaml_cpp",
    sha256 = "43e6a9fcb146ad871515f0d0873947e5d497a1c9c60c58cb102a97b47208b7c3",
    strip_prefix = "yaml-cpp-yaml-cpp-0.7.0",
    urls = ["https://github.com/jbeder/yaml-cpp/archive/refs/tags/yaml-cpp-0.7.0.tar.gz"],
)

# Google Benchmark library
http_archive(
    name = "com_google_benchmark",
    sha256 = "6132883bc8c9b0df5375b16ab520fac1a85dc9e4cf5be59480448ece74b278d4",
    strip_prefix = "benchmark-1.6.1",
    urls = [
        "https://github.com/google/benchmark/archive/refs/tags/v1.6.1.tar.gz",
    ],
)
