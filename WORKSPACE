load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Google Protocol Buffers.
http_archive(
    name = "rules_proto",
    sha256 = "602e7161d9195e50246177e7c55b2f39950a9cf7366f74ed5f22fd45750cd208",
    strip_prefix = "rules_proto-97d8af4dc474595af3900dd85cb3a29ad28cc313",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz",
        "https://github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz",
    ],
)

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")
rules_proto_dependencies()
rules_proto_toolchains()

# Google Abseil C++ Library
# Example Useage: #include "absl/string"
http_archive(
    name = "com_google_absl",
    strip_prefix = "abseil-cpp-20200225.2",
    sha256 = "f41868f7a938605c92936230081175d1eae87f6ea2c248f41077c8f88316f111",
    urls = [
        "https://github.com/abseil/abseil-cpp/archive/20200225.2.tar.gz",
    ]
)

# GoogleTest
# Example Useage: #include "gtest/gtest.h"
http_archive(
    name = "com_google_test",
    strip_prefix = "googletest-release-1.10.0",
    sha256="9dc9157a9a1551ec7a7e43daea9a694a0bb5fb8bec81235d8a1e6ef64c716dcb",
    urls = [
        "https://github.com/google/googletest/archive/release-1.10.0.tar.gz",
    ]
)
