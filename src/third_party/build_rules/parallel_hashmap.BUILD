# Copyright 2020 Gan Tu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("@rules_cc//cc:defs.bzl", "cc_library")

# Custom Bazel build for LevelDB on Unix/MacOS
# Simply depend on "@com_google_leveldb//:leveldb" for use
#
# For Windows, you may need to change LEVELDB_PLATFORM_POSIX to
# LEVELDB_PLATFORM_WINDOWS and uncomment all the "...windows..."
# in the "exclude" folder below. However, I haven't tested that.
cc_library(
    name = "parallel_hashmap",
    srcs = glob(
        ["**/*.cc"],
        exclude = [
            "doc/**",
            "**/*_test.cc",
            "examples/**",
            "benchmark/**",
            "tests/**",
            "css/**",
            "html/**",
            "cmake/**",
        ],
    ),
    hdrs = glob(
        ["**/*.h"],
        exclude = [
            "doc/**",
            "examples/**",
            "benchmark/**",
            "tests/**",
            "css/**",
            "html/**",
            "cmake/**",
        ],
    ),
    defines = [
    ],
    includes = ["include"],
    linkopts = [
        "-Wno-dev",
        "-pthread",
    ],
    linkstatic = 1,
    visibility = ["//visibility:public"],
)
