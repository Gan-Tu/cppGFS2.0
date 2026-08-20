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

# Custom Bazel build for Parallel Hashmap (a header-only library).
# Simply depend on "@parallel_hashmap//:parallel_hashmap" for use
cc_library(
    name = "parallel_hashmap",
    hdrs = glob(["parallel_hashmap/*.h"]),
    linkopts = ["-pthread"],
    visibility = ["//visibility:public"],
)
