[![Build Status](https://travis-ci.com/Michael-Tu/cppGFS2.0.svg?branch=master)](https://travis-ci.com/Michael-Tu/cppGFS2.0)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# cppGFS2.0
A distributed Google File System (GFS) with multiple masters, implemented in C++

## Getting Started

In this project, we plan to use [Bazel](http://bazel.build) as our main build tool. You can install Bazel by following their website [instructions](https://docs.bazel.build/versions/master/install.html).

For MacOS, you can use Homebrew:

```
brew tap bazelbuild/tap
brew install bazelbuild/tap/bazel
```

For Linux, you can run the `scripts/install_bazel.sh` with desired Bazel version:

```
export BAZEL_VERSION=3.1.0
chmod +x scripts/install_bazel.sh
scripts/install_bazel.sh
```

Then, from the root directory, you can run Bazel commands as normal. For example:

```
bazel build ...
bazel test --test_output=errors ...
```

To learn more about how to use Bazel, or how to write Bazel build rule for C++, see the [official documentation](https://docs.bazel.build/versions/master/bazel-overview.html).

## Running GFS client

Make sure you have the GFS server clusters are up and running.

You can either write a binary by importing the GFS client at `src/client/gfs_client.h`, or using the GFS command line binary.

To build the command line binary, run

```
bazel build :gfs_client_main
```

Then, you can run any of these modes:

```
# To create a file
bazel-bin/gfs_client_main --mode=create --filename=/test

# To read a file
bazel-bin/gfs_client_main --mode=read --filename=/test --offset=0 --nbytes=100

# To write a file
# This will create the file if it doesn't exist; if you don't want this behavior,
# use the 'mode_no_create' mode instead
bazel-bin/gfs_client_main --mode=write --filename=/test --offset=0 --data='Hello World!'
```

## Running GFS server clusters using Docker

Make sure you have [Docker](https://docs.docker.com/engine/install/) and [Docker compose](https://docs.docker.com/compose/install/) installed

To start all servers and expose respective server ports outside of Docker for connection, run:

```
docker-compose up --build
```

Then, you can use GFS client to interact with the cluster.

After you are done with it, turn everything off by typing Ctrl + C, and then

```
docker-compose down
```

## Benchmark Performance

We use [Google Benchmarks](https://github.com/google/benchmark) open source library to test our performance.

To run them, simply start the GFS cluster in the background, and run the benchmark binaries in `src/benchmarks`

## Known Issues

- If the initialization of the first file chunk fails at any of the chunk server during file creation, the metadata will be created but chunks won't be initialized, so future file creation on the same filename fails, and file read/write will use inconsistent chunk server locations
- We do not bring stale replica up to date, at the moment


## C++ Style Guide

Please, if possible, follow [Google C++ style guide](https://google.github.io/styleguide/cppguide.html). If you use an IDE or any common text editors, they have extensions that help you auto format and lint your code for style errors.


