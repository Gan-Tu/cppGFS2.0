[![Build Status](https://travis-ci.com/Gan-Tu/cppGFS2.0.svg?branch=master)](https://travis-ci.com/Gan-Tu/cppGFS2.0)
![Docker Image Build](https://github.com/Gan-Tu/cppGFS2.0/workflows/Docker%20Image%20CI/badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# cppGFS2.0
A [distributed Google File System (GFS)](https://research.google/pubs/pub51), implemented in C++

## Demo

* [Presentation Slide](https://docs.google.com/presentation/d/1bQr_XluTRHOCHalKPw1VULvgMpBHS4MAqIj6UrpRQPM/edit?usp=sharing)
* [Demo Video](https://youtu.be/EX-ELL_43Og)
* [Our Paper](data/CS244B_Final_Paper.pdf)

## Getting Started

In this project, we plan to use [Bazel](http://bazel.build) as our main build tool. You can install Bazel by following their website [instructions](https://docs.bazel.build/versions/master/install.html).

As of writing, you will need at least Bazel version 5.1.0 for a proper working demo.

For MacOS, you can use:

```
export BAZEL_VERSION=5.1.0
curl -fLO "https://github.com/bazelbuild/bazel/releases/download/${BAZEL_VERSION}/bazel-${BAZEL_VERSION}-installer-darwin-x86_64.sh"
chmod +x "bazel-${BAZEL_VERSION}-installer-darwin-x86_64.sh"
./bazel-${BAZEL_VERSION}-installer-darwin-x86_64.sh --user

export PATH="$PATH:$HOME/bin"
bazel --version
```

For Linux, you can run the `scripts/install_bazel.sh` with desired Bazel version:

```
export BAZEL_VERSION=5.1.0
chmod +x scripts/install_bazel.sh
scripts/install_bazel.sh
```

_For ARM64 architecture (such as Apple M1 chips), use `scripts/install_bazel_arm64.sh`. You may need to comment out the `apt` lines._

Then, from the root directory, you can run Bazel commands as normal. For example:

```
bazel build ...
bazel test --test_output=errors ...
```

To learn more about how to use Bazel, or how to write Bazel build rule for C++, see the [official documentation](https://docs.bazel.build/versions/master/bazel-overview.html).

**Note**: If you get an error in the form of `illegal thread local variable reference to regular symbol`, try adding `--features=-supports_dynamic_linker` to your Bazel build flags. For example, `bazel build --features=-supports_dynamic_linker ...`.

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
# use the 'write_no_create' mode instead
bazel-bin/gfs_client_main --mode=write --filename=/test --offset=0 --data='Hello World!'
```

## Running GFS server clusters using Docker

Make sure you have [Docker](https://docs.docker.com/engine/install/) and [Docker compose](https://docs.docker.com/compose/install/) installed

To start all servers and expose respective server ports outside of Docker for connection, run:

```
docker-compose up --build
```

_As of writing, the Dockerfile doesn't support MacBook M1 Max yet. You will need to manually update the file to install/use a ARM64 compatible Bazel image. You can refer to the `scripts` folder for installation instructions._

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


