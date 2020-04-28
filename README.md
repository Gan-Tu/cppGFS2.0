[![Build Status](https://travis-ci.com/Michael-Tu/cppGFS2.0.svg?branch=master)](https://travis-ci.com/Michael-Tu/cppGFS2.0)

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
bazel build :all
bazel test :all
```

To learn more about how to use Bazel, or how to write Bazel build rule for C++, see the [official documentation](https://docs.bazel.build/versions/master/bazel-overview.html).

## Examples

I've included example code from gRPC. You can try it by first build all the code:

```
bazel build :all
```

### Synchronised gRPC

Then, in one terminal, you can start the server by typing

```
bazel run :greeter_server
```

Then, in another terminal, you can run a client to issue a request

```
bazel run :greeter_client
```

You should see a prompt "Greeter received: Hello world".

### Asynchronous gRPC

Then, in one terminal, you can start the server by typing

```
bazel run :greeter_async_server
```

Then, in another terminal, you can run a client to issue a request

```
bazel run :greeter_async_client
```

You should see a prompt "Greeter received: Hello world xx" with numbers randomly printed between 0 and 99, and blocks forever until you use Ctrl+C to quit.
