# syntax=docker/dockerfile:1

# Build (and run) image for the GFS servers. Works on both amd64 and arm64:
# bazelisk downloads the Bazel version pinned in .bazelversion for the
# container's architecture.
FROM ubuntu:24.04

ARG TARGETARCH

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential \
    ca-certificates \
    curl \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Install bazelisk, which fetches the Bazel version pinned by .bazelversion
RUN curl -fsSL \
    "https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-${TARGETARCH}" \
    -o /usr/local/bin/bazel \
    && chmod +x /usr/local/bin/bazel

WORKDIR /app

# Copy the build configuration first so dependency downloads are cached as
# their own layer and survive source-only changes
COPY .bazelversion .bazelrc MODULE.bazel ./
COPY src/third_party ./src/third_party

# Build and cache protos generated code
COPY src/protos ./src/protos
RUN bazel build //src/protos/...

# Build and cache common code
COPY src/common ./src/common
RUN bazel build //src/common/...

# Build and cache client code
COPY src/client ./src/client
RUN bazel build //src/client/...

# Build and cache server code
COPY src/server ./src/server
COPY data ./data
RUN bazel build //src/server/...

# Build the rest (examples, benchmarks, command line client)
COPY . .
RUN bazel build //src/... //examples/... //:gfs_client_main
