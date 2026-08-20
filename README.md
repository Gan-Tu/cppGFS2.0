![CI](https://github.com/Gan-Tu/cppGFS2.0/workflows/CI/badge.svg)
![Docker Image Build](https://github.com/Gan-Tu/cppGFS2.0/workflows/Docker%20Image%20CI/badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# cppGFS2.0

A distributed [Google File System (GFS)](https://research.google/pubs/pub51), implemented in modern C++.

The system implements the core design of the [GFS paper (SOSP 2003)](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf): a single master holding all file metadata, a configurable number of chunk servers storing fixed-size replicated chunks, and a client library that talks to the master for metadata and directly to chunk servers for data.

## Documentation & Demo

* [System Design](docs/DESIGN.md) ([PDF](docs/DESIGN.pdf)) — architecture of the current implementation and how it maps to the GFS paper
* [Original CS244B Paper (2020)](data/CS244B_Final_Paper.pdf) — the course paper describing the system as first built
* [Presentation Slide](https://docs.google.com/presentation/d/1bQr_XluTRHOCHalKPw1VULvgMpBHS4MAqIj6UrpRQPM/edit?usp=sharing)
* [Demo Video](https://youtu.be/EX-ELL_43Og)

## Architecture: what's implemented from the GFS paper

| GFS paper mechanism | Status |
| --- | --- |
| Single master, chunk servers, client library (§2.3–2.4) | ✅ |
| 64MB chunks, immutable globally-unique handles, 3-way replication (§2.5) | ✅ (all configurable) |
| Master metadata persistence and crash recovery — namespace, file→chunk mapping, and chunk versions are written through to an on-disk store before mutations are acknowledged, and recovered on restart (operation log + checkpoint role, §2.6.3) | ✅ |
| Chunk locations *not* persisted; re-learned from chunk server reports (§2.6.2) | ✅ |
| Chunk leases with a primary that defines the mutation order; version number advances only when a new lease is granted (§3.1, §4.5) | ✅ |
| Mutation serialization: the primary applies concurrent writes in a serial order and forwards them to secondaries in that same order (§3.1) | ✅ |
| Stale replica detection: chunk servers report chunk versions; the master tells them to delete stale replicas and adopts higher versions after its own crash (§4.5) | ✅ |
| Garbage collection: deleted files' chunks are reclaimed lazily via the regular master↔chunk-server report exchange (§4.4) | ✅ |
| Re-replication: when a chunk server dies, the master instructs another server to clone under-replicated chunks directly from a live replica (§4.3) | ✅ |
| Data integrity: 32-bit CRC checksums per 64KB block, verified on reads and partial overwrites, updated incrementally on writes (§5.2) | ✅ |
| Client metadata caching with refresh-and-retry when cache goes stale (§2.7.1) | ✅ |
| Data pushed from client to replicas | ⚠️ Client pushes to all replicas in parallel, instead of the paper's pipelined chain (§3.2) |
| Record append, snapshot (§3.3, §3.4) | ❌ Not implemented |
| Shadow masters / master replication (§5.1.3) | ❌ Not implemented (single master) |

## Getting Started

The project builds with [Bazel](https://bazel.build) using Bazel modules (bzlmod). Install [bazelisk](https://github.com/bazelbuild/bazelisk), which automatically uses the Bazel version pinned in [.bazelversion](.bazelversion):

```bash
scripts/install_bazelisk.sh
```

or with Homebrew:

```bash
brew install bazelisk
```

Then, from the root directory, run Bazel commands as normal:

```bash
bazel build //...
```

```bash
bazel test --test_output=errors //tests/...
```

All dependencies (gRPC, Protobuf, Abseil, LevelDB, ...) are pinned in [MODULE.bazel](MODULE.bazel) and downloaded automatically on first build.

## The Playground: try it in your browser

The fastest way to experience the system is the **GFS Playground**, an interactive web app bundled with the repo. Make sure you have [Docker](https://docs.docker.com/engine/install/) with the compose plugin installed (works on both x86_64 and Apple Silicon), then:

```bash
docker compose up --build
```

and open **http://localhost:8080**. You get:

* a live **cluster view** (master + three chunk servers) with per-server *Kill*, *Wipe disk*, and *Restart* controls, animated links, and one-click access to each server's live log;
* a **files console** that creates, writes, reads, and deletes files through the real GFS client;
* three guided, one-click **failure drills** — *Chunk server crash*, *Disk loss & self-healing*, and *Master crash & recovery* — each running real operations against the live cluster while narrating every step (heartbeat detection, re-replication cloning, metadata recovery) as it happens;
* a live **activity feed** of cluster events parsed from the servers' logs: lease grants, version advances, stale-replica deletion, garbage collection, re-replication.

Nothing in the playground is simulated — killing a server is a real `SIGKILL`, wiping a disk really deletes its LevelDB store, and the healing you watch is the master doing its job.

The cluster's databases persist in the `gfs_data` volume across restarts. Stop with Ctrl+C, then `docker compose down` (add `-v` to also wipe all stored files). The playground also runs without Docker: `python3 webapp/server.py` from the repo root after `bazel build //...`.

### Classic multi-container cluster (no web UI)

To run each server in its own container instead — the traditional deployment this project has always supported:

```bash
COMPOSE_PROFILES=cluster docker compose up --build
```

The two modes share host ports (50051–50054), so run one at a time. Note for pre-existing checkouts: server databases now live under `data/dbs/` (previously `data/gfs_db_*`), and the compose volume holds only the databases, so config edits take effect on rebuild.

## Running the GFS client

With the cluster up, use the command line client (or write your own binary against [src/client/gfs_client.h](src/client/gfs_client.h)):

```bash
bazel build //:gfs_client_main
```

Then run any of these modes:

```bash
# To create a file
bazel-bin/gfs_client_main --mode=create --filename=/test
```

```bash
# To write to a file (creates it if it doesn't exist;
# use --mode=write_no_create to disable creation)
bazel-bin/gfs_client_main --mode=write --filename=/test --offset=0 --data='Hello World!'
```

```bash
# To read a file
bazel-bin/gfs_client_main --mode=read --filename=/test --offset=0 --nbytes=100
```

```bash
# To delete a file
bazel-bin/gfs_client_main --mode=remove --filename=/test
```

## Configuration

Cluster topology, chunk block size, replication factor, storage paths, and timeouts (lease, heartbeat, gRPC deadline, client cache) all live in [data/config.yml](data/config.yml). Each master or chunk server entry under `disk.leveldb` names the on-disk LevelDB store that server uses; a master with no entry runs without metadata persistence (and warns at startup).

## Fault tolerance behaviors to try

* **Kill a chunk server** while the cluster is running: the master's heartbeat monitor detects it, unregisters it, and re-replicates the chunks it held onto the remaining servers from a live replica. Reads and writes keep working.
* **Restart the master**: the namespace, file→chunk mappings, and chunk versions are recovered from its metadata store; chunk locations are re-learned as chunk servers report in.
* **Bring a dead chunk server back**: any chunks that missed writes while it was down are detected as stale by version comparison in its next report, and the server deletes them.

## Benchmark Performance

We use the [Google Benchmark](https://github.com/google/benchmark) library. Start a GFS cluster in the background, then run the benchmark binaries under [src/benchmarks](src/benchmarks).

## C++ Style Guide

Please, if possible, follow [Google C++ style guide](https://google.github.io/styleguide/cppguide.html). If you use an IDE or any common text editors, they have extensions that help you auto format and lint your code for style errors.
