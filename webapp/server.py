#!/usr/bin/env python3
"""cppGFS2.0 Playground.

A self-contained web app for interactively exercising the GFS cluster:
create/write/read/delete files, kill and restart servers, wipe a chunk
server's disk, and watch the system heal itself (heartbeat detection,
re-replication, stale replica deletion, garbage collection) in a live
event feed.

It supervises the master and chunk servers as child processes (so failure
injection is a real SIGKILL), performs file operations by invoking the
regular gfs_client_main binary, and parses the servers' logs into events.

Zero third-party dependencies: Python 3 standard library only.

Usage:
    python3 webapp/server.py            # from the repo root, after
                                        # `bazel build //...`
Environment:
    GFS_PLAYGROUND_PORT    HTTP port for the UI (default 8080)
    GFS_PLAYGROUND_CONFIG  cluster config (default webapp/config.yml)
    GFS_PLAYGROUND_LOGS    log directory (default logs/playground)
"""

import json
import os
import re
import shutil
import signal
import subprocess
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.environ.get("GFS_PLAYGROUND_CONFIG", "webapp/config.yml")
LOG_DIR = os.path.join(ROOT, os.environ.get("GFS_PLAYGROUND_LOGS",
                                            "logs/playground"))
HTTP_PORT = int(os.environ.get("GFS_PLAYGROUND_PORT")
                or os.environ.get("PORT") or 8080)
STATIC_DIR = os.path.join(ROOT, "webapp", "static")

MASTER_BIN = os.path.join(
    ROOT, "bazel-bin/src/server/master_server/run_master_server_main")
CHUNK_BIN = os.path.join(
    ROOT, "bazel-bin/src/server/chunk_server/run_chunk_server_main")
CLIENT_BIN = os.path.join(ROOT, "bazel-bin/gfs_client_main")

# Kept inside data/dbs so the docker volume persists it together with
# the cluster state
FILES_REGISTRY = os.path.join(ROOT, "data", "dbs",
                              "playground_files.json")

FILENAME_RE = re.compile(r"^/[A-Za-z0-9._\-/]+$")

# ----------------------------------------------------------------------------
# Minimal YAML reading (only what our config needs: two-level nested maps)
# ----------------------------------------------------------------------------


def load_cluster_config(path):
    """Parse the subset of config.yml the playground needs."""
    servers = []
    section = None
    subsection = None
    network = {}
    leveldb = {}
    server_lists = {"master_servers": [], "chunk_servers": []}
    current_server = None
    with open(os.path.join(ROOT, path)) as f:
        for raw in f:
            line = raw.split("#", 1)[0].rstrip()
            if not line.strip():
                continue
            indent = len(line) - len(line.lstrip())
            text = line.strip()
            if indent == 0:
                section = text.rstrip(":")
                subsection = None
                continue
            if section == "servers" and text.startswith("- "):
                server_lists[subsection].append(text[2:].strip())
            elif section == "servers" and text.endswith(":"):
                subsection = text.rstrip(":")
            elif section == "network":
                if indent == 2 and text.endswith(":"):
                    current_server = text.rstrip(":")
                elif indent >= 4 and ":" in text and current_server:
                    key, _, value = text.partition(":")
                    network.setdefault(current_server, {})[key.strip()] = \
                        value.strip()
            elif section == "disk":
                if indent == 2 and text.rstrip(":") == "leveldb":
                    subsection = "leveldb"
                elif subsection == "leveldb" and ":" in text:
                    key, _, value = text.partition(":")
                    leveldb[key.strip()] = value.strip()
    for name in server_lists["master_servers"]:
        servers.append({
            "name": name, "role": "master",
            "port": int(network.get(name, {}).get("port", 0)),
            "db": leveldb.get(name, ""),
        })
    for name in server_lists["chunk_servers"]:
        servers.append({
            "name": name, "role": "chunk",
            "port": int(network.get(name, {}).get("port", 0)),
            "db": leveldb.get(name, ""),
        })
    return servers


# ----------------------------------------------------------------------------
# Process supervision
# ----------------------------------------------------------------------------


class ManagedServer:
    """One supervised GFS server process."""

    def __init__(self, spec):
        self.name = spec["name"]
        self.role = spec["role"]
        self.port = spec["port"]
        self.db_path = os.path.join(ROOT, spec["db"]) if spec["db"] else None
        self.log_path = os.path.join(LOG_DIR, self.name + ".log")
        self.proc = None
        self.started_at = None
        self.lock = threading.Lock()

    def command(self):
        if self.role == "master":
            return [MASTER_BIN, "--config_path=" + CONFIG_PATH,
                    "--master_name=" + self.name]
        return [CHUNK_BIN, "--config_path=" + CONFIG_PATH,
                "--chunk_server_name=" + self.name]

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self):
        with self.lock:
            if self.alive():
                return False
            log = open(self.log_path, "ab")
            log.write(("\n==== playground: starting %s at %s ====\n" % (
                self.name, time.strftime("%H:%M:%S"))).encode())
            log.flush()
            self.proc = subprocess.Popen(
                self.command(), cwd=ROOT, stdout=log, stderr=log,
                start_new_session=True)
            log.close()
            self.started_at = time.time()
            return True

    def kill(self):
        with self.lock:
            if not self.alive():
                return False
            self.proc.kill()
            self.proc.wait(timeout=10)
            return True

    def wipe_disk(self):
        """Simulate disk loss: the server must be down; its DB is deleted."""
        with self.lock:
            if self.alive() or not self.db_path:
                return False
            shutil.rmtree(self.db_path, ignore_errors=True)
            return True

    def db_size(self):
        if not self.db_path or not os.path.isdir(self.db_path):
            return 0
        total = 0
        for dirpath, _, filenames in os.walk(self.db_path):
            for filename in filenames:
                try:
                    total += os.path.getsize(os.path.join(dirpath, filename))
                except OSError:
                    pass
        return total

    def status(self):
        return {
            "name": self.name,
            "role": self.role,
            "port": self.port,
            "running": self.alive(),
            "pid": self.proc.pid if self.alive() else None,
            "uptime_s": int(time.time() - self.started_at)
                        if self.alive() and self.started_at else 0,
            "db_size_bytes": self.db_size(),
            "db_present": bool(self.db_path and os.path.isdir(self.db_path)),
        }


# ----------------------------------------------------------------------------
# Log parsing -> event feed
# ----------------------------------------------------------------------------

EVENT_PATTERNS = [
    ("server-joined", "info",
     re.compile(r"Registering chunk server (\S+)"),
     lambda m: "Chunk server %s registered with the master" % m.group(1)),
    ("server-lost", "danger",
     re.compile(r"Unregistering chunk server: (\S+)"),
     lambda m: "Heartbeat failed — master unregistered %s" % m.group(1)),
    ("rereplication", "repair",
     re.compile(r"Re-replicating chunk (\S+) \(version (\d+)\): instructing "
                r"(\S+) to clone it from (\S+)"),
     lambda m: "Re-replicating chunk %s v%s: %s cloning from %s" % (
         m.group(1), m.group(2), m.group(3), m.group(4))),
    ("rereplication-done", "success",
     re.compile(r"Chunk (\S+) successfully re-replicated to (\S+)"),
     lambda m: "Chunk %s re-replicated to %s" % (m.group(1), m.group(2))),
    ("clone-done", "success",
     re.compile(r"Successfully cloned chunk (\S+) of version (\d+) "
                r"\((\d+) bytes\) from (\S+)"),
     lambda m: "Cloned chunk %s v%s (%s bytes) from %s" % (
         m.group(1), m.group(2), m.group(3), m.group(4))),
    ("stale-replica", "warn",
     re.compile(r"Chunk handle (\S+) at version (\d+) is stale \(master has "
                r"version (\d+)\)"),
     lambda m: "Stale replica of chunk %s detected (v%s < master's v%s) — "
               "ordered deletion" % (m.group(1), m.group(2), m.group(3))),
    ("gc-ordered", "warn",
     re.compile(r"Chunk handle (\S+) no longer exists in the master's "
                r"metadata"),
     lambda m: "Chunk %s belongs to a deleted file — garbage collection "
               "ordered" % m.group(1)),
    ("gc-done", "info",
     re.compile(r"Received stale / deleted chunk handle (\S+)"),
     lambda m: "Garbage-collected chunk %s from disk" % m.group(1)),
    ("lease", "info",
     re.compile(r"Grant lease request for chunk (\S+) at (\S+) accepted"),
     lambda m: "Lease for chunk %s granted to %s (primary)" % (
         m.group(1), m.group(2))),
    ("lease-reused", "info",
     re.compile(r"Reusing existing lease for (\S+), held by (\S+)"),
     lambda m: "Reused valid lease for chunk %s (primary %s)" % (
         m.group(1), m.group(2))),
    ("version", "info",
     re.compile(r"Advancing chunk version for chunk handle (\S+) from (\d+) "
                r"to (\d+)"),
     lambda m: "Chunk %s version advanced %s → %s (new lease)" % (
         m.group(1), m.group(2), m.group(3))),
    ("master-recovered", "success",
     re.compile(r"Metadata store recovered (\d+) file\(s\), (\d+) chunk"),
     lambda m: "Master recovered %s file(s) and %s chunk version record(s) "
               "from its metadata store" % (m.group(1), m.group(2))),
    ("chunk-created", "info",
     re.compile(r"Chunk handle created: (\S+) for file (\S+)"),
     lambda m: "Chunk %s created for file %s" % (m.group(1), m.group(2))),
    ("corruption", "danger",
     re.compile(r"Data corruption detected while (\w+) chunk (\S+)"),
     lambda m: "Checksum mismatch detected while %s chunk %s" % (
         m.group(1), m.group(2))),
]

GLOG_LINE = re.compile(r"^[IWEF](\d{8} \d{2}:\d{2}:\d{2})")


class EventCollector:
    """Incrementally tails server logs and turns known lines into events."""

    def __init__(self, servers):
        self.servers = servers
        self.offsets = {}
        self.events = deque(maxlen=400)
        self.next_id = 1
        self.lock = threading.Lock()

    def add(self, server, etype, severity, text, when):
        self.events.append({
            "id": self.next_id, "server": server, "type": etype,
            "severity": severity, "text": text, "time": when,
        })
        self.next_id += 1

    def poll(self):
        with self.lock:
            for server in self.servers:
                path = server.log_path
                try:
                    size = os.path.getsize(path)
                except OSError:
                    continue
                offset = self.offsets.get(path, 0)
                if size < offset:  # log truncated/rotated
                    offset = 0
                if size == offset:
                    continue
                with open(path, "rb") as f:
                    f.seek(offset)
                    chunk = f.read(min(size - offset, 4 * 1024 * 1024))
                    self.offsets[path] = offset + len(chunk)
                for line in chunk.decode("utf-8", "replace").splitlines():
                    timestamp = None
                    glog = GLOG_LINE.match(line)
                    if glog:
                        timestamp = glog.group(1)[9:]  # HH:MM:SS
                    for etype, severity, pattern, render in EVENT_PATTERNS:
                        match = pattern.search(line)
                        if match:
                            self.add(server.name, etype, severity,
                                     render(match),
                                     timestamp or time.strftime("%H:%M:%S"))
                            break

    def since(self, after_id):
        self.poll()
        with self.lock:
            return [e for e in self.events if e["id"] > after_id]


# ----------------------------------------------------------------------------
# File registry (the master has no "list files" RPC; the playground remembers
# the files created through it, persisted in the data volume)
# ----------------------------------------------------------------------------


class FileRegistry:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.files = {}
        try:
            with open(path) as f:
                self.files = json.load(f)
        except (OSError, ValueError):
            self.files = {}

    def _save(self):
        try:
            with open(self.path, "w") as f:
                json.dump(self.files, f)
        except OSError:
            pass

    def record(self, filename, **fields):
        with self.lock:
            entry = self.files.setdefault(filename, {})
            entry.update(fields)
            self._save()

    def remove(self, filename):
        with self.lock:
            self.files.pop(filename, None)
            self._save()

    def list(self):
        with self.lock:
            return [dict(name=k, **v) for k, v in sorted(self.files.items())]


# ----------------------------------------------------------------------------
# GFS client operations (shell out to the regular command line client)
# ----------------------------------------------------------------------------


def run_client(mode, filename, data=None, offset=None, nbytes=None):
    cmd = [CLIENT_BIN, "--config_path=" + CONFIG_PATH, "--mode=" + mode,
           "--filename=" + filename]
    if data is not None:
        cmd.append("--data=" + data)
    if offset is not None:
        cmd.append("--offset=" + str(offset))
    if nbytes is not None:
        cmd.append("--nbytes=" + str(nbytes))
    try:
        proc = subprocess.run(cmd, cwd=ROOT, capture_output=True,
                              timeout=60, text=True)
    except subprocess.TimeoutExpired:
        return {"ok": False, "detail": "client timed out after 60s"}
    output = (proc.stdout or "") + (proc.stderr or "")
    result = {"ok": proc.returncode == 0}
    if mode == "read":
        read_match = re.search(r"Data read: '(.*)'", output, re.S)
        bytes_match = re.search(r"Read (\d+) bytes", output)
        result["data"] = read_match.group(1) if read_match else ""
        result["bytes_read"] = int(bytes_match.group(1)) if bytes_match else 0
    if not result["ok"]:
        errors = [l for l in output.splitlines() if l.startswith("E")]
        result["detail"] = (errors[-1].split("] ", 1)[-1]
                            if errors else "operation failed")
    return result


# ----------------------------------------------------------------------------
# HTTP API
# ----------------------------------------------------------------------------


class Playground:
    def __init__(self):
        os.makedirs(LOG_DIR, exist_ok=True)
        os.makedirs(os.path.join(ROOT, "data", "dbs"), exist_ok=True)
        self.servers = [ManagedServer(s)
                        for s in load_cluster_config(CONFIG_PATH)]
        self.by_name = {s.name: s for s in self.servers}
        self.collector = EventCollector(self.servers)
        self.registry = FileRegistry(FILES_REGISTRY)
        self.started_at = time.time()

    def start_all(self):
        # Master first, then chunk servers (they report themselves to it)
        for server in self.servers:
            if server.role == "master":
                server.start()
        time.sleep(0.5)
        for server in self.servers:
            if server.role == "chunk":
                server.start()

    def shutdown(self):
        for server in self.servers:
            try:
                server.kill()
            except Exception:
                pass


PLAYGROUND = None  # initialized in main()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # quiet the request log
        pass

    # -- helpers ------------------------------------------------------------

    def send_json(self, payload, code=200):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0 or length > 1024 * 1024:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode())
        except ValueError:
            return {}

    def server_from(self, body):
        name = body.get("server", "")
        return PLAYGROUND.by_name.get(name)

    # -- routes -------------------------------------------------------------

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        route = parsed.path

        if route == "/" or route == "/index.html":
            return self.send_static("index.html", "text/html; charset=utf-8")

        if route == "/api/status":
            return self.send_json({
                "servers": [s.status() for s in PLAYGROUND.servers],
                "files": PLAYGROUND.registry.list(),
                "uptime_s": int(time.time() - PLAYGROUND.started_at),
            })

        if route == "/api/events":
            after = int(query.get("after", ["0"])[0])
            return self.send_json(
                {"events": PLAYGROUND.collector.since(after)})

        if route == "/api/logs":
            name = query.get("server", [""])[0]
            server = PLAYGROUND.by_name.get(name)
            if not server:
                return self.send_json({"error": "unknown server"}, 404)
            lines = int(query.get("lines", ["120"])[0])
            try:
                with open(server.log_path, "rb") as f:
                    f.seek(0, os.SEEK_END)
                    f.seek(max(0, f.tell() - 256 * 1024))
                    tail = f.read().decode("utf-8", "replace").splitlines()
            except OSError:
                tail = []
            return self.send_json({"lines": tail[-lines:]})

        if route == "/api/files/read":
            filename = query.get("filename", [""])[0]
            if not FILENAME_RE.match(filename):
                return self.send_json({"ok": False,
                                       "detail": "invalid filename"}, 400)
            offset = int(query.get("offset", ["0"])[0])
            nbytes = min(int(query.get("nbytes", ["256"])[0]), 65536)
            return self.send_json(
                run_client("read", filename, offset=offset, nbytes=nbytes))

        return self.send_json({"error": "not found"}, 404)

    def do_POST(self):
        route = urlparse(self.path).path
        body = self.read_body()

        if route.startswith("/api/files/"):
            filename = body.get("filename", "")
            if not FILENAME_RE.match(filename):
                return self.send_json({"ok": False,
                                       "detail": "invalid filename"}, 400)

            if route == "/api/files/create":
                result = run_client("create", filename)
                if result["ok"]:
                    PLAYGROUND.registry.record(
                        filename, created=time.strftime("%H:%M:%S"), size=0)
                return self.send_json(result)

            if route == "/api/files/write":
                data = str(body.get("data", ""))[:65536]
                offset = int(body.get("offset", 0))
                result = run_client("write", filename, data=data,
                                    offset=offset)
                if result["ok"]:
                    PLAYGROUND.registry.record(
                        filename, size=offset + len(data),
                        last_write=time.strftime("%H:%M:%S"))
                return self.send_json(result)

            if route == "/api/files/delete":
                result = run_client("remove", filename)
                if result["ok"]:
                    PLAYGROUND.registry.remove(filename)
                return self.send_json(result)

        if route.startswith("/api/chaos/"):
            server = self.server_from(body)
            if not server:
                return self.send_json({"ok": False,
                                       "detail": "unknown server"}, 400)

            if route == "/api/chaos/kill":
                killed = server.kill()
                return self.send_json({"ok": killed,
                                       "detail": None if killed
                                       else "already down"})

            if route == "/api/chaos/wipe":
                server.kill()
                time.sleep(0.2)
                wiped = server.wipe_disk()
                return self.send_json({"ok": wiped,
                                       "detail": None if wiped
                                       else "cannot wipe while running"})

            if route == "/api/chaos/start":
                started = server.start()
                return self.send_json({"ok": started,
                                       "detail": None if started
                                       else "already running"})

            if route == "/api/chaos/restart":
                server.kill()
                time.sleep(0.3)
                started = server.start()
                return self.send_json({"ok": started})

        return self.send_json({"error": "not found"}, 404)

    def send_static(self, filename, content_type):
        path = os.path.join(STATIC_DIR, filename)
        try:
            with open(path, "rb") as f:
                body = f.read()
        except OSError:
            self.send_json({"error": "missing static file"}, 500)
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def main():
    global PLAYGROUND
    for binary in (MASTER_BIN, CHUNK_BIN, CLIENT_BIN):
        if not os.path.exists(binary):
            raise SystemExit(
                "Missing %s — run `bazel build //...` first (the Docker "
                "image does this at build time)." % binary)

    PLAYGROUND = Playground()
    PLAYGROUND.start_all()

    httpd = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), Handler)

    def stop(_signum, _frame):
        PLAYGROUND.shutdown()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    print("cppGFS2.0 Playground running at http://localhost:%d" % HTTP_PORT,
          flush=True)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
