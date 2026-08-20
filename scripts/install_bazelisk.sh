#!/usr/bin/env bash
#
# Install bazelisk, a launcher that automatically downloads and runs the
# Bazel version pinned in this repository's .bazelversion file.
# Works on Linux and macOS, for both x86_64 and arm64.
#
# Usage:
#   chmod +x scripts/install_bazelisk.sh
#   scripts/install_bazelisk.sh
#
# Alternatively, install with Homebrew (macOS/Linux): brew install bazelisk

set -euo pipefail

case "$(uname -s)" in
  Linux)  os="linux" ;;
  Darwin) os="darwin" ;;
  *) echo "Unsupported OS: $(uname -s)"; exit 1 ;;
esac

case "$(uname -m)" in
  x86_64)        arch="amd64" ;;
  arm64|aarch64) arch="arm64" ;;
  *) echo "Unsupported architecture: $(uname -m)"; exit 1 ;;
esac

url="https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-${os}-${arch}"
install_dir="${INSTALL_DIR:-/usr/local/bin}"

echo "Downloading ${url}..."
tmpfile="$(mktemp)"
curl -fsSL "${url}" -o "${tmpfile}"
chmod +x "${tmpfile}"

echo "Installing to ${install_dir}/bazel (may prompt for sudo)..."
if [[ -w "${install_dir}" ]]; then
  mv "${tmpfile}" "${install_dir}/bazel"
else
  sudo mv "${tmpfile}" "${install_dir}/bazel"
fi

echo "Installed: $(command -v bazel)"
bazel version || true
