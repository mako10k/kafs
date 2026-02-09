#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/kafs-test.run-eio.XXXXXX")
IMG="$WORKDIR/test-eio-trigger.img"
STRACE_LOG="$WORKDIR/strace-with-eio.log"

cleanup() {
  rm -rf "$WORKDIR" 2>/dev/null || true
}
trap cleanup EXIT

echo "Setting up test image..."
truncate -s 100M "$IMG"
./src/mkfs.kafs "$IMG" >/dev/null 2>&1

echo ""
echo "=== Running fsck.kafs with EIO injection and strace ==="
echo ""

LD_PRELOAD=./force-eio.so strace -f -e trace=pread64,pwrite64,fstat,lstat,openat,read,write \
  -o "$STRACE_LOG" \
  ./src/fsck.kafs "$IMG" 2>&1 | head -50

echo ""
echo "=== Captured syscalls with EIO injection ==="
echo ""
grep -E "pread64|EIO|errno|= -[0-9]" "$STRACE_LOG" | head -30

echo ""
echo "=== Key findings: pread64 syscalls near error ==="
grep -B2 -A2 "pread64.*=" "$STRACE_LOG" | head -40

echo ""
echo "Cleanup..."
