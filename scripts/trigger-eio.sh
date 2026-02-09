#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/kafs-test.trigger-eio.XXXXXX")
IMG="$WORKDIR/test-eio-corrupted.img"
STRACE_LOG="$WORKDIR/strace-eio-trigger.log"

cleanup() {
  rm -rf "$WORKDIR" 2>/dev/null || true
}
trap cleanup EXIT

echo "Creating test image..."
truncate -s 100M "$IMG"
./src/mkfs.kafs "$IMG" >/dev/null 2>&1

echo ""
echo "Corrupting image (truncating to simulate short read)..."
ORIG_SIZE=$(stat -c%s "$IMG")
NEW_SIZE=$((ORIG_SIZE / 2))
truncate -s "$NEW_SIZE" "$IMG"
echo "Truncated from $ORIG_SIZE to $NEW_SIZE bytes"

echo ""
echo "=== Running fsck.kafs with strace (should hit EIO on short read) ==="
echo ""

strace -f -e trace=pread64,pwrite64,fstat,lstat,openat \
  -o "$STRACE_LOG" \
  ./src/fsck.kafs "$IMG" 2>&1 | head -30

echo ""
echo "=== Strace: Failed pread/pwrite operations ==="
grep "= -[0-9]" "$STRACE_LOG" | grep -E "pread|pwrite|fstat" || echo "(None)"

echo ""
echo "=== Strace: Last pread/pwrite before potential EOF ==="
grep "pread64" "$STRACE_LOG" | tail -10

echo ""
echo "Cleanup..."
