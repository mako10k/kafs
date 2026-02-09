#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/kafs-test.min-fsck.XXXXXX")
IMG="$WORKDIR/test-eio-repro.img"
MNT="$WORKDIR/mnt"
STRACE_LOG="$WORKDIR/strace-fsck-eio.log"
KAFS_PID=""

cleanup() {
  set +e
  fusermount3 -u "$MNT" 2>/dev/null || umount "$MNT" 2>/dev/null || true
  if [[ -n "${KAFS_PID:-}" ]]; then
    kill "$KAFS_PID" 2>/dev/null || true
    wait "$KAFS_PID" 2>/dev/null || true
  fi
  rm -rf "$WORKDIR" 2>/dev/null || true
}
trap cleanup EXIT

# Create a fresh image
echo "Creating fresh KAFS image..."
truncate -s 100M "$IMG"
./src/mkfs.kafs "$IMG" >/dev/null 2>&1
echo "✓ Created $IMG"

# Write some test data
mkdir -p "$MNT"
export KAFS_DEBUG=0
export KAFS_IMAGE="$IMG"
./src/kafs "$MNT" -f -s > /dev/null 2>&1 &
KAFS_PID=$!

# Wait for mount
sleep 2
if ! grep -Fq "$MNT" /proc/mounts 2>/dev/null; then
  echo "✗ Mount failed"
  exit 1
fi

echo "✓ Mounted, creating test files..."
mkdir -p "$MNT/testdir"
dd if=/dev/urandom of="$MNT/testdir/file1.bin" bs=1K count=100 2>/dev/null
dd if=/dev/urandom of="$MNT/testdir/file2.bin" bs=1K count=50 2>/dev/null
echo "test data" > "$MNT/testdir/file3.txt"
sync

# Unmount
fusermount3 -u "$MNT" 2>/dev/null || umount "$MNT" 2>/dev/null || true
sleep 1

echo ""
echo "=== Running fsck.kafs with strace (capturing EIO) ==="
echo ""

strace -f -e trace=file,pread64,pwrite64,read,write,openat,getdents64,fstat,lstat,statx -o "$STRACE_LOG" \
  ./src/fsck.kafs "$IMG" 2>&1

echo ""
echo "=== Strace output (filtered for errors) ==="
echo ""
grep -E "pread|pwrite|openat|read\(" "$STRACE_LOG" | grep -E "= -[0-9]|EIO" || echo "(No errors found in normal case)"

echo ""
echo "=== Full strace for pread/pwrite operations ==="
grep "pread\|pwrite" "$STRACE_LOG" | head -20 || echo "(None)"

echo ""
echo "Done (cleaned up)."
