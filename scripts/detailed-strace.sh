#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/kafs-test.detailed-strace.XXXXXX")
IMG="$WORKDIR/test-strace.img"
MNT="$WORKDIR/mnt"
STRACE_LOG="$WORKDIR/strace-detailed.log"
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

truncate -s 100M "$IMG"
./src/mkfs.kafs "$IMG" >/dev/null 2>&1

mkdir -p "$MNT"
export KAFS_IMAGE="$IMG"
./src/kafs "$MNT" -f -s > /dev/null 2>&1 &
KAFS_PID=$!
sleep 2

if grep -Fq "$MNT" /proc/mounts 2>/dev/null; then
  echo "Writing test files..."
  dd if=/dev/urandom of="$MNT/file1" bs=1K count=200 2>/dev/null
  dd if=/dev/urandom of="$MNT/file2" bs=1K count=100 2>/dev/null
  sync
  fusermount3 -u "$MNT" 2>/dev/null || umount "$MNT" 2>/dev/null
fi
sleep 1

echo ""
echo "=== Detailed strace of fsck.kafs (showing pread64 operations) ==="
echo ""

strace -f -v -s 100 -e trace=pread64,pwrite64,openat,fstat,lstat -o "$STRACE_LOG" \
  ./src/fsck.kafs "$IMG" 2>&1 | grep -E "pread64|pwrite64|openat.*img|ERROR|EIO" | head -20

echo ""
echo "=== Strace log: pread64 to IMG file (offset >1MB) ==="
grep "pread64.*3.*)" "$STRACE_LOG" | awk '$0 ~ /10915/ || $0 ~ /[0-9]{7,}/' | head -15

echo ""
echo "=== Summary ==="
echo "Total pread64 calls to image: $(grep 'pread64(3,' "$STRACE_LOG" | wc -l)"
echo "Total pwrite64 calls to image: $(grep 'pwrite64(3,' "$STRACE_LOG" | wc -l)"
echo "Failed operations (return < 0): $(grep 'pread64.*= -' "$STRACE_LOG" | wc -l)"
echo ""
