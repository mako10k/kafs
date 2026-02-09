#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/kafs-test.final-strace.XXXXXX")
IMG="$WORKDIR/final-test.img"
MNT="$WORKDIR/mnt"
STRACE_LOG="$WORKDIR/final-strace.log"
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

echo "=========================================="
echo "MINIMAL REPRODUCTION: KAFS FSCK + STRACE"
echo "=========================================="
echo ""

mkdir -p "$MNT"

echo "[1] Creating KAFS image..."
truncate -s 100M "$IMG"
./src/mkfs.kafs "$IMG" >/dev/null 2>&1

echo "[2] Mounting and writing files..."
export KAFS_IMAGE="$IMG"
./src/kafs "$MNT" -f -s > /dev/null 2>&1 &
KAFS_PID=$!
sleep 2

if ! grep -Fq "$MNT" /proc/mounts 2>/dev/null; then
  echo "ERROR: Mount failed"
  exit 1
fi

mkdir -p "$MNT/data"
dd if=/dev/urandom of="$MNT/data/testfile" bs=1K count=500 2>/dev/null
sync

echo "[3] Unmounting..."
fusermount3 -u "$MNT" 2>/dev/null || true
sleep 1

echo "[4] Running fsck.kafs with strace..."
echo ""

strace -f -e trace=file,read,write,getdents64,statx,lstat,fstat,rename,unlink,fsync,fdatasync \
  -o "$STRACE_LOG" \
  ./src/fsck.kafs "$IMG" 2>&1

echo ""
echo "=========================================="
echo "STRACE ANALYSIS: Key syscalls to IMG file"
echo "=========================================="
echo ""

echo "=== Successful pread64 calls to image file (fd=3) ==="
echo "(Showing offset, size, return value)"
echo ""
grep "pread64(3," "$STRACE_LOG" | head -5 | sed 's/.*pread64(3, \(.*\), \([0-9]*\), \([0-9]*\)) = \(.*\)/  offset=\3 size=\2 returned=\4/'

echo ""
echo "=== Failed syscalls (if any) ==="
grep "= -[0-9]" "$STRACE_LOG" | head -10 || echo "  (None - all operations successful)"

echo ""
echo "=== fstat calls on image file ==="
grep "fstat(3," "$STRACE_LOG" | head -3

echo ""
echo "=== Summary ==="
echo "Total file operations traced:"
grep -E "pread64\(3|pwrite64\(3|fstat\(3|openat.*img" "$STRACE_LOG" | wc -l | xargs echo "  Count:"

echo "✓ Done"
