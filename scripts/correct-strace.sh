#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/kafs-test.correct-strace.XXXXXX")
IMG="$WORKDIR/eio-minimal.img"
MNT="$WORKDIR/mnt"
STRACE_LOG="$WORKDIR/correct-strace.log"
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

echo "KAFS FSCK Strace: File Operations Analysis"
echo "==========================================="
echo ""

mkdir -p "$MNT"

echo "[Step 1] Creating and populating image..."
truncate -s 50M "$IMG"
./src/mkfs.kafs "$IMG" >/dev/null 2>&1

export KAFS_IMAGE="$IMG"
./src/kafs "$MNT" -f -s > /dev/null 2>&1 &
KAFS_PID=$!
sleep 2

if grep -Fq "$MNT" /proc/mounts 2>/dev/null; then
  mkdir -p "$MNT/testdir"
  dd if=/dev/urandom of="$MNT/testdir/data" bs=1K count=100 2>/dev/null
  sync
  fusermount3 -u "$MNT" 2>/dev/null || true
fi
sleep 1

echo "[Step 2] Running fsck.kafs with strace..."
echo ""

strace -f -e trace=file,pread64,pwrite64,read,write,getdents64,fstat,lstat \
  -o "$STRACE_LOG" \
  ./src/fsck.kafs "$IMG" 2>&1 | head -10

echo "[Step 3] Analyzing strace output..."
echo ""
echo "=== pread64 syscalls to image file (fd=3) ==="
grep "pread64(3," "$STRACE_LOG" | head -8 | cat -n

echo ""
echo "=== fstat syscalls on image file ==="
grep "fstat(3," "$STRACE_LOG" | cat -n

echo ""
echo "=== Failed read/write operations ==="
grep -E "\(3,.*\) = -" "$STRACE_LOG" || echo "  (None - all successful)"

echo ""
echo "=== Summary of syscalls ==="
echo "pread64 to image:  $(grep 'pread64(3,' "$STRACE_LOG" | wc -l) calls"
echo "pwrite64 to image: $(grep 'pwrite64(3,' "$STRACE_LOG" | wc -l) calls"
echo "fstat on image:    $(grep 'fstat(3,' "$STRACE_LOG" | wc -l) calls"
echo ""
echo "✓ Complete"
