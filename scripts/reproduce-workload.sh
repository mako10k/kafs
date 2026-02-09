#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/kafs-test.workload.XXXXXX")
IMG="$WORKDIR/test-fresh.img"
MNT="$WORKDIR/mnt"
LOG="$WORKDIR/kafs-debug-test.log"
KAFS="$ROOT_DIR/src/kafs"
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

echo "=== KAFS Fresh Workload Reproduction with Debug Logging ==="
echo "Time: $(date)"
echo ""

mkdir -p "$MNT"

echo "1. Creating fresh KAFS filesystem image..."
SIZE=$((64 * 1024 * 1024))
truncate -s "$SIZE" "$IMG"
./src/mkfs.kafs "$IMG" > /dev/null 2>&1
echo "✓ Image created: $IMG (64MB)"

echo ""
echo "2. Mounting with KAFS (KAFS_DEBUG=1)..."

export KAFS_DEBUG=1
export KAFS_IMAGE="$IMG"

"$KAFS" "$MNT" -f -s > "$LOG" 2>&1 &
KAFS_PID=$!
echo "KAFS PID: $KAFS_PID"

MOUNTED=0
for i in {1..100}; do
  if grep -Fq "$MNT" /proc/mounts 2>/dev/null; then
    MOUNTED=1
    echo "✓ Mounted"
    break
  fi
  sleep 0.1
done

if [[ $MOUNTED -eq 0 ]]; then
  echo "✗ Mount failed"
  [[ -f "$LOG" ]] && echo "Log:" && cat "$LOG"
  exit 1
fi

sleep 1

echo ""
echo "3. Git workload operations (simulating git clone)..."

echo "Creating .git structure..."
mkdir -p "$MNT/.git/hooks" 2>/dev/null || true
mkdir -p "$MNT/.git/objects" 2>/dev/null || true

echo "Writing files..."
for i in {0..30}; do
  SIZE=$((10000 + RANDOM * 50))
  dd if=/dev/urandom of="$MNT/.git/objects/object_$i" bs=1 count=$SIZE 2>/dev/null
done

echo "#!/bin/bash" > "$MNT/.git/hooks/pre-commit" 2>/dev/null || true
chmod +x "$MNT/.git/hooks/pre-commit" 2>/dev/null || true

echo "Listing structure..."
ls -lR "$MNT/.git" 2>/dev/null | head -50

echo ""
echo "Directory stat:"
find "$MNT" -type f 2>/dev/null | wc -l
echo "files found"

echo ""
echo "Computing file hashes..."
find "$MNT/.git/objects" -type f 2>/dev/null | head -10 | while read -r f; do
  md5sum "$f" 2>/dev/null
 done

echo ""
echo "Parallel reads (4 workers)..."
find "$MNT/.git/objects" -type f 2>/dev/null | head -20 | xargs -P 4 -I {} md5sum {} 2>/dev/null | head -20

echo ""
echo "Recursive copy..."
timeout 15 cp -r "$MNT" "$WORKDIR/kafs-test-copy" 2>/dev/null && echo "✓ Copy done" || echo "✓ Copy completed/timed out"

echo ""
echo "4. Unmounting..."

fusermount3 -u "$MNT" 2>/dev/null || umount "$MNT" 2>/dev/null || true
sleep 1
kill "$KAFS_PID" 2>/dev/null || true
wait "$KAFS_PID" 2>/dev/null || true
KAFS_PID=""

echo "✓ Done"
echo ""
echo "=== Log Analysis ==="

if [[ ! -f "$LOG" ]]; then
  echo "ERROR: Log file not found!"
  exit 1
fi

LOGLINES=$(wc -l < "$LOG")
echo "Log file: $LOG"
echo "Total lines: $LOGLINES"

echo ""
echo "=== Key Errors/Warnings/EOF Lines ==="

echo "--- Errors ---"
grep -i "error" "$LOG" 2>/dev/null | head -20 || echo "(none)"

echo ""
echo "--- Warnings ---"
grep -i "warning" "$LOG" 2>/dev/null | head -20 || echo "(none)"

echo ""
echo "--- EOF Diagnostics (kafs_blk_read: unexpected EOF) ---"
grep "kafs_blk_read.*EOF" "$LOG" 2>/dev/null || echo "(none)"

echo ""
echo "--- Unexpected Errors ---"
grep -i "unexpected" "$LOG" 2>/dev/null || echo "(none)"

echo ""
echo "=== Last 100 lines of KAFS Debug Log ==="
tail -100 "$LOG"

echo ""
echo "=== Reproduction Complete ==="
