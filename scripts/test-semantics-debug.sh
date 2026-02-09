#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/kafs-test.semantics.XXXXXX")

DEFAULT_IMG="$ROOT_DIR/src/semantics.img"
if [[ -f "$ROOT_DIR/semantics.img" ]]; then
  DEFAULT_IMG="$ROOT_DIR/semantics.img"
fi

IMG="${KAFS_SEMANTICS_IMG:-$DEFAULT_IMG}"
MNT="$WORKDIR/mnt"
LOG="$WORKDIR/kafs-semantics-eof.log"
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

echo "=== KAFS Semantics Image Workload with Debug Logging ==="
echo "Image: $IMG"
echo "Time: $(date)"
echo ""

mkdir -p "$MNT"

echo "Mounting with KAFS (KAFS_DEBUG=1)..."

export KAFS_DEBUG=1
export KAFS_IMAGE="$IMG"

timeout 300 "$KAFS" "$MNT" -f -s > "$LOG" 2>&1 &
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
  [[ -f "$LOG" ]] && head -50 "$LOG"
  exit 1
fi

sleep 1

echo ""
echo "Running workload..."

echo "Listing directories..."
find "$MNT" -type f 2>/dev/null | head -50

echo ""
echo "Computing checksums..."
find "$MNT" -type f 2>/dev/null | head -20 | while read -r f; do
  md5sum "$f" 2>/dev/null || true
done

echo ""
echo "Parallel reads..."
find "$MNT" -type f 2>/dev/null | head -30 | xargs -P 8 -I {} md5sum {} 2>/dev/null || true

echo ""
echo "Recursive operations..."
timeout 20 cp -r "$MNT" "$WORKDIR/kafs-semantics-copy" 2>/dev/null && echo "Copy done" || echo "Copy timeout/failed"

echo ""
echo "Unmounting..."

fusermount3 -u "$MNT" 2>/dev/null || umount "$MNT" 2>/dev/null || true
sleep 2

kill "$KAFS_PID" 2>/dev/null || true
wait "$KAFS_PID" 2>/dev/null || true
KAFS_PID=""

echo "Done"
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
echo "=== Errors and Warnings ==="
grep -i "error\|warning" "$LOG" 2>/dev/null | head -20 || echo "(none)"

echo ""
echo "=== EOF Diagnostics ==="
grep "kafs_blk_read.*EOF" "$LOG" 2>/dev/null || echo "(none)"

echo ""
echo "=== Unexpected Errors ==="
grep -i "unexpected" "$LOG" 2>/dev/null | head -20 || echo "(none)"

echo ""
echo "=== Summary Stats ==="
echo "Total log lines: $LOGLINES"
grep -c "kafs_blk_read" "$LOG" 2>/dev/null || true

echo ""
echo "=== Last 100 lines ==="
tail -100 "$LOG"
