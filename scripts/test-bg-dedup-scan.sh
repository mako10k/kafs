#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/kafs-test.bg-dedup.XXXXXX")
IMG="$WORKDIR/bg-dedup.img"
MNT="$WORKDIR/mnt"
LOG="$WORKDIR/bg-dedup.log"
KAFS="$ROOT_DIR/src/kafs"
KAFSCTL="$ROOT_DIR/src/kafsctl"
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

json_u64() {
    local key="$1"
    local json="$2"
    echo "$json" | sed -n "s/.*\"${key}\":[[:space:]]*\([0-9][0-9]*\).*/\1/p" | head -n1
}

echo "=== KAFS Background Dedup Regression Test ==="
echo "Time: $(date)"
echo ""

mkdir -p "$MNT"

# Small HRL ratio makes ENOSPC reproducible so evict/retry path can be exercised.
SIZE=$((80 * 1024 * 1024))
truncate -s "$SIZE" "$IMG"
./src/mkfs.kafs "$IMG" --hrl-entry-ratio 0.001 >/dev/null 2>&1

echo "1. Mounting with background dedup scan enabled..."
export KAFS_DEBUG=1
export KAFS_IMAGE="$IMG"

"$KAFS" "$MNT" -f -s \
  -o pendinglog_cap_initial=2,pendinglog_cap_min=2,pendinglog_cap_max=2,bg_dedup_scan=on,bg_dedup_interval_ms=20 \
  >"$LOG" 2>&1 &
KAFS_PID=$!

MOUNTED=0
for _ in {1..120}; do
    if grep -Fq "$MNT" /proc/mounts 2>/dev/null; then
        MOUNTED=1
        break
    fi
    sleep 0.1
done

if [[ $MOUNTED -eq 0 ]]; then
    echo "✗ mount failed"
    [[ -f "$LOG" ]] && tail -n 80 "$LOG"
    exit 1
fi

chmod 777 "$MNT"

echo "2. Creating pressure and duplicate DIRECT blocks..."
cd "$MNT"

# Fill HRL with unique blocks first.
for i in $(seq 1 128); do
    dd if=/dev/urandom of="uniq-${i}.bin" bs=4096 count=1 status=none

done

# Two identical files likely land as DIRECT legacy blocks when HRL is saturated.
dd if=/dev/urandom of=dup-source.bin bs=4096 count=1 status=none
cp dup-source.bin dup-a.bin
cp dup-source.bin dup-b.bin
sync

cd - >/dev/null

echo "3. Waiting for background scan convergence..."
ok=0
last_json=""
for _ in {1..120}; do
    last_json=$("$KAFSCTL" fsstat "$MNT" --json 2>/dev/null || true)
    repl=$(json_u64 "bg_dedup_replacements" "$last_json")
    evict=$(json_u64 "bg_dedup_evicts" "$last_json")
    retry=$(json_u64 "bg_dedup_retries" "$last_json")

    repl=${repl:-0}
    evict=${evict:-0}
    retry=${retry:-0}

    if [[ "$repl" -ge 2 && "$evict" -ge 1 && "$retry" -ge 1 ]]; then
        ok=1
        break
    fi
    sleep 0.25
done

if [[ $ok -ne 1 ]]; then
    echo "✗ counters did not converge as expected"
    echo "--- fsstat ---"
    echo "$last_json"
    echo "--- log tail ---"
    tail -n 120 "$LOG" || true
    exit 1
fi

echo "✓ bg dedup counters observed"
echo "  replacements: $repl"
echo "  evicts: $evict"
echo "  retries: $retry"

echo "4. Unmounting..."
fusermount3 -u "$MNT" 2>/dev/null || umount "$MNT" 2>/dev/null || true
kill "$KAFS_PID" 2>/dev/null || true
wait "$KAFS_PID" 2>/dev/null || true
KAFS_PID=""

echo "✓ regression test passed"
