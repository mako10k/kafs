#!/usr/bin/env bash
# measure-lock-locality-perf.sh
# Compare lock-locality metrics for metadata-heavy workload (issue #59).
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

GIT_COMMON_DIR=$(git rev-parse --path-format=absolute --git-common-dir)
REPO_ROOT=$(dirname "$GIT_COMMON_DIR")

BASELINE_DIR="${1:-$REPO_ROOT/.worktree/issue-59-baseline}"
OPTIMIZED_DIR="$ROOT_DIR"
BASELINE_REV="${BASELINE_REV:-f9a6247}"
STAMP=$(date +%Y%m%d-%H%M%S)
OUT_DIR="$ROOT_DIR/report/perf/lock-locality-$STAMP"
mkdir -p "$OUT_DIR"

if ! command -v npm >/dev/null 2>&1; then
  echo "SKIP: npm not found"
  exit 77
fi

if [[ ! -x "$BASELINE_DIR/src/kafs" ]]; then
  echo "[setup] baseline worktree missing or not built: $BASELINE_DIR"
  echo "[setup] preparing baseline from rev=$BASELINE_REV"
  git worktree add "$BASELINE_DIR" "$BASELINE_REV" >/dev/null
  (
    cd "$BASELINE_DIR"
    autoreconf -fi >/dev/null
    ./configure --quiet
    make -j"$(nproc)" >/dev/null
  )
fi

if [[ ! -x "$OPTIMIZED_DIR/src/kafs" || ! -x "$OPTIMIZED_DIR/src/mkfs.kafs" || ! -x "$OPTIMIZED_DIR/src/fsck.kafs" ]]; then
  echo "[setup] optimized binaries missing. building in $OPTIMIZED_DIR"
  (
    cd "$OPTIMIZED_DIR"
    make -j"$(nproc)" >/dev/null
  )
fi

echo "=== lock locality performance measurement ==="
echo "baseline  : $BASELINE_DIR ($(git -C "$BASELINE_DIR" rev-parse --short HEAD 2>/dev/null || echo 'unknown'))"
echo "optimized : $OPTIMIZED_DIR ($(git -C "$OPTIMIZED_DIR" rev-parse --short HEAD))"
echo "output    : $OUT_DIR"
echo

run_workload() {
  local label="$1"
  local bin_dir="$2"
  local out_prefix="$OUT_DIR/$label"

  echo "--- [$label] starting workload ---"

  KAFS_BIN="$bin_dir/src/kafs" \
  KAFSCTL_BIN="$OPTIMIZED_DIR/src/kafsctl" \
  MKFS_BIN="$bin_dir/src/mkfs.kafs" \
  FSCK_BIN="$bin_dir/src/fsck.kafs" \
    /usr/bin/time -v \
    "$OPTIMIZED_DIR/scripts/workload-npm-offline-local.sh" \
    >"$out_prefix.stdout" 2>"$out_prefix.time"

  echo "--- [$label] done ---"
}

extract_fsstat_json() {
  local file="$1"
  awk '/FSSTAT_JSON_BEGIN/{f=1;next}/FSSTAT_JSON_END/{f=0}f' "$file" 2>/dev/null || true
}

extract_json_field() {
  local json="$1"
  local key="$2"
  printf "%s\n" "$json" | grep -m1 "\"$key\"" | awk -F'[:,]' '{gsub(/[ "]/,"",$2); print $2}' || true
}

elapsed_sec() {
  local f="$1"
  grep "Elapsed (wall clock)" "$f" | awk '{print $NF}' | awk -F: '{if(NF==3)print($1*3600)+($2*60)+$3;else print($1*60)+$2}'
}

percent_delta() {
  local base="$1"
  local cur="$2"
  if [[ -z "$base" || -z "$cur" || "$base" = "0" ]]; then
    echo "N/A"
    return
  fi
  awk "BEGIN{printf \"%.2f\", (($cur-$base)/$base)*100.0}"
}

run_workload "baseline" "$BASELINE_DIR"
run_workload "optimized" "$OPTIMIZED_DIR"

BL_TIME=$(elapsed_sec "$OUT_DIR/baseline.time" 2>/dev/null || echo "N/A")
OPT_TIME=$(elapsed_sec "$OUT_DIR/optimized.time" 2>/dev/null || echo "N/A")

BL_FSSTAT=$(extract_fsstat_json "$OUT_DIR/baseline.stdout")
OPT_FSSTAT=$(extract_fsstat_json "$OUT_DIR/optimized.stdout")

BL_LOCK_INODE_WAIT_NS=$(extract_json_field "$BL_FSSTAT" "lock_inode_wait_ns")
OPT_LOCK_INODE_WAIT_NS=$(extract_json_field "$OPT_FSSTAT" "lock_inode_wait_ns")
BL_LOCK_INODE_CONTENDED=$(extract_json_field "$BL_FSSTAT" "lock_inode_contended")
OPT_LOCK_INODE_CONTENDED=$(extract_json_field "$OPT_FSSTAT" "lock_inode_contended")
BL_LOCK_INODE_ACQUIRE=$(extract_json_field "$BL_FSSTAT" "lock_inode_acquire")
OPT_LOCK_INODE_ACQUIRE=$(extract_json_field "$OPT_FSSTAT" "lock_inode_acquire")
BL_LOCK_INODE_RATE=$(extract_json_field "$BL_FSSTAT" "lock_inode_contended_rate")
OPT_LOCK_INODE_RATE=$(extract_json_field "$OPT_FSSTAT" "lock_inode_contended_rate")

BL_LOCK_BITMAP_WAIT_NS=$(extract_json_field "$BL_FSSTAT" "lock_bitmap_wait_ns")
OPT_LOCK_BITMAP_WAIT_NS=$(extract_json_field "$OPT_FSSTAT" "lock_bitmap_wait_ns")
BL_LOCK_HRL_BUCKET_WAIT_NS=$(extract_json_field "$BL_FSSTAT" "lock_hrl_bucket_wait_ns")
OPT_LOCK_HRL_BUCKET_WAIT_NS=$(extract_json_field "$OPT_FSSTAT" "lock_hrl_bucket_wait_ns")

TIME_DELTA_PCT=$(percent_delta "$BL_TIME" "$OPT_TIME")
INODE_WAIT_DELTA_PCT=$(percent_delta "${BL_LOCK_INODE_WAIT_NS:-}" "${OPT_LOCK_INODE_WAIT_NS:-}")

REPORT="$OUT_DIR/report.md"
cat >"$REPORT" <<EOF
# Lock Locality Performance Report
- Date: $(date -Is)
- Baseline commit: $(git -C "$BASELINE_DIR" rev-parse --short HEAD 2>/dev/null || echo 'unknown')
- Optimized commit: $(git -C "$OPTIMIZED_DIR" rev-parse --short HEAD)
- Workload: npm-offline (18 local packages, mixed IO)

## Elapsed Time

| variant   | wall-clock (s) |
|-----------|----------------|
| baseline  | $BL_TIME |
| optimized | $OPT_TIME |
| delta (%) | ${TIME_DELTA_PCT} |

## Lock Counters

| counter | baseline | optimized | delta (%) |
|---------|----------|-----------|-----------|
| lock_inode_acquire | ${BL_LOCK_INODE_ACQUIRE:-N/A} | ${OPT_LOCK_INODE_ACQUIRE:-N/A} | N/A |
| lock_inode_contended | ${BL_LOCK_INODE_CONTENDED:-N/A} | ${OPT_LOCK_INODE_CONTENDED:-N/A} | N/A |
| lock_inode_contended_rate | ${BL_LOCK_INODE_RATE:-N/A} | ${OPT_LOCK_INODE_RATE:-N/A} | N/A |
| lock_inode_wait_ns | ${BL_LOCK_INODE_WAIT_NS:-N/A} | ${OPT_LOCK_INODE_WAIT_NS:-N/A} | ${INODE_WAIT_DELTA_PCT} |
| lock_bitmap_wait_ns | ${BL_LOCK_BITMAP_WAIT_NS:-N/A} | ${OPT_LOCK_BITMAP_WAIT_NS:-N/A} | N/A |
| lock_hrl_bucket_wait_ns | ${BL_LOCK_HRL_BUCKET_WAIT_NS:-N/A} | ${OPT_LOCK_HRL_BUCKET_WAIT_NS:-N/A} | N/A |

## Raw fsstat output (baseline)
\`\`\`
$BL_FSSTAT
\`\`\`

## Raw fsstat output (optimized)
\`\`\`
$OPT_FSSTAT
\`\`\`
EOF

echo
echo "=== Summary ==="
echo "  baseline elapsed (s): $BL_TIME"
echo "  optimized elapsed (s): $OPT_TIME"
echo "  elapsed delta (%)    : $TIME_DELTA_PCT"
echo "  lock_inode_wait_ns   : ${BL_LOCK_INODE_WAIT_NS:-N/A} -> ${OPT_LOCK_INODE_WAIT_NS:-N/A}"
echo "  wait_ns delta (%)    : $INODE_WAIT_DELTA_PCT"
echo
echo "Full report: $REPORT"
