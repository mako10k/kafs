#!/usr/bin/env bash
# measure-metadata-lookup-perf.sh
# Compare metadata lookup performance: baseline vs optimized (issue #58)
#
# Usage:
#   scripts/measure-metadata-lookup-perf.sh [--baseline-dir DIR]
#
# Defaults:
#   BASELINE_DIR = .worktree/issue-58-baseline
#   OPTIMIZED_DIR = . (workspace root)
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

BASELINE_DIR="${1:-$ROOT_DIR/.worktree/issue-58-baseline}"
OPTIMIZED_DIR="$ROOT_DIR"
STAMP=$(date +%Y%m%d-%H%M%S)
OUT_DIR="$ROOT_DIR/report/perf/metadata-lookup-$STAMP"
mkdir -p "$OUT_DIR"

if ! command -v npm >/dev/null 2>&1; then
  echo "SKIP: npm not found"
  exit 77
fi

echo "=== metadata lookup performance measurement ==="
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

# Run baseline
run_workload "baseline" "$BASELINE_DIR"

# Run optimized
run_workload "optimized" "$OPTIMIZED_DIR"

# Extract elapsed time helper
elapsed_sec() {
  local f="$1"
  grep "Elapsed (wall clock)" "$f" | awk '{print $NF}' | awk -F: '{ if(NF==3) print ($1*3600)+($2*60)+$3; else print ($1*60)+$2 }'
}

# Extract fsstat field helper
fsstat_val() {
  local stdout="$1"
  local key="$2"
  awk -F: "/$key/{gsub(/[^0-9.]/,\"\",\$2); print \$2}" "$stdout" 2>/dev/null | head -1
}

# Parse results
BL_TIME=$(elapsed_sec "$OUT_DIR/baseline.time" 2>/dev/null || echo "N/A")
OPT_TIME=$(elapsed_sec "$OUT_DIR/optimized.time" 2>/dev/null || echo "N/A")

# Extract fsstat JSON from optimized stdout
OPT_FSSTAT=$(awk '/FSSTAT_JSON_BEGIN/{found=1;next}/FSSTAT_JSON_END/{found=0}found' "$OUT_DIR/optimized.stdout" 2>/dev/null || true)
BL_FSSTAT=$(awk '/FSSTAT_JSON_BEGIN/{found=1;next}/FSSTAT_JSON_END/{found=0}found' "$OUT_DIR/baseline.stdout" 2>/dev/null || true)

extract_json_field() {
  local json="$1"
  local key="$2"
  echo "$json" | grep "\"$key\"" | awk -F'[:,]' '{gsub(/ /,"",$2); print $2}' | head -1
}

OPT_ACCESS_CALLS=$(extract_json_field "$OPT_FSSTAT" "access_calls")
OPT_PATH_WALK=$(extract_json_field "$OPT_FSSTAT" "access_path_walk_calls")
OPT_FH_HITS=$(extract_json_field "$OPT_FSSTAT" "access_fh_fastpath_hits")
OPT_FH_RATE=$(extract_json_field "$OPT_FSSTAT" "access_fh_fastpath_rate")
OPT_DIR_SNAP=$(extract_json_field "$OPT_FSSTAT" "dir_snapshot_calls")
OPT_SNAP_BYTES=$(extract_json_field "$OPT_FSSTAT" "dir_snapshot_bytes")
OPT_META_LOAD=$(extract_json_field "$OPT_FSSTAT" "dir_snapshot_meta_load_calls")
OPT_VIEW_NEXT=$(extract_json_field "$OPT_FSSTAT" "dirent_view_next_calls")

# Compute speedup
if [[ "$BL_TIME" != "N/A" && "$OPT_TIME" != "N/A" ]]; then
  SPEEDUP=$(awk "BEGIN{if($OPT_TIME>0) printf \"%.3f\", $BL_TIME/$OPT_TIME; else print \"N/A\"}")
else
  SPEEDUP="N/A"
fi

# Save report
REPORT="$OUT_DIR/report.md"
cat >"$REPORT" <<EOF
# Metadata Lookup Performance Report
- Date: $(date -Is)
- Baseline commit: $(git -C "$BASELINE_DIR" rev-parse --short HEAD 2>/dev/null || echo 'unknown')
- Optimized commit: $(git -C "$OPTIMIZED_DIR" rev-parse --short HEAD)
- Workload: npm-offline (18 local packages, mixed IO)

## Elapsed Time

| variant   | wall-clock (s) |
|-----------|---------------|
| baseline  | $BL_TIME |
| optimized | $OPT_TIME |
| speedup   | ${SPEEDUP}x |

## Metadata Lookup Counters (optimized only)

| counter | value |
|---------|-------|
| access_calls | ${OPT_ACCESS_CALLS:-N/A} |
| access_path_walk_calls | ${OPT_PATH_WALK:-N/A} |
| access_fh_fastpath_hits | ${OPT_FH_HITS:-N/A} |
| access_fh_fastpath_rate | ${OPT_FH_RATE:-N/A} |
| dir_snapshot_calls | ${OPT_DIR_SNAP:-N/A} |
| dir_snapshot_bytes | ${OPT_SNAP_BYTES:-N/A} |
| dir_snapshot_meta_load_calls | ${OPT_META_LOAD:-N/A} |
| dirent_view_next_calls | ${OPT_VIEW_NEXT:-N/A} |

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
echo "  baseline elapsed  : ${BL_TIME}s"
echo "  optimized elapsed : ${OPT_TIME}s"
echo "  speedup           : ${SPEEDUP}x"
echo
echo "  [optimized counters]"
echo "  access_calls           : ${OPT_ACCESS_CALLS:-N/A}"
echo "  access_path_walk_calls : ${OPT_PATH_WALK:-N/A}"
echo "  access_fh_fastpath_hits: ${OPT_FH_HITS:-N/A}"
echo "  access_fh_fastpath_rate: ${OPT_FH_RATE:-N/A}"
echo "  dir_snapshot_calls     : ${OPT_DIR_SNAP:-N/A}"
echo "  dir_snapshot_meta_load : ${OPT_META_LOAD:-N/A}"
echo "  dirent_view_next_calls : ${OPT_VIEW_NEXT:-N/A}"
echo
echo "Full report: $REPORT"
