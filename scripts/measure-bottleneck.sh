#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

STAMP=$(date +%Y%m%d-%H%M%S)
OUT_DIR="$ROOT_DIR/report/perf/$STAMP"
mkdir -p "$OUT_DIR"

USE_PERF=1
USE_STRACE=${KAFS_MEASURE_STRACE:-0}
MODE="full"

if [[ "${1:-}" = "--quick" ]]; then
  MODE="quick"
fi

if ! command -v perf >/dev/null 2>&1; then
  USE_PERF=0
fi
if [[ "$USE_STRACE" = "1" ]] && ! command -v strace >/dev/null 2>&1; then
  USE_STRACE=0
fi

cat >"$OUT_DIR/environment.txt" <<EOF
timestamp: $(date -Is)
uname: $(uname -a)
pwd: $PWD
use_perf: $USE_PERF
use_strace: $USE_STRACE
git_head: $(git rev-parse --short HEAD)
EOF

if command -v lscpu >/dev/null 2>&1; then
  lscpu >"$OUT_DIR/lscpu.txt" 2>/dev/null || true
fi

run_case() {
  local name="$1"
  shift

  local base="$OUT_DIR/$name"
  local cmd=("$@")

  echo "[measure] case=$name"
  printf '%q ' "${cmd[@]}" >"$base.command.txt"
  echo >>"$base.command.txt"

  /usr/bin/time -v "${cmd[@]}" >"$base.stdout.log" 2>"$base.time.log" || true

  if [[ "$USE_PERF" = "1" ]]; then
    perf stat -d "${cmd[@]}" >"$base.perf.stdout.log" 2>"$base.perf.stat.log" || true
  fi

  if [[ "$USE_STRACE" = "1" ]]; then
    strace -ff -ttT -o "$base.strace" "${cmd[@]}" >"$base.strace.stdout.log" 2>"$base.strace.stderr.log" || true
  fi
}

# W1: directory-op heavy (hooks direct test)
run_case w1_dir_ops "$ROOT_DIR/scripts/test-hooks-direct.sh"

# W2: git-like directory/file operations
run_case w2_git_ops "$ROOT_DIR/scripts/test-git-operations.sh"

# W3: npm-like offline install (mixed directory + file writes, network-free)
run_case w3_npm_offline "$ROOT_DIR/scripts/workload-npm-offline-local.sh"

if [[ "$MODE" = "full" ]]; then
  # W4: mixed workload with parallel reads/copy
  run_case w4_mixed "$ROOT_DIR/scripts/reproduce-workload.sh"
fi

{
  echo "# Bottleneck Measurement Summary ($STAMP)"
  echo
  echo "- Mode: $MODE"
  echo "- Cases:"
  echo "  - w1_dir_ops"
  echo "  - w2_git_ops"
  echo "  - w3_npm_offline"
  if [[ "$MODE" = "full" ]]; then
    echo "  - w4_mixed"
  fi
  echo
  echo "## Files"
  echo
  echo "- environment: environment.txt"
  echo "- per case:"
  echo "  - *.stdout.log"
  echo "  - *.time.log"
  echo "  - *.perf.stat.log (if perf available)"
  echo "  - *.strace.* (if KAFS_MEASURE_STRACE=1)"
  echo
  echo "## Quick next checks"
  echo
  echo "1. Compare elapsed/user/sys from *.time.log"
  echo "2. Compare context-switches and CPU stats from *.perf.stat.log"
  echo "3. If needed, rerun with strace:"
  echo "   KAFS_MEASURE_STRACE=1 scripts/measure-bottleneck.sh"
} >"$OUT_DIR/SUMMARY.md"

echo "[measure] done: $OUT_DIR"
