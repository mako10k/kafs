#!/usr/bin/env bash
set -euo pipefail
# Code clone detection via jscpd
if ! command -v npx >/dev/null 2>&1; then
  echo "npx not found. Please install Node.js/npm to run jscpd." >&2
  exit 0
fi
REPORT_DIR=${REPORT_DIR:-report/clone}
CLONES_TIMEOUT_SECONDS=${CLONES_TIMEOUT_SECONDS:-180}
mkdir -p "$REPORT_DIR"

run_jscpd() {
  local output_dir=$1
  local threshold=$2
  local pattern=$3

  local cmd=(
    npx --yes jscpd@latest
    --min-lines 8
    --threshold "$threshold"
    --reporters json,console
    --ignore "**/autom4te.cache/**"
    --ignore "**/.deps/**"
    --ignore "**/Makefile.in"
    --ignore "**/Makefile"
    --format "c,c-header"
    --pattern "$pattern"
    --output "$output_dir"
  )

  if command -v timeout >/dev/null 2>&1; then
    NPM_CONFIG_UPDATE_NOTIFIER=false timeout --preserve-status "${CLONES_TIMEOUT_SECONDS}s" "${cmd[@]}"
  else
    NPM_CONFIG_UPDATE_NOTIFIER=false "${cmd[@]}"
  fi
}

echo "[clones] strict gate: src/**/*.{c,h}"
run_jscpd "$REPORT_DIR/src" 1 "src/**/*.{c,h}"

echo "[clones] informational report: tests/**/*.{c,h}"
run_jscpd "$REPORT_DIR/tests" 100 "tests/**/*.{c,h}"

if [ -f "$REPORT_DIR/src/jscpd-report.json" ]; then
  cp "$REPORT_DIR/src/jscpd-report.json" "$REPORT_DIR/jscpd-report.json"
fi
