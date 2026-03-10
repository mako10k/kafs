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

JSCPD_CMD=(
  npx --yes jscpd@latest
  --min-lines 8
  --threshold 5
  --reporters json,console
  --ignore "**/autom4te.cache/**"
  --ignore "**/tests_*.c"
  --format "c,c-header"
  --pattern "src/**/*.{c,h}"
  --output "$REPORT_DIR"
)

if command -v timeout >/dev/null 2>&1; then
  NPM_CONFIG_UPDATE_NOTIFIER=false timeout --preserve-status "${CLONES_TIMEOUT_SECONDS}s" "${JSCPD_CMD[@]}"
else
  NPM_CONFIG_UPDATE_NOTIFIER=false "${JSCPD_CMD[@]}"
fi
