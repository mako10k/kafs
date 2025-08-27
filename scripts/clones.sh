#!/usr/bin/env bash
set -euo pipefail
# Code clone detection via jscpd
if ! command -v npx >/dev/null 2>&1; then
  echo "npx not found. Please install Node.js/npm to run jscpd." >&2
  exit 0
fi
REPORT_DIR=${REPORT_DIR:-report/clone}
mkdir -p "$REPORT_DIR"
npx --yes jscpd@latest \
  --min-lines 10 \
  --threshold 1 \
  --reporters json,console \
  --ignore "**/autom4te.cache/**" \
  --format "c,c-header" \
  --pattern 'src/**/*.{c,h}' \
  --output "$REPORT_DIR" || true
