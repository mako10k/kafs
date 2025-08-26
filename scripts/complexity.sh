#!/usr/bin/env bash
set -euo pipefail
# Complexity metrics using lizard
if ! command -v lizard >/dev/null 2>&1; then
  echo "lizard not found. Install it (pip install lizard) to run complexity analysis." >&2
  exit 0
fi
REPORT_DIR=${REPORT_DIR:-report/complexity}
mkdir -p "$REPORT_DIR"
lizard -l c -x "*/autom4te.cache/*" -x "*/test-driver" src > "$REPORT_DIR/lizard.txt" || true
cat "$REPORT_DIR/lizard.txt"
