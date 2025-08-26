#!/usr/bin/env bash
set -euo pipefail
# Dead code analysis using cppcheck
if ! command -v cppcheck >/dev/null 2>&1; then
  echo "cppcheck not found. Install it to run dead code analysis." >&2
  exit 0
fi
REPORT_DIR=${REPORT_DIR:-report/cppcheck}
mkdir -p "$REPORT_DIR"
cppcheck \
  --enable=warning,style,performance,portability,unusedFunction \
  --std=c11 \
  --language=c \
  --force \
  --inline-suppr \
  --quiet \
  --template=gcc \
  -I src \
  --suppress=missingIncludeSystem \
  --output-file="$REPORT_DIR/cppcheck.txt" \
  src || true
cat "$REPORT_DIR/cppcheck.txt" || true
