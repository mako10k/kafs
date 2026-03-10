#!/usr/bin/env bash
set -euo pipefail
# Dead code analysis using cppcheck
if ! command -v cppcheck >/dev/null 2>&1; then
  echo "cppcheck not found. Install it to run dead code analysis." >&2
  exit 0
fi
REPORT_DIR=${REPORT_DIR:-report/cppcheck}
CPPCHECK_TIMEOUT_SECONDS=${CPPCHECK_TIMEOUT_SECONDS:-180}
mkdir -p "$REPORT_DIR"
echo "[deadcode] start: cppcheck"
if command -v timeout >/dev/null 2>&1; then
  timeout --preserve-status "${CPPCHECK_TIMEOUT_SECONDS}s" \
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
      src || {
    rc=$?
    if [[ $rc -eq 124 || $rc -eq 137 ]]; then
      echo "[deadcode] timeout: cppcheck (${CPPCHECK_TIMEOUT_SECONDS}s)" >&2
    fi
  }
else
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
fi
echo "[deadcode] done: cppcheck"
cat "$REPORT_DIR/cppcheck.txt" || true
