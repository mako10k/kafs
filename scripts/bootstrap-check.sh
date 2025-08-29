#!/usr/bin/env bash
set -euo pipefail

# Simple bootstrap + build + test runner for kafs (Autotools)
# - Runs autoreconf only if ./configure is missing or RECONF=1
# - Runs ./configure if Makefile missing or RECONF=1
# - Executes `make -C src check` with parallelism

ROOT_DIR="$(CDPATH= cd -- "$(dirname "$0")"/.. && pwd)"
cd "$ROOT_DIR"

JOBS="${JOBS:-}"
if [[ -z "$JOBS" ]]; then
  if command -v nproc >/dev/null 2>&1; then JOBS="$(nproc)"; else JOBS=2; fi
fi

echo "[bootstrap] repo: $ROOT_DIR"

if [[ "${RECONF:-0}" != "0" || ! -x ./configure ]]; then
  echo "[bootstrap] running autoreconf -fi"
  autoreconf -fi
fi

if [[ "${RECONF:-0}" != "0" || ! -f ./Makefile ]]; then
  echo "[bootstrap] running ./configure"
  ./configure
fi

echo "[build] make -C src -j$JOBS check"
make -C src -j"$JOBS" check

if [[ -f src/test-suite.log ]]; then
  echo "[summary] src/test-suite.log"
  sed -n '1,120p' src/test-suite.log
fi

echo "[done] bootstrap-check completed"
