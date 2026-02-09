#!/usr/bin/env bash
set -euo pipefail

# bootstrap-check.sh: Autotools bootstrap + build + test runner for kafs
# Options:
#   --reconf            Force autoreconf/configure
#   --jobs N            Parallel jobs for make (default: nproc or 2)
#   --tests "A B ..."   Run only specified tests (Automake TESTS override)
#   --valgrind          Run tests under valgrind (TESTS_ENVIRONMENT)
#   --asan              Build with AddressSanitizer (requires reconfigure)
#   --skip-fuse         Run only non-FUSE tests (HRL tests)
#   -h|--help           Show help

ROOT_DIR="$(CDPATH= cd -- "$(dirname "$0")"/.. && pwd)"
cd "$ROOT_DIR"

RECONF="${RECONF:-0}"
JOBS="${JOBS:-}"
USER_TESTS=""
USE_VALGRIND="0"
USE_ASAN="0"
SKIP_FUSE="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --reconf) RECONF=1 ; shift ;;
    --jobs) JOBS="${2:-}" ; shift 2 ;;
    --tests) USER_TESTS="${2:-}" ; shift 2 ;;
    --valgrind) USE_VALGRIND=1 ; shift ;;
    --asan) USE_ASAN=1 ; RECONF=1 ; shift ;;
    --skip-fuse) SKIP_FUSE=1 ; shift ;;
    -h|--help)
      cat <<EOF
Usage: scripts/bootstrap-check.sh [options]
  --reconf            Force autoreconf/configure
  --jobs N            Parallel jobs for make (default: nproc or 2)
  --tests "A B ..."   Run only specified tests
  --valgrind          Run tests under valgrind
  --asan              Build with AddressSanitizer (implies --reconf)
  --skip-fuse         Run only non-FUSE tests
  -h, --help          Show this help
Env:
  JOBS, RECONF can also be set via environment.
EOF
      exit 0 ;;
    *) echo "Unknown option: $1" >&2 ; exit 2 ;;
  esac
done

if [[ -z "$JOBS" ]]; then
  if command -v nproc >/dev/null 2>&1; then JOBS="$(nproc)"; else JOBS=2; fi
fi

echo "[bootstrap] repo: $ROOT_DIR"

# Preflight: required tools
for cmd in pkg-config gcc make; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "[error] missing tool: $cmd" >&2; exit 127; }
done

# Check fuse3 headers/library
if ! pkg-config --exists fuse3 ; then
  echo "[warn] fuse3 not found via pkg-config; FUSE tests may fail" >&2
fi

# Optional FUSE capability check (non-fatal)
if [[ "$SKIP_FUSE" != "1" ]]; then
  if [[ ! -e /dev/fuse ]]; then
    echo "[warn] /dev/fuse not present; FUSE tests will likely fail. Use --skip-fuse to avoid." >&2
  fi
  if ! command -v fusermount3 >/dev/null 2>&1; then
    echo "[warn] fusermount3 not found; unmount steps may fail."
  fi
fi

if [[ "$RECONF" != "0" || ! -x ./configure ]]; then
  echo "[bootstrap] running autoreconf -fi"
  autoreconf -fi
fi

CFG_CFLAGS=""
CFG_LDFLAGS=""
if [[ "$USE_ASAN" == "1" ]]; then
  echo "[cfg] enabling AddressSanitizer"
  CFG_CFLAGS="-Wall -Werror -g -O0 -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -fsanitize=address"
  CFG_LDFLAGS="-fsanitize=address"
fi

if [[ "$RECONF" != "0" || ! -f ./Makefile ]]; then
  echo "[bootstrap] running ./configure"
  if [[ -n "$CFG_CFLAGS$CFG_LDFLAGS" ]]; then
    CFLAGS="$CFG_CFLAGS" LDFLAGS="$CFG_LDFLAGS" ./configure
  else
    ./configure
  fi
fi

# Determine tests to run
MAKE_ARGS=()
if [[ -n "$USER_TESTS" ]]; then
  MAKE_ARGS+=("TESTS=$USER_TESTS")
elif [[ "$SKIP_FUSE" == "1" ]]; then
  MAKE_ARGS+=("TESTS=hrl_smoketest hrl_dec_ref_by_blo hrl_unconfigured")
fi

# Optional valgrind wrapper
TESTS_ENVIRONMENT=""
if [[ "$USE_VALGRIND" == "1" ]]; then
  if command -v valgrind >/dev/null 2>&1; then
    TESTS_ENVIRONMENT="valgrind --quiet --leak-check=full --error-exitcode=42"
    echo "[test] running under valgrind"
  else
    echo "[warn] valgrind not found; proceeding without it" >&2
  fi
fi

echo "[build] make -j$JOBS check ${MAKE_ARGS[*]:-}"
if [[ -n "$TESTS_ENVIRONMENT" ]]; then
  TESTS_ENVIRONMENT="$TESTS_ENVIRONMENT" make -j"$JOBS" check "${MAKE_ARGS[@]}"
else
  make -j"$JOBS" check "${MAKE_ARGS[@]}"
fi

for log in tests/test-suite.log test-suite.log src/test-suite.log; do
  if [[ -f "$log" ]]; then
    echo "[summary] $log"
    sed -n '1,160p' "$log" || true
    break
  fi
done


echo "[done] bootstrap-check completed"
