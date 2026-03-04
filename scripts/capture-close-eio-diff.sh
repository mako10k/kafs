#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

IMAGE_PATH=${1:-/tmp/issue2-kafs-repair.img}
MOUNT_POINT=${2:-/tmp/issue2-kafs-repair-mnt}
WORKLOAD_DIR=${3:-/home/katsumata-m/kscr_selfhost}
MANIFEST_PATH=${4:-kscr/Cargo.toml}
MAX_ATTEMPTS=${MAX_ATTEMPTS:-12}
STRACE_DAEMON=${STRACE_DAEMON:-0}
STRACE_CARGO=${STRACE_CARGO:-1}

OUT_DIR=$(mktemp -d "${TMPDIR:-/tmp}/kafs-close-eio-diff.XXXXXX")
SUCCESS_ATTEMPT=""
FAIL_ATTEMPT=""

cleanup() {
  fusermount -u "$MOUNT_POINT" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_mounted() {
  local i fstype
  for i in $(seq 1 80); do
    fstype=$(findmnt -T "$MOUNT_POINT" -o FSTYPE --noheadings 2>/dev/null || true)
    if [[ "$fstype" == "fuse.kafs" ]]; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

start_daemon() {
  local attempt=$1
  mkdir -p "$MOUNT_POINT"
  fusermount -u "$MOUNT_POINT" >/dev/null 2>&1 || true

  # NOTE: strace around kafs can disable setuid effects of fusermount3 and make mount fail.
  if [[ "$STRACE_DAEMON" == "1" ]]; then
    KAFS_DEBUG=2 strace -ff \
      -o "$OUT_DIR/kafs.$attempt.strace" \
      -e trace=openat,close,rename,renameat,renameat2,write,pwrite64,fsync,fdatasync,ftruncate,msync \
      ./src/kafs --image "$IMAGE_PATH" "$MOUNT_POINT" -f \
      >"$OUT_DIR/kafs.$attempt.log" 2>&1 &
  else
    KAFS_DEBUG=2 ./src/kafs --image "$IMAGE_PATH" "$MOUNT_POINT" -f \
      >"$OUT_DIR/kafs.$attempt.log" 2>&1 &
  fi
  echo $! >"$OUT_DIR/kafs.$attempt.pid"

  wait_mounted
}

stop_daemon() {
  local attempt=$1
  fusermount -u "$MOUNT_POINT" >/dev/null 2>&1 || true
  if [[ -f "$OUT_DIR/kafs.$attempt.pid" ]]; then
    local pid
    pid=$(cat "$OUT_DIR/kafs.$attempt.pid")
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  fi
}

extract_signature() {
  local trace_glob=$1
  local out=$2
  awk '
    match($0, /([a-zA-Z0-9_]+)\(.*\)[[:space:]]+= ([^ ]+)/, m) {
      print m[1], m[2]
    }
  ' $trace_glob 2>/dev/null | sort | uniq -c | sort -nr > "$out"
}

extract_eio_context() {
  local trace_glob=$1
  local out=$2
  rg -n -C 2 "= -1 EIO|Input/output" $trace_glob >"$out" 2>/dev/null || true
}

trace_glob_for_attempt() {
  local attempt=$1
  if [[ "$STRACE_CARGO" == "1" ]] && compgen -G "$OUT_DIR/cargo.$attempt.strace*" >/dev/null; then
    echo "$OUT_DIR"/cargo."$attempt".strace*
    return 0
  fi
  if [[ "$STRACE_DAEMON" == "1" ]] && compgen -G "$OUT_DIR/kafs.$attempt.strace*" >/dev/null; then
    echo "$OUT_DIR"/kafs."$attempt".strace*
    return 0
  fi
  return 1
}

echo "Output directory: $OUT_DIR"
echo "Image: $IMAGE_PATH"
echo "Mount: $MOUNT_POINT"
echo "Workload: $WORKLOAD_DIR ($MANIFEST_PATH)"
echo "Trace mode: daemon=${STRACE_DAEMON} cargo=${STRACE_CARGO}"
echo ""

for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  echo "=== Attempt $attempt/$MAX_ATTEMPTS ==="

  if ! start_daemon "$attempt"; then
    echo "Mount failed at attempt $attempt" | tee -a "$OUT_DIR/summary.txt"
    stop_daemon "$attempt"
    continue
  fi

  set +e
  if [[ "$STRACE_CARGO" == "1" ]]; then
    (
      cd "$WORKLOAD_DIR"
      CARGO_TARGET_DIR="$MOUNT_POINT/target" strace -ff \
        -o "$OUT_DIR/cargo.$attempt.strace" \
        -e trace=openat,close,rename,renameat,renameat2,write,pwrite64,fsync,fdatasync,ftruncate,msync \
        cargo build --manifest-path "$MANIFEST_PATH" --all-features -j1
    ) >"$OUT_DIR/cargo.$attempt.stdout" 2>"$OUT_DIR/cargo.$attempt.stderr"
  else
    (
      cd "$WORKLOAD_DIR"
      CARGO_TARGET_DIR="$MOUNT_POINT/target" cargo build --manifest-path "$MANIFEST_PATH" --all-features -j1
    ) >"$OUT_DIR/cargo.$attempt.stdout" 2>"$OUT_DIR/cargo.$attempt.stderr"
  fi
  cargo_rc=$?
  set -e

  stop_daemon "$attempt"

  if [[ $cargo_rc -eq 0 ]]; then
    echo "attempt=$attempt result=success" | tee -a "$OUT_DIR/summary.txt"
    [[ -z "$SUCCESS_ATTEMPT" ]] && SUCCESS_ATTEMPT=$attempt
  else
    echo "attempt=$attempt result=failure rc=$cargo_rc" | tee -a "$OUT_DIR/summary.txt"
    [[ -z "$FAIL_ATTEMPT" ]] && FAIL_ATTEMPT=$attempt
  fi

  if [[ -n "$SUCCESS_ATTEMPT" && -n "$FAIL_ATTEMPT" ]]; then
    break
  fi
  echo ""
done

if [[ -z "$SUCCESS_ATTEMPT" || -z "$FAIL_ATTEMPT" ]]; then
  echo ""
  echo "Could not capture both success and failure within $MAX_ATTEMPTS attempts."
  echo "See: $OUT_DIR/summary.txt"
  exit 2
fi

SUCCESS_SIG="$OUT_DIR/success.signature.txt"
FAIL_SIG="$OUT_DIR/fail.signature.txt"
SUCCESS_EIO="$OUT_DIR/success.eio.txt"
FAIL_EIO="$OUT_DIR/fail.eio.txt"

SUCCESS_GLOB=$(trace_glob_for_attempt "$SUCCESS_ATTEMPT" || true)
FAIL_GLOB=$(trace_glob_for_attempt "$FAIL_ATTEMPT" || true)

if [[ -z "$SUCCESS_GLOB" || -z "$FAIL_GLOB" ]]; then
  echo "No trace files found for success/failure attempts." >&2
  echo "Try STRACE_CARGO=1 (default) or STRACE_DAEMON=1." >&2
  exit 3
fi

extract_signature "$SUCCESS_GLOB" "$SUCCESS_SIG"
extract_signature "$FAIL_GLOB" "$FAIL_SIG"
extract_eio_context "$SUCCESS_GLOB" "$SUCCESS_EIO"
extract_eio_context "$FAIL_GLOB" "$FAIL_EIO"

diff -u "$SUCCESS_SIG" "$FAIL_SIG" > "$OUT_DIR/signature.diff" || true

{
  echo "success_attempt=$SUCCESS_ATTEMPT"
  echo "fail_attempt=$FAIL_ATTEMPT"
  echo "success_trace_glob=$SUCCESS_GLOB"
  echo "fail_trace_glob=$FAIL_GLOB"
  echo "summary_file=$OUT_DIR/summary.txt"
  echo "signature_diff=$OUT_DIR/signature.diff"
  echo "fail_eio=$FAIL_EIO"
  echo "success_eio=$SUCCESS_EIO"
} | tee "$OUT_DIR/RESULT.txt"

echo ""
echo "Done. RESULT: $OUT_DIR/RESULT.txt"
