#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

DEFAULT_MOUNT_OPTIONS="rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)

IMAGE=""
REPORT_ROOT="$ROOT_DIR/report/v6-controlled-write-smoke"
REPORT_DIR=""
MOUNT_DIR=""
USER_MOUNT_DIR=0
KEEP_WORKDIR=0
YES=0
MOUNT_TIMEOUT_MS="${KAFS_V6_WRITE_SMOKE_MOUNT_TIMEOUT_MS:-15000}"
MOUNT_OPTIONS="${KAFS_V6_WRITE_SMOKE_MOUNT_OPTIONS:-$DEFAULT_MOUNT_OPTIONS}"
KAFS_DEBUG_LEVEL="${KAFS_V6_WRITE_SMOKE_DEBUG:-0}"

KAFS_BIN="${KAFS_BIN:-$ROOT_DIR/src/kafs}"
KAFSDUMP_BIN="${KAFSDUMP_BIN:-$ROOT_DIR/src/kafsdump}"
FSCK_BIN="${FSCK_BIN:-$ROOT_DIR/src/fsck.kafs}"

WORKDIR=""
KAFS_PID=""
MOUNTED=0

usage() {
  cat <<'USAGEEOF'
Usage:
  scripts/v6-controlled-write-smoke.sh --image IMAGE --yes [options]

Description:
  Run the format v6 controlled-write operator smoke against an existing v6
  destination image. This mutates IMAGE. The script records before/after
  kafsdump JSON, fsck output, image stat/digest, mount log, and the exact
  regular-file create/write/fsync workload under a timestamped report directory.

Options:
  --image IMAGE              Existing v6 image to validate and mutate
  --yes                      Required acknowledgement that IMAGE will change
  --report-root DIR          Parent directory for timestamped reports
                             (default: report/v6-controlled-write-smoke)
  --report-dir DIR           Exact report directory to create/use
  --mount-dir DIR            Empty mountpoint to use instead of a temp dir
  --mount-options OPTS       Override mount options; unsafe options are refused
  --timeout-ms N             Mount wait timeout in milliseconds (default: 15000)
  --keep-workdir             Keep the temporary mount workdir after exit
  -h, --help                 Show this help

Environment:
  KAFS_BIN, KAFSDUMP_BIN, FSCK_BIN can override tool paths.
  KAFS_V6_WRITE_SMOKE_MOUNT_OPTIONS can override the default mount options.
  KAFS_V6_WRITE_SMOKE_MOUNT_TIMEOUT_MS can override the default timeout.
  KAFS_V6_WRITE_SMOKE_DEBUG can set KAFS_DEBUG for the mount (default: 0).
USAGEEOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

abs_existing_path() {
  local path="$1"
  local dir
  local base

  dir=$(dirname "$path")
  base=$(basename "$path")
  (cd "$dir" && printf "%s/%s\n" "$(pwd)" "$base")
}

resolve_exe() {
  local value="$1"
  local label="$2"

  if [[ "$value" == */* ]]; then
    [[ -x "$value" ]] || die "$label is not executable: $value"
    abs_existing_path "$value"
    return
  fi

  command -v "$value" >/dev/null 2>&1 || die "$label not found: $value"
  command -v "$value"
}

validate_unsigned() {
  local label="$1"
  local value="$2"

  [[ "$value" =~ ^[0-9]+$ ]] || die "$label must be an unsigned integer: $value"
}

validate_mount_options() {
  local opts="$1"
  local has_rw=0
  local has_v6_write=0
  local has_no_writeback=0
  local has_no_trim=0
  local has_bg_off=0
  local has_fsync_full=0
  local raw
  local tok
  local -a tokens

  [[ -n "$opts" ]] || die "mount options are empty"

  IFS=',' read -r -a tokens <<<"$opts"
  for raw in "${tokens[@]}"; do
    tok=${raw//[[:space:]]/}
    [[ -n "$tok" ]] || die "empty mount option in: $opts"

    case "$tok" in
      rw)
        has_rw=1
        ;;
      ro)
        die "unsafe mount option refused for controlled write smoke: ro"
        ;;
      v6_write_mount|v6-write-mount)
        has_v6_write=1
        ;;
      v6_inspection_mount|v6-inspection-mount)
        die "unsafe mount option refused for controlled write smoke: $tok"
        ;;
      no_writeback_cache|no-writeback-cache)
        has_no_writeback=1
        ;;
      writeback_cache|writeback-cache)
        die "unsafe mount option refused for controlled write smoke: $tok"
        ;;
      no_trim_on_free|no-trim-on-free)
        has_no_trim=1
        ;;
      trim_on_free|trim-on-free)
        die "unsafe mount option refused for controlled write smoke: $tok"
        ;;
      bg_dedup_scan=off|dedup_scan=off|no_bg_dedup_scan|no-bg-dedup-scan)
        has_bg_off=1
        ;;
      bg_dedup_scan|dedup_scan|bg_dedup_scan=on|dedup_scan=on)
        die "unsafe mount option refused for controlled write smoke: $tok"
        ;;
      fsync_policy=full)
        has_fsync_full=1
        ;;
      fsync_policy=*)
        die "controlled write smoke requires fsync_policy=full, got: $tok"
        ;;
      hotplug|hotplug=*|hotplug_uds=*|hotplug-uds=*|hotplug_back_bin=*|hotplug-back-bin=*)
        die "hotplug options are refused for controlled write smoke: $tok"
        ;;
    esac
  done

  [[ "$has_rw" -eq 1 ]] || die "controlled write smoke requires -o rw"
  [[ "$has_v6_write" -eq 1 ]] || die "controlled write smoke requires -o v6_write_mount"
  [[ "$has_no_writeback" -eq 1 ]] || die "controlled write smoke requires no_writeback_cache"
  [[ "$has_no_trim" -eq 1 ]] || die "controlled write smoke requires no_trim_on_free"
  [[ "$has_bg_off" -eq 1 ]] || die "controlled write smoke requires bg_dedup_scan=off"
  [[ "$has_fsync_full" -eq 1 ]] || die "controlled write smoke requires fsync_policy=full"
}

quote_cmd() {
  local arg
  for arg in "$@"; do
    printf "%q " "$arg"
  done
  printf "\n"
}

is_mounted() {
  local mp="$1"

  if command -v findmnt >/dev/null 2>&1; then
    findmnt -rn --mountpoint "$mp" >/dev/null 2>&1
    return
  fi

  awk -v mp="$mp" '$2 == mp { found = 1 } END { exit found ? 0 : 1 }' /proc/mounts
}

capture_command() {
  local name="$1"
  local stdout_path="$2"
  local stderr_path="$3"
  shift 3

  quote_cmd "$@" >"$REPORT_DIR/${name}.cmd"
  set +e
  "$@" >"$stdout_path" 2>"$stderr_path"
  local rc=$?
  set -e
  printf "%s\n" "$rc" >"$REPORT_DIR/${name}.status"
  return "$rc"
}

write_image_meta() {
  local phase="$1"
  local stat_path="$REPORT_DIR/image-${phase}.stat"
  local digest_path="$REPORT_DIR/image-${phase}.sha256"

  stat "$IMAGE_ABS" >"$stat_path" 2>"$REPORT_DIR/image-${phase}.stat.stderr" || return 1
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$IMAGE_ABS" >"$digest_path"
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$IMAGE_ABS" >"$digest_path"
  else
    echo "sha256sum or shasum is required for image digest capture" \
      >"$REPORT_DIR/image-${phase}.sha256.stderr"
    return 1
  fi
}

write_workload_script() {
  cat >"$WORKLOAD_PY" <<'PYEOF'
#!/usr/bin/env python3
import os
import sys


def write_all(fd, data, offset=None):
    view = memoryview(data)
    done = 0
    while done < len(view):
        if offset is None:
            n = os.write(fd, view[done:])
        else:
            n = os.pwrite(fd, view[done:], offset + done)
        if n <= 0:
            raise OSError("short write")
        done += n


def read_exact(fd, size, offset):
    out = bytearray()
    while len(out) < size:
        chunk = os.pread(fd, size - len(out), offset + len(out))
        if not chunk:
            raise OSError("short read")
        out.extend(chunk)
    return bytes(out)


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: workload.py <mounted-output-file>")

    path = sys.argv[1]
    block0 = bytes((0x41 + (i % 23)) & 0xFF for i in range(4096))
    zero = b"\x00" * 4096
    patch = b"v6-operator-partial-write"
    patch_offset = 4096 + 123

    print(f"create {path}")
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
    try:
        print("write block0 4096 bytes")
        write_all(fd, block0)
        print("fsync block0")
        os.fsync(fd)

        print("pwrite zero block at offset 4096")
        write_all(fd, zero, 4096)
        if hasattr(os, "fdatasync"):
            print("fdatasync zero block")
            os.fdatasync(fd)
        else:
            print("fsync zero block")
            os.fsync(fd)

        print(f"pwrite partial patch at offset {patch_offset}")
        write_all(fd, patch, patch_offset)
        print("fsync partial patch")
        os.fsync(fd)

        print("verify block0")
        if read_exact(fd, 4096, 0) != block0:
            raise OSError("block0 readback mismatch")

        print("verify zero plus partial patch block")
        block1 = read_exact(fd, 4096, 4096)
        expected = bytearray(zero)
        expected[123:123 + len(patch)] = patch
        if block1 != bytes(expected):
            raise OSError("partial block readback mismatch")
    finally:
        os.close(fd)

    print("regular-file create/write/fsync workload passed")


if __name__ == "__main__":
    main()
PYEOF
  chmod +x "$WORKLOAD_PY"
}

start_mount() {
  quote_cmd "$KAFS_BIN" "$IMAGE_ABS" "$MOUNT_DIR" -f -o "$MOUNT_OPTIONS" \
    >"$REPORT_DIR/mount.cmd"

  set +e
  KAFS_DEBUG="$KAFS_DEBUG_LEVEL" "$KAFS_BIN" "$IMAGE_ABS" "$MOUNT_DIR" -f -o "$MOUNT_OPTIONS" \
    >"$MOUNT_LOG" 2>&1 &
  KAFS_PID=$!
  set -e

  local waited=0
  local step_ms=100
  while [[ "$waited" -lt "$MOUNT_TIMEOUT_MS" ]]; do
    if is_mounted "$MOUNT_DIR"; then
      MOUNTED=1
      printf "0\n" >"$REPORT_DIR/mount.status"
      return 0
    fi

    if ! kill -0 "$KAFS_PID" 2>/dev/null; then
      wait "$KAFS_PID" >/dev/null 2>&1 || true
      printf "1\n" >"$REPORT_DIR/mount.status"
      return 1
    fi

    sleep 0.1
    waited=$((waited + step_ms))
  done

  printf "1\n" >"$REPORT_DIR/mount.status"
  return 1
}

run_unmount_attempt() {
  quote_cmd "$@"
  if command -v timeout >/dev/null 2>&1; then
    timeout 5s "$@"
  else
    "$@"
  fi
}

unmount_smoke() {
  local rc=1

  set +e
  {
    if command -v fusermount3 >/dev/null 2>&1; then
      run_unmount_attempt fusermount3 -u "$MOUNT_DIR"
      rc=$?
    elif command -v fusermount >/dev/null 2>&1; then
      run_unmount_attempt fusermount -u "$MOUNT_DIR"
      rc=$?
    else
      run_unmount_attempt umount "$MOUNT_DIR"
      rc=$?
    fi
  } >"$REPORT_DIR/unmount.stdout" 2>"$REPORT_DIR/unmount.stderr" || rc=$?
  set -e

  if is_mounted "$MOUNT_DIR"; then
    {
      echo "primary unmount failed or left mount active; trying umount"
      run_unmount_attempt umount "$MOUNT_DIR"
    } >>"$REPORT_DIR/unmount.stdout" 2>>"$REPORT_DIR/unmount.stderr" || true
  fi

  if [[ -n "${KAFS_PID:-}" ]]; then
    wait "$KAFS_PID" >/dev/null 2>&1 || true
    KAFS_PID=""
  fi

  if is_mounted "$MOUNT_DIR"; then
    rc=1
  else
    MOUNTED=0
    rc=0
  fi
  printf "%s\n" "$rc" >"$REPORT_DIR/unmount.status"
  return "$rc"
}

# shellcheck disable=SC2317
cleanup() {
  set +e
  if [[ "$MOUNTED" -eq 1 && -n "${MOUNT_DIR:-}" ]]; then
    if command -v fusermount3 >/dev/null 2>&1; then
      fusermount3 -u "$MOUNT_DIR" >/dev/null 2>&1 || true
      fusermount3 -u -z "$MOUNT_DIR" >/dev/null 2>&1 || true
    elif command -v fusermount >/dev/null 2>&1; then
      fusermount -u "$MOUNT_DIR" >/dev/null 2>&1 || true
    else
      umount "$MOUNT_DIR" >/dev/null 2>&1 || true
    fi
  fi
  if [[ -n "${KAFS_PID:-}" ]]; then
    kill "$KAFS_PID" >/dev/null 2>&1 || true
    wait "$KAFS_PID" >/dev/null 2>&1 || true
  fi
  if [[ "$KEEP_WORKDIR" -eq 0 && -n "${WORKDIR:-}" && "$USER_MOUNT_DIR" -eq 0 ]]; then
    rm -rf "$WORKDIR"
  fi
}
trap cleanup EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image)
      [[ $# -ge 2 ]] || die "missing value for --image"
      IMAGE="$2"
      shift 2
      ;;
    --yes)
      YES=1
      shift
      ;;
    --report-root)
      [[ $# -ge 2 ]] || die "missing value for --report-root"
      REPORT_ROOT="$2"
      shift 2
      ;;
    --report-dir)
      [[ $# -ge 2 ]] || die "missing value for --report-dir"
      REPORT_DIR="$2"
      shift 2
      ;;
    --mount-dir)
      [[ $# -ge 2 ]] || die "missing value for --mount-dir"
      MOUNT_DIR="$2"
      USER_MOUNT_DIR=1
      shift 2
      ;;
    --mount-options)
      [[ $# -ge 2 ]] || die "missing value for --mount-options"
      MOUNT_OPTIONS="$2"
      shift 2
      ;;
    --timeout-ms)
      [[ $# -ge 2 ]] || die "missing value for --timeout-ms"
      MOUNT_TIMEOUT_MS="$2"
      shift 2
      ;;
    --keep-workdir)
      KEEP_WORKDIR=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      die "unknown option: $1"
      ;;
  esac
done

[[ -n "$IMAGE" ]] || die "--image is required"
[[ "$YES" -eq 1 ]] || die "--yes is required because this smoke mutates IMAGE"
validate_unsigned "--timeout-ms" "$MOUNT_TIMEOUT_MS"
[[ "$MOUNT_TIMEOUT_MS" -gt 0 ]] || die "--timeout-ms must be greater than zero"
validate_mount_options "$MOUNT_OPTIONS"
command -v python3 >/dev/null 2>&1 || die "python3 not found"

[[ -f "$IMAGE" ]] || die "image not found: $IMAGE"
[[ -r "$IMAGE" && -w "$IMAGE" ]] || die "image must be readable and writable: $IMAGE"
IMAGE_ABS=$(abs_existing_path "$IMAGE")

KAFS_BIN=$(resolve_exe "$KAFS_BIN" "KAFS_BIN")
KAFSDUMP_BIN=$(resolve_exe "$KAFSDUMP_BIN" "KAFSDUMP_BIN")
FSCK_BIN=$(resolve_exe "$FSCK_BIN" "FSCK_BIN")

if [[ -z "$REPORT_DIR" ]]; then
  REPORT_DIR="$REPORT_ROOT/$STAMP"
fi
mkdir -p "$REPORT_DIR"
REPORT_DIR=$(cd "$REPORT_DIR" && pwd)

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/kafs-v6-write-smoke.XXXXXX")
if [[ -z "$MOUNT_DIR" ]]; then
  MOUNT_DIR="$WORKDIR/mnt"
else
  mkdir -p "$MOUNT_DIR"
  MOUNT_DIR=$(cd "$MOUNT_DIR" && pwd)
fi
mkdir -p "$MOUNT_DIR"

if is_mounted "$MOUNT_DIR"; then
  die "mountpoint is already mounted: $MOUNT_DIR"
fi
if [[ -n "$(find "$MOUNT_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  die "mountpoint must be empty: $MOUNT_DIR"
fi

MOUNT_LOG="$REPORT_DIR/mount.log"
WORKLOAD_PY="$REPORT_DIR/workload.py"
WORKLOAD_NAME="kafs-v6-controlled-write-smoke-${STAMP}.bin"
WORKLOAD_PATH="$MOUNT_DIR/$WORKLOAD_NAME"

cat >"$REPORT_DIR/manifest.env" <<EOF
image=$IMAGE_ABS
report_dir=$REPORT_DIR
mount_dir=$MOUNT_DIR
mount_options=$MOUNT_OPTIONS
timeout_ms=$MOUNT_TIMEOUT_MS
kafs_debug=$KAFS_DEBUG_LEVEL
kafs_bin=$KAFS_BIN
kafsdump_bin=$KAFSDUMP_BIN
fsck_bin=$FSCK_BIN
workload_file=$WORKLOAD_NAME
EOF

write_workload_script

overall_rc=0
unmount_rc=0
did_mount=0

if ! write_image_meta before; then
  overall_rc=1
fi

if ! capture_command kafsdump-before "$REPORT_DIR/kafsdump-before.json" \
  "$REPORT_DIR/kafsdump-before.stderr" "$KAFSDUMP_BIN" --json "$IMAGE_ABS"; then
  overall_rc=1
fi

if ! capture_command fsck-before "$REPORT_DIR/fsck-before.stdout" \
  "$REPORT_DIR/fsck-before.stderr" "$FSCK_BIN" --balanced-check "$IMAGE_ABS"; then
  overall_rc=1
fi

if [[ "$overall_rc" -eq 0 ]]; then
  if ! start_mount; then
    overall_rc=1
  else
    did_mount=1
    if ! capture_command workload "$REPORT_DIR/workload.stdout" "$REPORT_DIR/workload.stderr" \
      python3 "$WORKLOAD_PY" "$WORKLOAD_PATH"; then
      overall_rc=1
    fi

    if ! unmount_smoke; then
      unmount_rc=1
      overall_rc=1
    fi
  fi
fi

if [[ "$did_mount" -eq 1 && "$unmount_rc" -eq 0 && "$MOUNTED" -eq 0 ]]; then
  if ! write_image_meta after; then
    overall_rc=1
  fi
  if ! capture_command kafsdump-after "$REPORT_DIR/kafsdump-after.json" \
    "$REPORT_DIR/kafsdump-after.stderr" "$KAFSDUMP_BIN" --json "$IMAGE_ABS"; then
    overall_rc=1
  fi
  if ! capture_command fsck-after "$REPORT_DIR/fsck-after.stdout" \
    "$REPORT_DIR/fsck-after.stderr" "$FSCK_BIN" --balanced-check "$IMAGE_ABS"; then
    overall_rc=1
  fi
fi

if [[ "$did_mount" -eq 1 && -f "$MOUNT_LOG" ]] &&
  ! grep -Fq "format v6 controlled write mount" "$MOUNT_LOG"; then
  echo "mount log missing controlled write admission message" \
    >"$REPORT_DIR/mount-log-admission.status"
  overall_rc=1
else
  echo "0" >"$REPORT_DIR/mount-log-admission.status"
fi

status="PASS"
if [[ "$overall_rc" -ne 0 ]]; then
  status="FAIL"
fi

cat >"$REPORT_DIR/SUMMARY.md" <<EOF
# KAFS v6 Controlled Write Smoke

- status: \`$status\`
- image: \`$IMAGE_ABS\`
- mount_options: \`$MOUNT_OPTIONS\`
- workload: \`$WORKLOAD_NAME\`
- report_dir: \`$REPORT_DIR\`

## Artifacts

- \`manifest.env\`
- \`workload.py\`
- \`mount.cmd\`
- \`mount.log\`
- \`workload.stdout\` / \`workload.stderr\`
- \`kafsdump-before.json\` / \`kafsdump-after.json\`
- \`fsck-before.stdout\` / \`fsck-after.stdout\`
- \`image-before.stat\` / \`image-after.stat\`
- \`image-before.sha256\` / \`image-after.sha256\`

This smoke uses explicit regular-file create/write/fsync operations. It does
not use cp, copy_file_range, ioctl copy, or reflink as acceptance evidence.
EOF

echo "v6 controlled write smoke $status"
echo "report: $REPORT_DIR"
exit "$overall_rc"
