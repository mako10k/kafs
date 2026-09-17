#!/usr/bin/env bash
# Bounded, destructive normal-workload rehearsal on one explicitly identified USB card.
# This is not the format-v7 real-media qualification or a power-cut test.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BIN_DIR=${KAFS_V7_REAL_MEDIA_BIN_DIR:-"$SCRIPT_DIR/../src"}
DEVICE_BY_ID=
EXPECTED_SIZE=
EXPECTED_MAJOR_MINOR=
CONFIRM_ERASE=
CONFIRM_DEVICE=
RESUME_FROM=
REPORT_DIR=
EXECUTE=0
MOUNT_PID=
MOUNT_DIR=

usage() {
  printf '%s\n' 'Usage: v7-real-media-nonpower-normal.sh --device-by-id PATH --expected-size-bytes N --expected-major-minor N:N [--bin-dir DIR] [--execute --confirm-erase PATH --report-dir NEW_DIR]'
  printf '%s\n' '   or: v7-real-media-nonpower-normal.sh --device-by-id PATH --expected-size-bytes N --expected-major-minor N:N --resume-from PRIOR_JSON [--bin-dir DIR] [--execute --confirm-device PATH --report-dir NEW_DIR]'
  printf '%s\n' 'Without --execute this performs read-only target and binary preflight.'
  printf '%s\n' 'Fresh execution reformats once; resume execution performs no format and completes only the known missing O_TRUNC case. Neither mode cuts power or injects faults.'
}

die() { printf 'ERROR: %s\n' "$*" >&2; exit 2; }

is_our_mount() {
  [[ -n "$MOUNT_DIR" ]] && findmnt -rn -M "$MOUNT_DIR" -o FSTYPE 2>/dev/null |
    grep -qx 'fuse.kafs-v7'
}

stop_mount() {
  if is_our_mount; then
    fusermount3 -u "$MOUNT_DIR" || return 1
  fi
  if [[ -n "$MOUNT_PID" ]]; then
    wait "$MOUNT_PID" || true
    MOUNT_PID=
  fi
  ! is_our_mount
}

on_exit() {
  local status=$?
  trap - EXIT
  if [[ -n "$MOUNT_PID" ]] && ! stop_mount; then
    printf 'WARNING: owned FUSE mount remains active at %s\n' "$MOUNT_DIR" >&2
    status=1
  fi
  if [[ "$EXECUTE" -eq 1 && -n "$REPORT_DIR" && -d "$REPORT_DIR" ]]; then
    if [[ "$status" -eq 0 ]]; then
      if [[ -n "$RESUME_FROM" ]]; then
        printf 'PASS_WITH_COMPLETION: linked prior case evidence; not real-media qualification\n' >"$REPORT_DIR/result.txt"
      else
        printf 'PASS: bounded normal workload; not real-media qualification\n' >"$REPORT_DIR/result.txt"
      fi
    else
      printf 'FAIL/INCOMPLETE: inspect logs and mount state; not real-media qualification\n' >"$REPORT_DIR/result.txt"
    fi
  fi
  exit "$status"
}
trap on_exit EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --device-by-id) [[ $# -ge 2 ]] || die 'missing device-by-id'; DEVICE_BY_ID=$2; shift 2 ;;
    --expected-size-bytes) [[ $# -ge 2 ]] || die 'missing expected-size-bytes'; EXPECTED_SIZE=$2; shift 2 ;;
    --expected-major-minor) [[ $# -ge 2 ]] || die 'missing expected-major-minor'; EXPECTED_MAJOR_MINOR=$2; shift 2 ;;
    --confirm-erase) [[ $# -ge 2 ]] || die 'missing confirm-erase'; CONFIRM_ERASE=$2; shift 2 ;;
    --confirm-device) [[ $# -ge 2 ]] || die 'missing confirm-device'; CONFIRM_DEVICE=$2; shift 2 ;;
    --resume-from) [[ $# -ge 2 ]] || die 'missing resume-from'; RESUME_FROM=$2; shift 2 ;;
    --report-dir) [[ $# -ge 2 ]] || die 'missing report-dir'; REPORT_DIR=$2; shift 2 ;;
    --bin-dir) [[ $# -ge 2 ]] || die 'missing bin-dir'; BIN_DIR=$2; shift 2 ;;
    --execute) EXECUTE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown option: $1" ;;
  esac
done

[[ "$DEVICE_BY_ID" == /dev/disk/by-id/* ]] || die 'device must be an absolute /dev/disk/by-id path'
[[ "$EXPECTED_SIZE" =~ ^[0-9]+$ && "$EXPECTED_SIZE" -gt 0 ]] || die 'invalid expected size'
[[ "$EXPECTED_MAJOR_MINOR" =~ ^[0-9]+:[0-9]+$ ]] || die 'invalid expected major:minor'
[[ -L "$DEVICE_BY_ID" ]] || die 'device-by-id is not a symlink'
DEVICE=$(readlink -e -- "$DEVICE_BY_ID") || die 'device symlink does not resolve'
[[ -b "$DEVICE" ]] || die 'resolved device is not a block device'
[[ $(lsblk -dnro TYPE "$DEVICE") == disk ]] || die 'resolved device is not a whole disk'
[[ $(lsblk -dnro TRAN "$DEVICE") == usb ]] || die 'resolved device is not USB transport'
[[ $(lsblk -dnro MAJ:MIN "$DEVICE" | tr -d ' ') == "$EXPECTED_MAJOR_MINOR" ]] || die 'major:minor mismatch'
[[ $(lsblk -dnbo SIZE "$DEVICE" | tr -d ' ') == "$EXPECTED_SIZE" ]] || die 'capacity mismatch'
command -v fuser >/dev/null || die 'fuser unavailable for raw-device busy check'
command -v timeout >/dev/null || die 'timeout unavailable for raw-device busy check'

check_device_idle() {
  if lsblk -nrpo MOUNTPOINT "$DEVICE" | grep -q '[^[:space:]]'; then
    die 'device or one of its partitions is mounted'
  fi
  if findmnt -rn -t fuse.kafs-v7 -o TARGET | grep -q '[^[:space:]]'; then
    die 'a v7 FUSE mount is active; raw-device exclusivity is unproven'
  fi
  local busy_status=0
  timeout 90 fuser "$DEVICE" >/dev/null 2>&1 || busy_status=$?
  case "$busy_status" in
    0) die 'raw device is open by another process' ;;
    1) ;;
    *) die "raw-device busy check failed (status $busy_status)" ;;
  esac
}

check_device_idle

for tool in kafs-v7 mkfs.kafs fsck.kafs kafsdump; do
  [[ -x "$BIN_DIR/$tool" ]] || die "missing executable: $BIN_DIR/$tool"
done
[[ -f "$SCRIPT_DIR/v7-real-media-normal-workload.py" ]] || die 'workload engine missing beside script'
command -v python3 >/dev/null || die 'python3 unavailable'
command -v fusermount3 >/dev/null || die 'fusermount3 unavailable'
command -v sha256sum >/dev/null || die 'sha256sum unavailable'

printf 'TARGET by-id=%s resolved=%s size=%s major:minor=%s transport=usb\n' \
  "$DEVICE_BY_ID" "$DEVICE" "$EXPECTED_SIZE" "$EXPECTED_MAJOR_MINOR"
if [[ -n "$RESUME_FROM" ]]; then
  [[ "$RESUME_FROM" == /* && -f "$RESUME_FROM" ]] || die 'resume-from must be an existing absolute report path'
  printf '%s\n' 'PLAN: zero reformat; pre/post offline checks; only missing O_TRUNC and proof; read-only proof remount; zero intentional process stops or power cuts.'
else
  printf '%s\n' 'PLAN: one whole-device v7/1KiB reformat; 12 normal cases; offline checks; read-only proof remount; zero intentional process stops or power cuts.'
fi
if [[ "$EXECUTE" -eq 0 ]]; then
  [[ -z "$CONFIRM_ERASE" && -z "$CONFIRM_DEVICE" && -z "$REPORT_DIR" ]] ||
    die 'confirmation/report directory require --execute'
  exit 0
fi

if [[ -n "$RESUME_FROM" ]]; then
  [[ -z "$CONFIRM_ERASE" && "$CONFIRM_DEVICE" == "$DEVICE_BY_ID" ]] ||
    die 'resume requires exact confirm-device and forbids confirm-erase'
else
  [[ -z "$CONFIRM_DEVICE" && "$CONFIRM_ERASE" == "$DEVICE_BY_ID" ]] ||
    die 'fresh run requires exact confirm-erase and forbids confirm-device'
fi
[[ -r "$DEVICE" && -w "$DEVICE" ]] || die 'device is not readable and writable by this account; no device write attempted'
[[ "$REPORT_DIR" == /* && ! -e "$REPORT_DIR" ]] || die 'report-dir must be a new absolute path'
mkdir -m 0700 -- "$REPORT_DIR"
MOUNT_DIR="$REPORT_DIR/mnt"
mkdir -m 0700 -- "$MOUNT_DIR"
printf 'by-id=%s\nresolved=%s\nsize-bytes=%s\nmajor:minor=%s\n' \
  "$DEVICE_BY_ID" "$DEVICE" "$EXPECTED_SIZE" "$EXPECTED_MAJOR_MINOR" >"$REPORT_DIR/target.txt"
uname -a >"$REPORT_DIR/host.txt"
sha256sum "$SCRIPT_DIR/v7-real-media-nonpower-normal.sh" \
  "$SCRIPT_DIR/v7-real-media-normal-workload.py" "$BIN_DIR"/{kafs-v7,mkfs.kafs,fsck.kafs,kafsdump} \
  >"$REPORT_DIR/executables.sha256"

# Repeat volatile identity and mount checks immediately before the device operation.
[[ $(readlink -e -- "$DEVICE_BY_ID") == "$DEVICE" && \
   $(lsblk -dnro MAJ:MIN "$DEVICE" | tr -d ' ') == "$EXPECTED_MAJOR_MINOR" && \
   $(lsblk -dnbo SIZE "$DEVICE" | tr -d ' ') == "$EXPECTED_SIZE" ]] || die 'device identity changed before operation'
check_device_idle
if [[ -z "$RESUME_FROM" ]]; then
  "$BIN_DIR/mkfs.kafs" "$DEVICE" --format-version 7 --blksize-log 10 --yes \
    >"$REPORT_DIR/mkfs.log" 2>&1
fi
"$BIN_DIR/fsck.kafs" --check "$DEVICE" >"$REPORT_DIR/fsck-before.log" 2>&1
"$BIN_DIR/kafsdump" --json "$DEVICE" >"$REPORT_DIR/dump-before.json" 2>"$REPORT_DIR/dump-before.log"
check_device_idle

start_mount() {
  local mode=$1
  local options=$2
  KAFS_IMAGE="$DEVICE" "$BIN_DIR/kafs-v7" "$MOUNT_DIR" "$mode" -f -o "$options" \
    >"$REPORT_DIR/mount-${mode#--}.log" 2>&1 &
  MOUNT_PID=$!
  local attempt
  for ((attempt = 0; attempt < 150; attempt++)); do
    if is_our_mount; then return 0; fi
    if ! kill -0 "$MOUNT_PID" 2>/dev/null; then break; fi
    sleep 0.1
  done
  die "mount failed or timed out: $mode"
}

start_mount --controlled-write-mount 'rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full'
if [[ -n "$RESUME_FROM" ]]; then
  python3 "$SCRIPT_DIR/v7-real-media-normal-workload.py" --mountpoint "$MOUNT_DIR" \
    --block-size 1024 --report "$REPORT_DIR/workload.json" \
    --resume-open-truncate-from "$RESUME_FROM" >"$REPORT_DIR/workload.log" 2>&1
else
  python3 "$SCRIPT_DIR/v7-real-media-normal-workload.py" --mountpoint "$MOUNT_DIR" \
    --block-size 1024 --report "$REPORT_DIR/workload.json" >"$REPORT_DIR/workload.log" 2>&1
fi
stop_mount || die 'controlled-write unmount failed'
"$BIN_DIR/fsck.kafs" --check "$DEVICE" >"$REPORT_DIR/fsck-after.log" 2>&1
"$BIN_DIR/kafsdump" --json "$DEVICE" >"$REPORT_DIR/dump-after.json" 2>"$REPORT_DIR/dump-after.log"

start_mount --inspection-mount ro
sha256sum "$MOUNT_DIR/proof.bin" >"$REPORT_DIR/proof-remount.sha256"
python3 - "$REPORT_DIR/workload.json" "$REPORT_DIR/proof-remount.sha256" "$MOUNT_DIR" "$RESUME_FROM" <<'PY'
import hashlib
import json
import os
import sys
with open(sys.argv[1]) as stream:
    report = json.load(stream)
with open(sys.argv[2]) as stream:
    digest = stream.read().split()[0]
if digest != report['proof']['sha256']:
    raise SystemExit('post-remount proof mismatch')
if sys.argv[4]:
    with open(sys.argv[4], 'rb') as stream:
        prior_bytes = stream.read()
    if (report['status'] != 'PASS_WITH_COMPLETION' or
            report['prior_report_sha256'] != hashlib.sha256(prior_bytes).hexdigest() or
            set(report['combined_cases']) != set(report['expected_cases'])):
        raise SystemExit('linked completion report mismatch')
elif report['status'] != 'PASS':
    raise SystemExit('normal workload report did not pass')
mountpoint = sys.argv[3]
for name in ('normal.bin', 'open-truncate.bin'):
    if os.stat(os.path.join(mountpoint, name)).st_size != 0:
        raise SystemExit('post-remount zero-size mismatch: ' + name)
if len([name for name in os.listdir(mountpoint) if name.startswith('d')]) < 2:
    raise SystemExit('post-remount directory entries missing')
PY
stop_mount || die 'inspection unmount failed'
printf 'PASS report=%s\n' "$REPORT_DIR"
