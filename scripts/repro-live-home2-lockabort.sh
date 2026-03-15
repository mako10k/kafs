#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 5 ]]; then
  echo "Usage: $0 <kafs_bin> <image_or_device> <mountpoint> [mkdir_count] [timeout_sec]" >&2
  echo "  example: $0 /home/mako10k/kafs/src/kafs /dev/sde1 /home2 20000 30" >&2
  exit 2
fi

kafs_bin="$1"
image_or_device="$2"
mnt="$3"
mkdir_count="${4:-20000}"
timeout_sec="${5:-30}"

if [[ ! -x "$kafs_bin" ]]; then
  echo "kafs binary not executable: $kafs_bin" >&2
  exit 2
fi

mkdir -p "$mnt"
run_id="$(date +%s)"
log_file="/tmp/kafs-live-repro-${run_id}.log"
op_log="/tmp/kafs-live-repro-op-${run_id}.log"
stat_pid=""
mount_pid=""
workdir="$mnt/.kafs-repro-${run_id}"

cleanup() {
  set +e
  if [[ -n "$stat_pid" ]]; then
    kill "$stat_pid" >/dev/null 2>&1 || true
    wait "$stat_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "$mount_pid" ]]; then
    kill "$mount_pid" >/dev/null 2>&1 || true
    wait "$mount_pid" >/dev/null 2>&1 || true
  fi
  if findmnt -n "$mnt" >/dev/null 2>&1; then
    timeout 5 fusermount3 -u -z "$mnt" >/dev/null 2>&1 || timeout 5 sudo umount -l "$mnt" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if findmnt -n "$mnt" >/dev/null 2>&1; then
  timeout 5 fusermount3 -u -z "$mnt" >/dev/null 2>&1 || timeout 5 sudo umount -l "$mnt" >/dev/null 2>&1 || true
fi

if [[ "$image_or_device" == /dev/* ]]; then
  sudo "$kafs_bin" "$image_or_device" "$mnt" -f -o rw,multi_thread=4,allow_other,dev,suid >"$log_file" 2>&1 &
else
  "$kafs_bin" "$image_or_device" "$mnt" -f -o rw,multi_thread=4,allow_other,dev,suid >"$log_file" 2>&1 &
fi
mount_pid="$!"

sleep 2
if ! timeout 5 stat -f "$mnt" >/dev/null 2>&1; then
  echo "MOUNT_FAIL log=$log_file" >&2
  exit 125
fi

(
  while :; do
    stat -f "$mnt" >/dev/null 2>&1 || break
  done
) &
stat_pid="$!"

set +e
if [[ "$image_or_device" == /dev/* ]]; then
  sudo mkdir -p "$workdir" >/dev/null 2>&1 || true
  sudo chown "$(id -u):$(id -g)" "$workdir" >/dev/null 2>&1 || true
fi

timeout "$timeout_sec" bash -c '
set -euo pipefail
mnt="$1"
workdir="$2"
mkdir_count="$3"
mkdir -p "$workdir"
i=1
while [[ "$i" -le "$mkdir_count" ]]; do
  if (( i % 100 == 0 )); then
    echo "progress: mkdir=$i"
  fi
  mkdir "$workdir/d$i"
  i=$((i+1))
done
' _ "$mnt" "$workdir" "$mkdir_count" >"$op_log" 2>&1
work_rc=$?
set -e

set +e
timeout 5 stat -f "$mnt" >/dev/null 2>&1
post_stat_rc=$?
set -e

if grep -Eq 'lock-order-violation|caught fatal signal 6|Aborted' "$log_file"; then
  echo "BAD lock-abort signature detected (work_rc=$work_rc post_stat_rc=$post_stat_rc) log=$log_file op=$op_log"
  exit 1
fi

if [[ $work_rc -ne 0 ]]; then
  echo "BAD workload failed (work_rc=$work_rc post_stat_rc=$post_stat_rc) log=$log_file op=$op_log"
  exit 1
fi

echo "GOOD workload completed without lock-abort signature (work_rc=$work_rc post_stat_rc=$post_stat_rc) log=$log_file op=$op_log"
exit 0
