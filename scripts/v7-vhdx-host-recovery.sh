#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WORKLOAD_BIN=${KAFS_V7_VHDX_WORKLOAD_BIN:-$ROOT_DIR/tests/v7_inspection_mount_smoketest}
FSCK_BIN=${KAFS_TEST_FSCK:-$ROOT_DIR/src/fsck.kafs}
KAFSDUMP_BIN=${KAFS_TEST_KAFSDUMP:-$ROOT_DIR/src/kafsdump}
KAFS_V7_BIN=${KAFS_TEST_KAFS_V7:-$ROOT_DIR/src/kafs-v7}

MODE=""
FAULT=""
STATE_ROOT=""
STATE_DIR=""
HOST_EVIDENCE=""

usage() {
  cat <<'EOF'
Usage:
  scripts/v7-vhdx-host-recovery.sh --preflight --state-root DIR
  scripts/v7-vhdx-host-recovery.sh --arm --fault POINT \
    --state-root DIR --state-dir DIR
  scripts/v7-vhdx-host-recovery.sh --verify --fault POINT \
    --state-root DIR --state-dir DIR --host-evidence FILE

POINT is one of journal_publish, checkpoint_copy, metadata_apply, or
journal_reclaim.

This runner uses only a dedicated regular-file KAFS image below STATE_ROOT. It
rejects DrvFs paths and never opens, formats, mounts, or raw-writes a VHD/VHDX.
The Windows controller owns distro termination and restart.
EOF
}

die() {
  echo "v7 VHDX recovery runner: $*" >&2
  exit 2
}

resolve_exe() {
  local value="$1"
  local label="$2"
  [[ -x "$value" ]] || die "$label is not executable: $value"
  (cd "$(dirname "$value")" && printf '%s/%s\n' "$(pwd)" "$(basename "$value")")
}

validate_runtime_pause_contract() {
  local binary="$1"
  local symbol
  for symbol in KAFS_V7_TEST_PAUSE_POINT KAFS_V7_TEST_PAUSE_MARKER; do
    LC_ALL=C grep -a -F -q -- "$symbol" "$binary" ||
      die "kafs-v7 runtime lacks the required pause contract: $symbol"
  done
}

canonical_future_path() {
  realpath -m -- "$1"
}

validate_fault() {
  case "$1" in
    journal_publish|checkpoint_copy|metadata_apply|journal_reclaim) ;;
    *) die "unsupported fault point: ${1:-<missing>}" ;;
  esac
}

validate_state_root() {
  [[ -n "$STATE_ROOT" && "$STATE_ROOT" == /* ]] || die "--state-root must be absolute"
  STATE_ROOT=$(canonical_future_path "$STATE_ROOT")
  local home_root
  home_root=$(realpath -e -- "$HOME")
  case "$STATE_ROOT" in
    /|/home|/root|/mnt|/mnt/*) die "unsafe or non-VHDX state root: $STATE_ROOT" ;;
  esac
  case "$STATE_ROOT" in
    "$home_root"/*) ;;
    *) die "state root must be below the current WSL home: $home_root" ;;
  esac

  local ancestor="$STATE_ROOT"
  while [[ ! -e "$ancestor" ]]; do
    local parent
    parent=$(dirname "$ancestor")
    [[ "$parent" != "$ancestor" ]] || die "cannot resolve state-root ancestor"
    ancestor="$parent"
  done
  [[ -d "$ancestor" && ! -L "$ancestor" ]] ||
    die "state-root ancestor must be a real directory: $ancestor"

  local root_source state_source root_fstype state_fstype
  root_source=$(findmnt -T "$ROOT_DIR" -n -o SOURCE)
  root_fstype=$(findmnt -T "$ROOT_DIR" -n -o FSTYPE)
  state_source=$(findmnt -T "$ancestor" -n -o SOURCE)
  state_fstype=$(findmnt -T "$ancestor" -n -o FSTYPE)
  [[ "$root_fstype" == ext4 && "$state_fstype" == ext4 ]] ||
    die "repository and state root must both be on ext4"
  [[ "$root_source" == "$state_source" ]] ||
    die "state root is not on the active Ubuntu VHDX filesystem ($state_source != $root_source)"
}

validate_state_dir_path() {
  [[ -n "$STATE_DIR" && "$STATE_DIR" == /* ]] || die "--state-dir must be absolute"
  STATE_DIR=$(canonical_future_path "$STATE_DIR")
  case "$STATE_DIR" in
    "$STATE_ROOT"/*) ;;
    *) die "state directory must be below state root" ;;
  esac
  [[ "$STATE_DIR" != "$STATE_ROOT" ]] || die "state directory must not equal state root"
}

write_arm_context() {
  local context="$STATE_DIR/arm-context.json"
  local root_source root_fstype head dirty
  root_source=$(findmnt -T "$STATE_DIR" -n -o SOURCE)
  root_fstype=$(findmnt -T "$STATE_DIR" -n -o FSTYPE)
  head=$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || printf unknown)
  dirty=false
  [[ -z "$(git -C "$ROOT_DIR" status --porcelain --untracked-files=no 2>/dev/null)" ]] || dirty=true
  python3 - "$context" "$FAULT" "$root_source" "$root_fstype" "$head" "$dirty" <<'PY'
import json
import os
from pathlib import Path
import platform
import sys
from datetime import datetime, timezone

path, fault, source, fstype, head, dirty = sys.argv[1:]
record = {
    "schema": "KAFS.V7VhdxArmContext.v1",
    "created_utc": datetime.now(timezone.utc).isoformat(),
    "fault": fault,
    "distro": os.environ.get("WSL_DISTRO_NAME", "unknown"),
    "kernel": platform.release(),
    "filesystem_source": source,
    "filesystem_type": fstype,
    "git_head": head,
    "tracked_worktree_dirty": dirty == "true",
    "dedicated_regular_file_image": True,
    "raw_vhdx_access": False,
    "real_media_qualified": False,
    "physical_power_interruption": False,
    "controller_independent_wear_qualified": False,
    "release_candidate_qualified": False,
}
Path(path).write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
  sync -f "$context"
}

validate_host_evidence() {
  [[ -n "$HOST_EVIDENCE" && -f "$HOST_EVIDENCE" && ! -L "$HOST_EVIDENCE" ]] ||
    die "--host-evidence must name a regular JSON file"
  python3 - "$HOST_EVIDENCE" "$FAULT" <<'PY'
import json
from datetime import datetime
from pathlib import Path
import sys

path = Path(sys.argv[1])
fault = sys.argv[2]
try:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
except (OSError, json.JSONDecodeError) as exc:
    raise SystemExit(f"invalid host evidence: {exc}")
required = {
    "schema": "KAFS.V7VhdxHostController.v1",
    "fault": fault,
    "controller": "wsl.exe --terminate",
    "terminate_exit_code": 0,
    "restart_exit_code": 0,
    "raw_vhdx_access": False,
    "real_media_qualified": False,
    "physical_power_interruption": False,
    "controller_independent_wear_qualified": False,
    "release_candidate_qualified": False,
}
for key, expected in required.items():
    if value.get(key) != expected:
        raise SystemExit(f"invalid host evidence field {key}: {value.get(key)!r}")
for key in ("distro", "vhdx_path", "marker_observed_utc", "terminated_utc", "restarted_utc"):
    if not isinstance(value.get(key), str) or not value[key]:
        raise SystemExit(f"missing host evidence field: {key}")
for key in ("vhdx_length_before", "vhdx_length_after"):
    if not isinstance(value.get(key), int) or value[key] <= 0:
        raise SystemExit(f"invalid host evidence field: {key}")
expected_distro = __import__("os").environ.get("WSL_DISTRO_NAME")
if expected_distro and value["distro"] != expected_distro:
    raise SystemExit(
        f"host evidence distro mismatch: {value['distro']!r} != {expected_distro!r}"
    )
if not value["vhdx_path"].lower().endswith(".vhdx"):
    raise SystemExit("host evidence path is not a VHDX")
try:
    marker_time = datetime.fromisoformat(value["marker_observed_utc"].replace("Z", "+00:00"))
    terminated_time = datetime.fromisoformat(value["terminated_utc"].replace("Z", "+00:00"))
    restarted_time = datetime.fromisoformat(value["restarted_utc"].replace("Z", "+00:00"))
except ValueError as exc:
    raise SystemExit(f"invalid host evidence timestamp: {exc}")
if not marker_time <= terminated_time <= restarted_time:
    raise SystemExit("host evidence timestamps are out of order")
PY
}

write_verification_manifest() {
  local manifest="$STATE_DIR/verification-manifest.json"
  local image_sha
  image_sha=$(sha256sum "$STATE_DIR/vhdx-recovery.img" | awk '{print $1}')
  python3 - "$manifest" "$FAULT" "$image_sha" <<'PY'
import json
from pathlib import Path
import sys
from datetime import datetime, timezone

path, fault, image_sha = sys.argv[1:]
record = {
    "schema": "KAFS.V7VhdxRecoveryEvidence.v1",
    "verified_utc": datetime.now(timezone.utc).isoformat(),
    "fault": fault,
    "image_sha256_after_recovery": image_sha,
    "host_terminate_restart_observed": True,
    "fsck_full_check_passed": True,
    "kafsdump_completed": True,
    "payload_and_recovery_diagnostic_passed": True,
    "dedicated_regular_file_image": True,
    "raw_vhdx_access": False,
    "real_media_qualified": False,
    "physical_power_interruption": False,
    "controller_independent_wear_qualified": False,
    "release_candidate_qualified": False,
}
Path(path).write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
  local hashes
  hashes=$(mktemp "${TMPDIR:-/tmp}/kafs-v7-vhdx-hashes.XXXXXX")
  (
    cd "$STATE_DIR"
    find . -maxdepth 1 -type f ! -name artifacts.sha256 -print0 |
      sort -z | xargs -0 sha256sum >"$hashes"
    mv "$hashes" artifacts.sha256
  )
  sync -f "$manifest"
  sync -f "$STATE_DIR/artifacts.sha256"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --preflight|--arm|--verify)
      [[ -z "$MODE" ]] || die "choose exactly one mode"
      MODE=${1#--}
      shift
      ;;
    --fault)
      [[ $# -ge 2 ]] || die "missing value for --fault"
      FAULT="$2"
      shift 2
      ;;
    --state-root)
      [[ $# -ge 2 ]] || die "missing value for --state-root"
      STATE_ROOT="$2"
      shift 2
      ;;
    --state-dir)
      [[ $# -ge 2 ]] || die "missing value for --state-dir"
      STATE_DIR="$2"
      shift 2
      ;;
    --host-evidence)
      [[ $# -ge 2 ]] || die "missing value for --host-evidence"
      HOST_EVIDENCE="$2"
      shift 2
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

[[ -n "$MODE" ]] || { usage >&2; die "a mode is required"; }
command -v findmnt >/dev/null 2>&1 || die "findmnt is required"
command -v python3 >/dev/null 2>&1 || die "python3 is required"
command -v realpath >/dev/null 2>&1 || die "realpath is required"
command -v sha256sum >/dev/null 2>&1 || die "sha256sum is required"
command -v grep >/dev/null 2>&1 || die "grep is required"
[[ "$(uname -r)" == *[Mm]icrosoft* ]] || die "this runner requires WSL2"
validate_state_root
WORKLOAD_BIN=$(resolve_exe "$WORKLOAD_BIN" "VHDX workload")
FSCK_BIN=$(resolve_exe "$FSCK_BIN" "fsck.kafs")
KAFSDUMP_BIN=$(resolve_exe "$KAFSDUMP_BIN" "kafsdump")
KAFS_V7_BIN=$(resolve_exe "$KAFS_V7_BIN" "kafs-v7")
validate_runtime_pause_contract "$KAFS_V7_BIN"
export KAFS_TEST_KAFS_V7="$KAFS_V7_BIN"

if [[ "$MODE" == preflight ]]; then
  [[ -z "$FAULT" && -z "$STATE_DIR" && -z "$HOST_EVIDENCE" ]] ||
    die "preflight accepts only --state-root"
  printf 'KAFS_V7_VHDX_PREFLIGHT PASS state_root=%s source=%s fstype=ext4 runtime=%s\n' \
    "$STATE_ROOT" "$(findmnt -T "$ROOT_DIR" -n -o SOURCE)" "$KAFS_V7_BIN"
  exit 0
fi

validate_fault "$FAULT"
validate_state_dir_path

if [[ "$MODE" == arm ]]; then
  [[ -z "$HOST_EVIDENCE" ]] || die "arm does not accept --host-evidence"
  [[ ! -e "$STATE_DIR" ]] || die "arm state directory already exists: $STATE_DIR"
  mkdir -p "$STATE_DIR"
  [[ ! -L "$STATE_DIR" ]] || die "arm state directory must not be a symlink"
  write_arm_context
  marker="$STATE_DIR/pause.marker"
  echo "KAFS_V7_VHDX_ARM fault=$FAULT state_dir=$STATE_DIR marker=$marker"
  exec env KAFS_V7_TEST_PAUSE_POINT="$FAULT" KAFS_V7_TEST_PAUSE_MARKER="$marker" \
    "$WORKLOAD_BIN" --vhdx-arm "$FAULT" "$STATE_DIR"
fi

[[ "$MODE" == verify ]] || die "internal mode error"
[[ -d "$STATE_DIR" && ! -L "$STATE_DIR" ]] || die "verify state directory is invalid"
[[ -f "$STATE_DIR/pause.marker" && ! -L "$STATE_DIR/pause.marker" ]] ||
  die "durable pause marker is missing"
[[ "$(tr -d '\r\n' <"$STATE_DIR/pause.marker")" == "$FAULT" ]] ||
  die "pause marker fault does not match $FAULT"
[[ -f "$STATE_DIR/vhdx-recovery.meta" && -f "$STATE_DIR/vhdx-recovery.img" ]] ||
  die "armed image state is incomplete"
[[ ! -e "$STATE_DIR/vhdx-verify.ok" ]] || die "state directory was already verified"
validate_host_evidence
cp -- "$HOST_EVIDENCE" "$STATE_DIR/host-controller.json"
sync -f "$STATE_DIR/host-controller.json"
"$WORKLOAD_BIN" --vhdx-verify "$FAULT" "$STATE_DIR"
"$FSCK_BIN" --full-check "$STATE_DIR/vhdx-recovery.img" \
  >"$STATE_DIR/fsck-full-check.stdout" 2>"$STATE_DIR/fsck-full-check.stderr"
"$KAFSDUMP_BIN" --json "$STATE_DIR/vhdx-recovery.img" \
  >"$STATE_DIR/kafsdump.json" 2>"$STATE_DIR/kafsdump.stderr"
write_verification_manifest
echo "KAFS_V7_VHDX_VERIFY $FAULT PASS state_dir=$STATE_DIR"
