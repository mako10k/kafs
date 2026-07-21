#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
REPORT_ROOT="$ROOT_DIR/report/v7-controlled-write-qualification"
REPORT_DIR=""
KEEP_WORKDIR=0
MOUNT_TIMEOUT_MS=${KAFS_V7_QUALIFICATION_MOUNT_TIMEOUT_MS:-15000}

WORKLOAD_BIN=${KAFS_V7_QUALIFICATION_WORKLOAD_BIN:-$ROOT_DIR/tests/v7_inspection_mount_smoketest}
KAFS_V7_BIN=${KAFS_TEST_KAFS_V7:-$ROOT_DIR/src/kafs-v7}
MKFS_BIN=${KAFS_TEST_MKFS:-$ROOT_DIR/src/mkfs.kafs}
FSCK_BIN=${KAFS_TEST_FSCK:-$ROOT_DIR/src/fsck.kafs}
KAFSDUMP_BIN=${KAFS_TEST_KAFSDUMP:-$ROOT_DIR/src/kafsdump}
GATE_BIN="$ROOT_DIR/scripts/v7-controlled-write-qualification-gate.sh"

WORK_ROOT=""

usage() {
  cat <<'EOF'
Usage:
  scripts/v7-controlled-write-qualification-dry-run.sh [options]

Run the current format-v7 controlled-write FUSE matrix against temporary sparse
file images and produce a machine-validated evidence report. This command never
accepts a device or caller-supplied image and does not perform a physical power
cut. Exit 77 means the host cannot run the FUSE workload and the report is SKIP.

Options:
  --report-root DIR    Parent directory for timestamped reports
  --report-dir DIR     Exact new or empty report directory
  --timeout-ms N       FUSE mount timeout (default: 15000)
  --keep-workdir       Keep generated sparse images below the report directory
  -h, --help           Show this help
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 2
}

resolve_exe() {
  local value="$1"
  local label="$2"
  if [[ "$value" == */* ]]; then
    [[ -x "$value" ]] || die "$label is not executable: $value"
    (cd "$(dirname "$value")" && printf '%s/%s\n' "$(pwd)" "$(basename "$value")")
    return
  fi
  command -v "$value" >/dev/null 2>&1 || die "$label not found: $value"
  command -v "$value"
}

cleanup() {
  if [[ -n "$WORK_ROOT" && -d "$WORK_ROOT" && "$KEEP_WORKDIR" -eq 0 ]]; then
    find "$WORK_ROOT" -depth -delete
  fi
}
trap cleanup EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
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

[[ "$MOUNT_TIMEOUT_MS" =~ ^[0-9]+$ && "$MOUNT_TIMEOUT_MS" -gt 0 ]] ||
  die "--timeout-ms must be a positive integer"

command -v python3 >/dev/null 2>&1 || die "python3 is required"
command -v sha256sum >/dev/null 2>&1 || die "sha256sum is required"
WORKLOAD_BIN=$(resolve_exe "$WORKLOAD_BIN" "qualification workload")
KAFS_V7_BIN=$(resolve_exe "$KAFS_V7_BIN" "kafs-v7")
MKFS_BIN=$(resolve_exe "$MKFS_BIN" "mkfs.kafs")
FSCK_BIN=$(resolve_exe "$FSCK_BIN" "fsck.kafs")
KAFSDUMP_BIN=$(resolve_exe "$KAFSDUMP_BIN" "kafsdump")
GATE_BIN=$(resolve_exe "$GATE_BIN" "qualification gate")

if [[ -z "$REPORT_DIR" ]]; then
  REPORT_DIR="$REPORT_ROOT/$STAMP"
fi
mkdir -p "$REPORT_DIR"
REPORT_DIR=$(cd "$REPORT_DIR" && pwd)
if [[ -n "$(find "$REPORT_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  die "report directory must be empty: $REPORT_DIR"
fi

ARTIFACT_DIR="$REPORT_DIR/artifacts"
LOG_DIR="$ARTIFACT_DIR/logs"
IMAGE_META_DIR="$ARTIFACT_DIR/images"
WORK_ROOT="$REPORT_DIR/work"
mkdir -p "$LOG_DIR" "$IMAGE_META_DIR" "$WORK_ROOT"

WORKLOAD_STDOUT="$ARTIFACT_DIR/workload.stdout"
WORKLOAD_STDERR="$ARTIFACT_DIR/workload.stderr"
WORKLOAD_STATUS="$ARTIFACT_DIR/workload.status"
WORKLOAD_COMMAND="$ARTIFACT_DIR/workload.command"

printf 'TMPDIR=%q KAFS_TEST_KEEP_WORKDIR=1 KAFS_TEST_MOUNT_TIMEOUT_MS=%q KAFS_TEST_KAFS_V7=%q KAFS_TEST_MKFS=%q KAFS_TEST_FSCK=%q KAFS_TEST_KAFSDUMP=%q %q\n' \
  "$WORK_ROOT" "$MOUNT_TIMEOUT_MS" "$KAFS_V7_BIN" "$MKFS_BIN" "$FSCK_BIN" \
  "$KAFSDUMP_BIN" "$WORKLOAD_BIN" >"$WORKLOAD_COMMAND"
sha256sum "$ROOT_DIR/scripts/v7-controlled-write-qualification-dry-run.sh" \
  "$GATE_BIN" "$WORKLOAD_BIN" "$KAFS_V7_BIN" "$MKFS_BIN" "$FSCK_BIN" \
  "$KAFSDUMP_BIN" >"$ARTIFACT_DIR/executables.sha256"

required_cases=(
  format_and_seed
  inspection_mount
  direct_partial_overwrite
  direct_multi_block_overwrite
  direct_growth
  direct_truncate
  single_indirect_write_truncate
  double_indirect_write_truncate
  double_indirect_to_single_truncate
  double_indirect_recovery_matrix
  single_indirect_to_direct_truncate
  single_indirect_recovery_matrix
  create_inline_write
  regular_inline_promotion
  direct_to_inline_truncate_rejection
  regular_inline_promotion_recovery
  directory_inline_append
  directory_inline_growth
  inspection_remount
  open_truncate
  controlled_write_normal_matrix
  journal_publish_recovery
  metadata_apply_recovery
  checkpoint_copy_recovery
  journal_reclaim_recovery
  directory_transition_normal_matrix
  directory_transition_recovery_matrix
  direct_limit_rejection
  degraded_inspection
  unpaired_fail_closed
)

write_manifest() {
  local status="$1"
  local libfuse
  local dirty="false"
  libfuse=$(pkg-config --modversion fuse3 2>/dev/null || printf 'unknown')
  if [[ -n "$(git -C "$ROOT_DIR" status --porcelain)" ]]; then
    dirty="true"
  fi

  python3 - "$REPORT_DIR" "$status" "$STAMP" "$(git -C "$ROOT_DIR" rev-parse HEAD)" \
    "$(uname -a)" "$(uname -r)" "$libfuse" "$dirty" <<'PY'
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import sys


report_dir = Path(sys.argv[1])
status = sys.argv[2]
stamp = sys.argv[3]
git_commit = sys.argv[4]
uname = sys.argv[5]
kernel = sys.argv[6]
libfuse = sys.argv[7]
git_dirty = sys.argv[8] == "true"
artifact_root = report_dir / "artifacts"
stdout_path = artifact_root / "workload.stdout"
stderr_path = artifact_root / "workload.stderr"

markers = set()
if stdout_path.is_file():
    for line in stdout_path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = re.fullmatch(r"KAFS_V7_QUALIFICATION_CASE ([a-z0-9_]+) PASS", line)
        if match:
            markers.add(match.group(1))

required_cases = [
    "format_and_seed",
    "inspection_mount",
    "direct_partial_overwrite",
    "direct_multi_block_overwrite",
    "direct_growth",
    "direct_truncate",
    "single_indirect_write_truncate",
    "double_indirect_write_truncate",
    "double_indirect_to_single_truncate",
    "double_indirect_recovery_matrix",
    "single_indirect_to_direct_truncate",
    "single_indirect_recovery_matrix",
    "create_inline_write",
    "regular_inline_promotion",
    "direct_to_inline_truncate_rejection",
    "regular_inline_promotion_recovery",
    "directory_inline_append",
    "directory_inline_growth",
    "inspection_remount",
    "open_truncate",
    "controlled_write_normal_matrix",
    "journal_publish_recovery",
    "metadata_apply_recovery",
    "checkpoint_copy_recovery",
    "journal_reclaim_recovery",
    "directory_transition_normal_matrix",
    "directory_transition_recovery_matrix",
    "direct_limit_rejection",
    "degraded_inspection",
    "unpaired_fail_closed",
    "offline_fsck",
    "offline_dump",
]

evidence = {
    "format_and_seed": ["artifacts/images/source.sha256"],
    "inspection_mount": ["artifacts/logs/v7-inspection.log"],
    "direct_partial_overwrite": ["artifacts/logs/v7-controlled-write.log"],
    "direct_multi_block_overwrite": ["artifacts/logs/v7-controlled-write.log"],
    "direct_growth": ["artifacts/logs/v7-controlled-write.log"],
    "direct_truncate": ["artifacts/logs/v7-controlled-write.log"],
    "single_indirect_write_truncate": ["artifacts/logs/v7-controlled-write.log"],
    "double_indirect_write_truncate": ["artifacts/logs/v7-controlled-write.log"],
    "double_indirect_to_single_truncate": ["artifacts/logs/v7-controlled-open-trunc.log"],
    "double_indirect_recovery_matrix": [
        "artifacts/logs/v7-double-journal_publish-recovery.log",
        "artifacts/logs/v7-double-metadata_apply-recovery.log",
        "artifacts/logs/v7-double-checkpoint_copy-recovery.log",
    ],
    "single_indirect_to_direct_truncate": ["artifacts/logs/v7-controlled-open-trunc.log"],
    "single_indirect_recovery_matrix": [
        "artifacts/logs/v7-single-journal_publish-recovery.log",
        "artifacts/logs/v7-single-metadata_apply-recovery.log",
        "artifacts/logs/v7-single-checkpoint_copy-recovery.log",
    ],
    "create_inline_write": ["artifacts/logs/v7-controlled-write.log"],
    "regular_inline_promotion": ["artifacts/logs/v7-controlled-write.log"],
    "direct_to_inline_truncate_rejection": ["artifacts/logs/v7-controlled-write.log"],
    "regular_inline_promotion_recovery": [
        "artifacts/logs/v7-inline-promotion-journal_publish-recovery.log",
        "artifacts/logs/v7-inline-promotion-metadata_apply-recovery.log",
        "artifacts/logs/v7-inline-promotion-checkpoint_copy-recovery.log",
    ],
    "directory_inline_append": ["artifacts/logs/v7-controlled-write.log"],
    "directory_inline_growth": ["artifacts/logs/v7-controlled-write.log"],
    "inspection_remount": ["artifacts/logs/v7-controlled-remount.log"],
    "open_truncate": ["artifacts/logs/v7-controlled-open-trunc.log"],
    "controlled_write_normal_matrix": ["artifacts/workload.stdout"],
    "journal_publish_recovery": ["artifacts/logs/v7-journal_publish-recovery.log"],
    "metadata_apply_recovery": ["artifacts/logs/v7-metadata_apply-recovery.log"],
    "checkpoint_copy_recovery": ["artifacts/logs/v7-checkpoint_copy-recovery.log"],
    "journal_reclaim_recovery": ["artifacts/logs/v7-controlled-reclaim-recovery.log"],
    "directory_transition_normal_matrix": ["artifacts/workload.stdout"],
    "directory_transition_recovery_matrix": [
        "artifacts/logs/v7-create-direct-growth-limit-minus-one-checkpoint_copy-recovery.log"
    ],
    "direct_limit_rejection": ["artifacts/logs/v7-create-limit-rejection.log"],
    "degraded_inspection": ["artifacts/logs/v7-degraded.log"],
    "unpaired_fail_closed": ["artifacts/workload.stdout"],
    "offline_fsck": ["artifacts/fsck-after.status", "artifacts/fsck-after.stdout"],
    "offline_dump": ["artifacts/kafsdump-after.status", "artifacts/kafsdump-after.json"],
}

artifacts = []
for path in sorted(artifact_root.rglob("*")):
    if not path.is_file():
        continue
    data = path.read_bytes()
    artifacts.append(
        {
            "path": path.relative_to(report_dir).as_posix(),
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    )

fallback = "artifacts/workload.stderr" if stderr_path.is_file() else "artifacts/workload.status"
results = []
for case_id in required_cases:
    if status == "PASS":
        case_status = "PASS" if case_id in markers or case_id.startswith("offline_") else "FAIL"
        case_evidence = evidence[case_id]
    else:
        case_status = status
        case_evidence = [fallback]
    results.append({"id": case_id, "status": case_status, "evidence": case_evidence})

manifest = {
    "schema": "KAFS.V7ControlledWriteQualification.v1",
    "scope": "non-destructive-file-image",
    "status": status,
    "claims": {
        "rc_eligible": False,
        "real_media_qualified": False,
        "controller_independent_wear": False,
    },
    "sample": {
        "id": f"v7-file-image-{stamp}",
        "media_class": "file-image",
        "backing": "temporary-sparse-image",
        "controller": "not-applicable",
        "power_interruption": "process-fault-injection",
    },
    "environment": {
        "generated_at_utc": stamp,
        "git_commit": git_commit,
        "git_dirty": git_dirty,
        "uname": uname,
        "kernel": kernel,
        "libfuse": libfuse,
    },
    "workload_engine": "tests/v7_inspection_mount_smoketest",
    "results": results,
    "artifacts": artifacts,
    "limitations": [
        "not-real-media-qualification",
        "no-physical-power-cut",
        "no-nand-ftl-independence-claim",
        "not-rc-approval",
    ],
}
(report_dir / "qualification.json").write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY
}

if [[ ! -r /dev/fuse || ! -w /dev/fuse ]]; then
  echo "skip: /dev/fuse is not readable and writable on this host" >"$WORKLOAD_STDERR"
  printf '77\n' >"$WORKLOAD_STATUS"
  : >"$WORKLOAD_STDOUT"
  write_manifest SKIP
  echo "v7 controlled-write qualification dry-run SKIP"
  echo "report: $REPORT_DIR"
  exit 77
fi

set +e
TMPDIR="$WORK_ROOT" KAFS_TEST_KEEP_WORKDIR=1 \
  KAFS_TEST_MOUNT_TIMEOUT_MS="$MOUNT_TIMEOUT_MS" \
  KAFS_TEST_KAFS_V7="$KAFS_V7_BIN" KAFS_TEST_MKFS="$MKFS_BIN" \
  KAFS_TEST_FSCK="$FSCK_BIN" KAFS_TEST_KAFSDUMP="$KAFSDUMP_BIN" \
  "$WORKLOAD_BIN" >"$WORKLOAD_STDOUT" 2>"$WORKLOAD_STDERR"
workload_rc=$?
set -e
printf '%s\n' "$workload_rc" >"$WORKLOAD_STATUS"

TEST_WORKDIR=$(find "$WORK_ROOT" -mindepth 1 -maxdepth 1 -type d \
  -name 'kafs-v7-inspection-mount-*' -print -quit)
if [[ -n "$TEST_WORKDIR" ]]; then
  while IFS= read -r log_path; do
    cp -- "$log_path" "$LOG_DIR/$(basename "$log_path")"
  done < <(find "$TEST_WORKDIR" -maxdepth 1 -type f -name '*.log' -print | sort)
fi

overall_rc=$workload_rc
if [[ "$workload_rc" -eq 0 && -n "$TEST_WORKDIR" ]]; then
  (
    cd "$TEST_WORKDIR"
    sha256sum v7-inspection.img >"$IMAGE_META_DIR/source.sha256"
    sha256sum v7-controlled.img >"$IMAGE_META_DIR/controlled.sha256"
  ) || overall_rc=1

  printf '%q --check %q\n' "$FSCK_BIN" "$TEST_WORKDIR/v7-controlled.img" \
    >"$ARTIFACT_DIR/fsck-after.command"
  set +e
  "$FSCK_BIN" --check "$TEST_WORKDIR/v7-controlled.img" \
    >"$ARTIFACT_DIR/fsck-after.stdout" 2>"$ARTIFACT_DIR/fsck-after.stderr"
  fsck_rc=$?
  set -e
  printf '%s\n' "$fsck_rc" >"$ARTIFACT_DIR/fsck-after.status"
  [[ "$fsck_rc" -eq 0 ]] || overall_rc=1

  printf '%q --json %q\n' "$KAFSDUMP_BIN" "$TEST_WORKDIR/v7-controlled.img" \
    >"$ARTIFACT_DIR/kafsdump-after.command"
  set +e
  "$KAFSDUMP_BIN" --json "$TEST_WORKDIR/v7-controlled.img" \
    >"$ARTIFACT_DIR/kafsdump-after.json" 2>"$ARTIFACT_DIR/kafsdump-after.stderr"
  dump_rc=$?
  set -e
  printf '%s\n' "$dump_rc" >"$ARTIFACT_DIR/kafsdump-after.status"
  [[ "$dump_rc" -eq 0 ]] || overall_rc=1
else
  overall_rc=1
fi

for case_id in "${required_cases[@]}"; do
  if ! grep -Fqx "KAFS_V7_QUALIFICATION_CASE $case_id PASS" "$WORKLOAD_STDOUT"; then
    echo "missing qualification marker: $case_id" >>"$WORKLOAD_STDERR"
    overall_rc=1
  fi
done

required_logs=(
  v7-inspection.log
  v7-controlled-write.log
  v7-controlled-remount.log
  v7-controlled-open-trunc.log
  v7-inline-promotion-journal_publish-recovery.log
  v7-inline-promotion-metadata_apply-recovery.log
  v7-inline-promotion-checkpoint_copy-recovery.log
  v7-single-journal_publish-recovery.log
  v7-single-metadata_apply-recovery.log
  v7-single-checkpoint_copy-recovery.log
  v7-journal_publish-recovery.log
  v7-metadata_apply-recovery.log
  v7-checkpoint_copy-recovery.log
  v7-controlled-reclaim-recovery.log
  v7-create-direct-growth-limit-minus-one-checkpoint_copy-recovery.log
  v7-create-limit-rejection.log
  v7-degraded.log
)
for log_name in "${required_logs[@]}"; do
  if [[ ! -f "$LOG_DIR/$log_name" ]]; then
    echo "missing raw log: $log_name" >>"$WORKLOAD_STDERR"
    overall_rc=1
  fi
done

if [[ "$overall_rc" -eq 0 ]]; then
  write_manifest PASS
  if ! "$GATE_BIN" --report-dir "$REPORT_DIR" --validate-only; then
    overall_rc=1
  fi
fi

if [[ "$overall_rc" -ne 0 ]]; then
  write_manifest FAIL
  "$GATE_BIN" --report-dir "$REPORT_DIR" --validate-only >/dev/null 2>&1 || true
  echo "v7 controlled-write qualification dry-run FAIL" >&2
  echo "report: $REPORT_DIR" >&2
  exit 1
fi

echo "v7 controlled-write qualification dry-run PASS"
echo "report: $REPORT_DIR"
echo "NOTE: this is non-destructive file-image evidence, not RC approval"
