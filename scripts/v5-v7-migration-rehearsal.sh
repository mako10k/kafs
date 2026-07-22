#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
REPORT_ROOT="$ROOT_DIR/report/v5-v7-migration-rehearsal"
REPORT_DIR=""
KEEP_WORKDIR=0
MOUNT_TIMEOUT_MS=${KAFS_V5_V7_REHEARSAL_MOUNT_TIMEOUT_MS:-15000}

WORKLOAD_BIN=${KAFS_TEST_V5_V7_IMPORT_WORKLOAD:-$ROOT_DIR/tests/v5_v7_import_smoketest}
KAFS_BIN=${KAFS_TEST_KAFS:-$ROOT_DIR/src/kafs}
KAFS_V7_BIN=${KAFS_TEST_KAFS_V7:-$ROOT_DIR/src/kafs-v7}
MKFS_BIN=${KAFS_TEST_MKFS:-$ROOT_DIR/src/mkfs.kafs}
FSCK_BIN=${KAFS_TEST_FSCK:-$ROOT_DIR/src/fsck.kafs}
KAFSRESIZE_BIN=${KAFS_TEST_KAFSRESIZE:-$ROOT_DIR/src/kafsresize}
KAFSDUMP_BIN=${KAFS_TEST_KAFSDUMP:-$ROOT_DIR/src/kafsdump}
INVENTORY_BIN=${KAFS_TEST_V5_V7_MOUNTED_INVENTORY:-$ROOT_DIR/scripts/v5-v7-mounted-inventory.py}
GATE_BIN=${KAFS_TEST_V5_V7_MIGRATION_EVIDENCE_GATE:-$ROOT_DIR/scripts/v5-v7-migration-evidence-gate.sh}

WORK_ROOT=""

usage() {
  cat <<'EOF'
Usage:
  scripts/v5-v7-migration-rehearsal.sh [options]

Run the v5-to-v7 normal, interrupted/resumed, rollback, and idempotence matrix
against disposable file images. The runner emits four T59 lifecycle bundles
and validates each with the migration evidence gate. It never accepts a
caller-supplied image, device, mountpoint, or production path.

Options:
  --report-root DIR    Parent directory for timestamped reports
  --report-dir DIR     Exact new or empty report directory
  --timeout-ms N       FUSE mount timeout (default: 15000)
  --keep-workdir       Keep all disposable workload images
  -h, --help           Show this help
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 2
}

resolve_exe() {
  local value=$1
  local label=$2
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
      REPORT_ROOT=$2
      shift 2
      ;;
    --report-dir)
      [[ $# -ge 2 ]] || die "missing value for --report-dir"
      REPORT_DIR=$2
      shift 2
      ;;
    --timeout-ms)
      [[ $# -ge 2 ]] || die "missing value for --timeout-ms"
      MOUNT_TIMEOUT_MS=$2
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

WORKLOAD_BIN=$(resolve_exe "$WORKLOAD_BIN" "migration workload")
KAFS_BIN=$(resolve_exe "$KAFS_BIN" "kafs")
KAFS_V7_BIN=$(resolve_exe "$KAFS_V7_BIN" "kafs-v7")
MKFS_BIN=$(resolve_exe "$MKFS_BIN" "mkfs.kafs")
FSCK_BIN=$(resolve_exe "$FSCK_BIN" "fsck.kafs")
KAFSRESIZE_BIN=$(resolve_exe "$KAFSRESIZE_BIN" "kafsresize")
KAFSDUMP_BIN=$(resolve_exe "$KAFSDUMP_BIN" "kafsdump")
INVENTORY_BIN=$(resolve_exe "$INVENTORY_BIN" "mounted inventory helper")
GATE_BIN=$(resolve_exe "$GATE_BIN" "migration evidence gate")

if [[ -z "$REPORT_DIR" ]]; then
  REPORT_DIR="$REPORT_ROOT/$STAMP"
fi
mkdir -p "$REPORT_DIR"
REPORT_DIR=$(cd "$REPORT_DIR" && pwd)
if [[ -n "$(find "$REPORT_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  die "report directory must be empty: $REPORT_DIR"
fi

ARTIFACT_DIR="$REPORT_DIR/artifacts"
BUNDLE_DIR="$REPORT_DIR/bundles"
WORK_ROOT="$REPORT_DIR/work"
mkdir -p "$ARTIFACT_DIR/images" "$ARTIFACT_DIR/logs" "$BUNDLE_DIR" "$WORK_ROOT"

if [[ ! -r /dev/fuse || ! -w /dev/fuse ]]; then
  printf '77\n' >"$ARTIFACT_DIR/workload.status"
  printf 'skip: /dev/fuse is unavailable\n' >"$ARTIFACT_DIR/workload.stderr"
  : >"$ARTIFACT_DIR/workload.stdout"
  echo "v5-to-v7 migration rehearsal SKIP"
  echo "report: $REPORT_DIR"
  exit 77
fi

printf 'TMPDIR=%q KAFS_TEST_KEEP_WORKDIR=1 KAFS_TEST_MOUNT_TIMEOUT_MS=%q %q\n' \
  "$WORK_ROOT" "$MOUNT_TIMEOUT_MS" "$WORKLOAD_BIN" >"$ARTIFACT_DIR/workload.command"
sha256sum "$ROOT_DIR/scripts/v5-v7-migration-rehearsal.sh" "$INVENTORY_BIN" "$GATE_BIN" \
  "$WORKLOAD_BIN" "$KAFS_BIN" "$KAFS_V7_BIN" "$MKFS_BIN" "$FSCK_BIN" \
  "$KAFSRESIZE_BIN" "$KAFSDUMP_BIN" >"$ARTIFACT_DIR/executables.sha256"

set +e
TMPDIR="$WORK_ROOT" KAFS_TEST_KEEP_WORKDIR=1 \
  KAFS_TEST_MOUNT_TIMEOUT_MS="$MOUNT_TIMEOUT_MS" \
  KAFS_TEST_KAFS="$KAFS_BIN" KAFS_TEST_KAFS_V7="$KAFS_V7_BIN" \
  KAFS_TEST_MKFS="$MKFS_BIN" KAFS_TEST_FSCK="$FSCK_BIN" \
  KAFS_TEST_KAFSRESIZE="$KAFSRESIZE_BIN" KAFS_TEST_KAFSDUMP="$KAFSDUMP_BIN" \
  KAFS_TEST_V5_V7_MOUNTED_INVENTORY="$INVENTORY_BIN" \
  "$WORKLOAD_BIN" >"$ARTIFACT_DIR/workload.stdout" 2>"$ARTIFACT_DIR/workload.stderr"
workload_rc=$?
set -e
printf '%s\n' "$workload_rc" >"$ARTIFACT_DIR/workload.status"
if [[ "$workload_rc" -eq 77 ]]; then
  echo "v5-to-v7 migration rehearsal SKIP"
  echo "report: $REPORT_DIR"
  exit 77
fi
[[ "$workload_rc" -eq 0 ]] || die "migration workload failed; inspect $ARTIFACT_DIR"

TEST_WORKDIR=$(find "$WORK_ROOT" -mindepth 1 -maxdepth 1 -type d \
  -name 'kafs-v5_v7_import_smoketest-*' -print -quit)
[[ -n "$TEST_WORKDIR" ]] || die "migration workload did not retain its workdir"

required_markers=(
  normal
  idempotence
  pending_ref_rejection
  bitmap_invalid_rejection
  resume_required
  resumed_accept
  rollback
  source_immutability
)
for case_id in "${required_markers[@]}"; do
  grep -Fqx "KAFS_V5_V7_MIGRATION_CASE $case_id PASS" \
    "$ARTIFACT_DIR/workload.stderr" || die "missing workload marker: $case_id"
done

required_files=(
  source-v5.img
  destination-v7.img
  resume-v7.img
  resume-attempt1-preserved.img
  rollback-preserved-failed.img
  source-before.sha256
  source-after.sha256
  source-inventory.json
  destination-inventory.json
  resume-inventory.json
)
for name in "${required_files[@]}"; do
  [[ -f "$TEST_WORKDIR/$name" && ! -L "$TEST_WORKDIR/$name" ]] ||
    die "required workload artifact is missing: $name"
done

source_before=$(<"$TEST_WORKDIR/source-before.sha256")
source_after=$(<"$TEST_WORKDIR/source-after.sha256")
[[ "$source_before" =~ ^[0-9a-f]{64}$ && "$source_before" == "$source_after" ]] ||
  die "source digest changed during migration workload"

cp -- "$TEST_WORKDIR/source-inventory.json" "$ARTIFACT_DIR/source-inventory.json"
cp -- "$TEST_WORKDIR/destination-inventory.json" "$ARTIFACT_DIR/destination-inventory.json"
cp -- "$TEST_WORKDIR/resume-inventory.json" "$ARTIFACT_DIR/resume-inventory.json"
cp --sparse=always -- "$TEST_WORKDIR/source-v5.img" \
  "$ARTIFACT_DIR/images/source-v5.img"
cp --sparse=always -- "$TEST_WORKDIR/destination-v7.img" \
  "$ARTIFACT_DIR/images/destination-v7.img"
cp --sparse=always -- "$TEST_WORKDIR/resume-v7.img" \
  "$ARTIFACT_DIR/images/resume-v7.img"
cp --sparse=always -- "$TEST_WORKDIR/resume-attempt1-preserved.img" \
  "$ARTIFACT_DIR/images/resume-attempt1-preserved.img"
cp --sparse=always -- "$TEST_WORKDIR/rollback-preserved-failed.img" \
  "$ARTIFACT_DIR/images/rollback-preserved-failed.img"
printf '%s\n' "$source_before" >"$ARTIFACT_DIR/images/source-before.sha256"
printf '%s\n' "$source_after" >"$ARTIFACT_DIR/images/source-after.sha256"

"$FSCK_BIN" --full-check "$TEST_WORKDIR/source-v5.img" \
  >"$ARTIFACT_DIR/source-fsck.stdout" 2>"$ARTIFACT_DIR/source-fsck.stderr"
for prefix in destination resume; do
  image="$TEST_WORKDIR/${prefix}-v7.img"
  "$FSCK_BIN" "$image" >"$ARTIFACT_DIR/${prefix}-fsck.stdout" \
    2>"$ARTIFACT_DIR/${prefix}-fsck.stderr"
  "$KAFSDUMP_BIN" --json "$image" >"$ARTIFACT_DIR/${prefix}-dump.json" \
    2>"$ARTIFACT_DIR/${prefix}-dump.stderr"
done

normal_sha=$(sha256sum "$TEST_WORKDIR/destination-v7.img" | awk '{print $1}')
resume_sha=$(sha256sum "$TEST_WORKDIR/resume-v7.img" | awk '{print $1}')
partial_sha=$(sha256sum "$ARTIFACT_DIR/images/resume-attempt1-preserved.img" | awk '{print $1}')
rollback_sha=$(sha256sum "$ARTIFACT_DIR/images/rollback-preserved-failed.img" | awk '{print $1}')
source_size=$(stat -c '%s' "$TEST_WORKDIR/source-v5.img")

python3 - "$ARTIFACT_DIR" "$BUNDLE_DIR" "$source_before" "$normal_sha" "$resume_sha" \
  "$partial_sha" "$rollback_sha" "$source_size" <<'PY'
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys


artifacts = Path(sys.argv[1])
bundles = Path(sys.argv[2])
source_image_sha, normal_sha, resume_sha, partial_sha, rollback_sha = sys.argv[3:8]
image_size = int(sys.argv[8])


def load(name):
    return json.loads((artifacts / name).read_text(encoding="utf-8"))


source_inventory = load("source-inventory.json")
normal_inventory = load("destination-inventory.json")
resume_inventory = load("resume-inventory.json")
if source_inventory != normal_inventory or source_inventory != resume_inventory:
    raise SystemExit("mounted source/destination semantic inventories differ")

dump = load("destination-dump.json")
block_size = dump["superblock"]["block_size"]
inode_count = dump["superblock"]["inode_count"]
group_count = dump["layout_descriptor"]["group_count"]
claims = {
    "production_cutover_authorized": False,
    "rc_eligible": False,
    "real_media_qualified": False,
    "physical_media_qualified": False,
    "release_candidate_qualified": False,
}
transitions = [
    "PLANNED->SOURCE_CAPTURED",
    "SOURCE_CAPTURED->DESTINATION_CREATED",
    "DESTINATION_CREATED->COPYING",
    "COPYING->VERIFYING",
    "VERIFYING->ACCEPTED",
    "COPYING->RESUME_REQUIRED",
    "VERIFYING->RESUME_REQUIRED",
    "RESUME_REQUIRED->COPYING",
    "COPYING->ROLLED_BACK",
    "VERIFYING->ROLLED_BACK",
    "RESUME_REQUIRED->ROLLED_BACK",
]
base = datetime.now(timezone.utc) - timedelta(minutes=2)


def stamp(offset):
    return (base + timedelta(seconds=offset)).isoformat().replace("+00:00", "Z")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


source_objects = source_inventory["objects"]
source_entries = source_inventory["entries"]
complete_ids = {item["object_id"] for item in source_objects[:2]}
partial_objects = [item for item in source_objects if item["object_id"] in complete_ids]
partial_entries = [item for item in source_entries if item["object_id"] in complete_ids]


def build_bundle(name, destination_id, decision_name, image_sha, attempt, history,
                 complete, destination_state, failure_reason=""):
    root = bundles / name
    root.mkdir()
    migration_id = f"T59C-{name.upper().replace('-', '_')}"
    plan_source = {
        "image_id": "source-v5.img",
        "format_version": 5,
        "image_size_bytes": image_size,
        "image_sha256": source_image_sha,
        "fsck_full_check_passed": True,
    }
    plan_destination = {
        "image_id": destination_id,
        "format_version": 7,
        "image_size_bytes": image_size,
        "inode_count": inode_count,
        "group_count": group_count,
        "block_size": block_size,
    }
    plan = {
        "schema": "KAFS.V5V7MigrationPlan.v1",
        "migration_id": migration_id,
        "created_at_utc": stamp(0),
        "source": plan_source,
        "destination": plan_destination,
        "policies": {
            "source_write_frozen": True,
            "preserve_types": ["directory", "regular", "symlink"],
            "preserve_metadata": [
                "permissions", "uid", "gid", "atime_ns", "mtime_ns", "hardlinks"
            ],
            "special_file_policy": "REJECT_BEFORE_DESTINATION_WRITE",
            "sparse_file_policy": "REJECT_BEFORE_DESTINATION_WRITE",
            "partial_destination_policy": "NEVER_ACCEPT",
            "rollback_policy": "PRESERVE_FAILED_DESTINATION_AND_SELECT_UNCHANGED_SOURCE",
            "idempotence_policy": "SAME_BINDINGS_SAME_SEMANTIC_INVENTORY",
            "phase_transitions": transitions,
        },
        "claims": claims,
    }
    write(root / "plan.json", plan)
    plan_sha = digest(root / "plan.json")
    source = {
        "schema": "KAFS.V5MigrationSourceInventory.v1",
        "migration_id": migration_id,
        "captured_at_utc": stamp(1),
        "plan_sha256": plan_sha,
        "image": plan_source,
        "objects": source_objects,
        "entries": source_entries,
        "claims": claims,
    }
    write(root / "source.json", source)
    source_sha = digest(root / "source.json")
    ledger_objects = []
    for item in source_objects:
        is_complete = complete == "all" or (
            complete is True and item["object_id"] in complete_ids
        )
        ledger_objects.append({
            "object_id": item["object_id"],
            "status": "COMPLETE" if is_complete else "PENDING",
            "bytes_total": item["size_bytes"],
            "bytes_copied": item["size_bytes"] if is_complete else 0,
            "payload_sha256": item["payload_sha256"],
        })
    ledger_state = {
        "ACCEPT": "COMPLETE",
        "RESUME_REQUIRED": "INTERRUPTED",
        "ROLLBACK": "ROLLED_BACK",
    }[decision_name]
    ledger = {
        "schema": "KAFS.V5V7MigrationCopyLedger.v1",
        "migration_id": migration_id,
        "plan_sha256": plan_sha,
        "source_sha256": source_sha,
        "attempt": attempt,
        "source_image_sha256_before": source_image_sha,
        "source_image_sha256_after": source_image_sha,
        "destination_image_id": destination_id,
        "phase_history": [
            {"phase": phase, "at_utc": stamp(2 + index), "attempt": phase_attempt}
            for index, (phase, phase_attempt) in enumerate(history)
        ],
        "objects": ledger_objects,
        "state": ledger_state,
        "updated_at_utc": stamp(2 + len(history)),
        "claims": claims,
    }
    write(root / "ledger.json", ledger)
    ledger_sha = digest(root / "ledger.json")
    accepted = decision_name == "ACCEPT"
    inventory_objects = source_objects if accepted else partial_objects
    inventory_entries = source_entries if accepted else partial_entries
    destination = {
        "schema": "KAFS.V7MigrationDestinationInventory.v1",
        "migration_id": migration_id,
        "captured_at_utc": stamp(3 + len(history)),
        "plan_sha256": plan_sha,
        "source_sha256": source_sha,
        "ledger_sha256": ledger_sha,
        "image": {**plan_destination, "image_sha256": image_sha},
        "state": destination_state,
        "objects": inventory_objects,
        "entries": inventory_entries,
        "fsck_full_check_passed": accepted,
        "kafsdump_completed": accepted,
        "admission_ready": accepted,
        "claims": claims,
    }
    write(root / "destination.json", destination)
    destination_sha = digest(root / "destination.json")
    noncomplete = [item for item in ledger_objects if item["status"] != "COMPLETE"]
    decision = {
        "schema": "KAFS.V5V7MigrationDecision.v1",
        "migration_id": migration_id,
        "operator_id": "automated-disposable-rehearsal",
        "decided_at_utc": stamp(4 + len(history)),
        "decision": decision_name,
        "bindings": {
            "plan_sha256": plan_sha,
            "source_sha256": source_sha,
            "ledger_sha256": ledger_sha,
            "destination_sha256": destination_sha,
        },
        "source_immutable": True,
        "destination_complete": accepted,
        "resume_from_object_id": noncomplete[0]["object_id"]
        if decision_name == "RESUME_REQUIRED" else None,
        "rollback_source_selected": decision_name == "ROLLBACK",
        "failure_reason": failure_reason,
        "claims": claims,
        "confirmation": f"MIGRATION T59A {migration_id} {decision_name}",
    }
    write(root / "decision.json", decision)
    names = ("decision.json", "destination.json", "ledger.json", "plan.json", "source.json")
    (root / "artifacts.sha256").write_text(
        "\n".join(f"{digest(root / item)}  ./{item}" for item in names) + "\n",
        encoding="utf-8",
    )


normal_history = [
    ("PLANNED", 1), ("SOURCE_CAPTURED", 1), ("DESTINATION_CREATED", 1),
    ("COPYING", 1), ("VERIFYING", 1), ("ACCEPTED", 1),
]
resume_required_history = [
    ("PLANNED", 1), ("SOURCE_CAPTURED", 1), ("DESTINATION_CREATED", 1),
    ("COPYING", 1), ("RESUME_REQUIRED", 1),
]
resumed_history = resume_required_history + [
    ("COPYING", 2), ("VERIFYING", 2), ("ACCEPTED", 2),
]
rollback_history = resume_required_history + [("ROLLED_BACK", 1)]

build_bundle("normal-accept", "destination-v7.img", "ACCEPT", normal_sha, 1,
             normal_history, "all", "COMPLETE")
build_bundle("resume-required", "resume-v7.img", "RESUME_REQUIRED", partial_sha, 1,
             resume_required_history, True, "PARTIAL", "injected interruption after two objects")
build_bundle("resumed-accept", "resume-v7.img", "ACCEPT", resume_sha, 2,
             resumed_history, "all", "COMPLETE")
build_bundle("rollback", "rollback-v7.img", "ROLLBACK", rollback_sha, 1,
             rollback_history, True, "PRESERVED_FAILED",
             "operator rollback preserved failed destination and selected unchanged source")
PY

declare -A bundle_decisions=(
  [normal-accept]=ACCEPT
  [resume-required]=RESUME_REQUIRED
  [resumed-accept]=ACCEPT
  [rollback]=ROLLBACK
)
for bundle in normal-accept resume-required resumed-accept rollback; do
  "$GATE_BIN" --evidence-dir "$BUNDLE_DIR/$bundle" \
    --require-decision "${bundle_decisions[$bundle]}" --validate-only \
    >"$ARTIFACT_DIR/${bundle}.gate.stdout" 2>"$ARTIFACT_DIR/${bundle}.gate.stderr"
done

python3 - "$REPORT_DIR" "$(git -C "$ROOT_DIR" rev-parse HEAD)" <<'PY'
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys


root = Path(sys.argv[1])
artifacts = []


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


for path in sorted((root / "artifacts").rglob("*")):
    if path.is_file():
        artifacts.append({
            "path": path.relative_to(root).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": digest(path),
        })
manifest = {
    "schema": "KAFS.V5V7MigrationRehearsal.v1",
    "scope": "disposable-file-images",
    "status": "PASS",
    "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "git_commit": sys.argv[2],
    "resume_semantics": "preserve-attempt-partial-and-replay-from-frozen-source",
    "fault_after_objects": 2,
    "cases": [
        {
            "id": "normal",
            "status": "PASS",
            "bundle": "bundles/normal-accept",
            "image": "artifacts/images/destination-v7.img",
        },
        {
            "id": "resume_required",
            "status": "PASS",
            "bundle": "bundles/resume-required",
            "image": "artifacts/images/resume-attempt1-preserved.img",
        },
        {
            "id": "resumed_accept",
            "status": "PASS",
            "bundle": "bundles/resumed-accept",
            "image": "artifacts/images/resume-v7.img",
            "strategy": "full-replay-from-frozen-source",
        },
        {
            "id": "rollback",
            "status": "PASS",
            "bundle": "bundles/rollback",
            "image": "artifacts/images/rollback-preserved-failed.img",
        },
        {"id": "idempotence", "status": "PASS", "evidence": "artifacts/workload.stderr"},
        {"id": "pending_ref_rejection", "status": "PASS", "evidence": "artifacts/workload.stderr"},
        {"id": "bitmap_invalid_rejection", "status": "PASS", "evidence": "artifacts/workload.stderr"},
    ],
    "claims": {
        "production_cutover_authorized": False,
        "real_media_qualified": False,
        "physical_media_qualified": False,
        "release_candidate_qualified": False,
    },
    "artifacts": artifacts,
    "limitations": [
        "not-in-place-partial-continuation",
        "not-production-migration",
        "not-real-media-qualification",
        "no-vhdx-or-wsl-interruption",
    ],
}
(root / "rehearsal.json").write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY

echo "KAFS_V5_V7_MIGRATION_REHEARSAL PASS"
echo "report: $REPORT_DIR"
