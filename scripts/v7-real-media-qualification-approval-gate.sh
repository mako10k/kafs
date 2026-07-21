#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/v7-real-media-qualification-approval-gate.sh \
    --matrix FILE --validate-only
  scripts/v7-real-media-qualification-approval-gate.sh \
    --matrix FILE --approval FILE --validate-only --require-approved

Validate the format-v7 real-media matrix and, when requested, an operator
approval bound to the exact matrix SHA-256. This command never opens a device,
mounts a filesystem, formats media, or controls power.
EOF
}

MATRIX=""
APPROVAL=""
VALIDATE_ONLY=0
REQUIRE_APPROVED=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --matrix)
      [[ $# -ge 2 ]] || { usage >&2; exit 2; }
      MATRIX="$2"
      shift 2
      ;;
    --approval)
      [[ $# -ge 2 ]] || { usage >&2; exit 2; }
      APPROVAL="$2"
      shift 2
      ;;
    --validate-only)
      VALIDATE_ONLY=1
      shift
      ;;
    --require-approved)
      REQUIRE_APPROVED=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      echo "unknown option: $1" >&2
      exit 2
      ;;
  esac
done

[[ "$VALIDATE_ONLY" -eq 1 ]] || { usage >&2; exit 2; }
[[ -n "$MATRIX" && -f "$MATRIX" ]] || {
  echo "approval gate: matrix not found: ${MATRIX:-<missing>}" >&2
  exit 2
}
if [[ "$REQUIRE_APPROVED" -eq 1 ]]; then
  [[ -n "$APPROVAL" && -f "$APPROVAL" ]] || {
    echo "approval gate: --approval FILE is required with --require-approved" >&2
    exit 2
  }
elif [[ -n "$APPROVAL" ]]; then
  echo "approval gate: --approval requires --require-approved" >&2
  exit 2
fi
command -v python3 >/dev/null 2>&1 || {
  echo "approval gate: python3 is required" >&2
  exit 2
}

python3 - "$MATRIX" "$APPROVAL" "$REQUIRE_APPROVED" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys


matrix_path = Path(sys.argv[1])
approval_path = Path(sys.argv[2]) if sys.argv[2] else None
require_approved = sys.argv[3] == "1"
errors: list[str] = []


def fail(message: str) -> None:
    errors.append(message)


def load_json(path: Path, label: str) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read {label}: {exc}")
        return {}
    if not isinstance(value, dict):
        fail(f"{label} must contain a JSON object")
        return {}
    return value


def require_string(obj: dict, key: str, prefix: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value.strip():
        fail(f"{prefix}.{key} must be a non-empty string")
        return ""
    return value


def require_true(obj: dict, key: str, prefix: str) -> None:
    if obj.get(key) is not True:
        fail(f"{prefix}.{key} must be true")


def require_false(obj: dict, key: str, prefix: str) -> None:
    if obj.get(key) is not False:
        fail(f"{prefix}.{key} must be false")


def require_string_list(obj: dict, key: str, prefix: str) -> list[str]:
    value = obj.get(key)
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        fail(f"{prefix}.{key} must be an array of non-empty strings")
        return []
    if len(value) != len(set(value)):
        fail(f"{prefix}.{key} must not contain duplicates")
    return value


def require_fields(obj: object, fields: tuple[str, ...], prefix: str) -> dict:
    if not isinstance(obj, dict):
        fail(f"{prefix} must be an object")
        return {}
    for field in fields:
        require_string(obj, field, prefix)
    return obj


matrix_bytes = matrix_path.read_bytes()
matrix_digest = hashlib.sha256(matrix_bytes).hexdigest()
matrix = load_json(matrix_path, "matrix")

if matrix.get("schema") != "KAFS.V7RealMediaQualificationMatrix.v1":
    fail("matrix.schema must be KAFS.V7RealMediaQualificationMatrix.v1")
matrix_id = require_string(matrix, "matrix_id", "matrix")
if matrix.get("scope") != "format-v7-bounded-direct-controlled-write":
    fail("matrix.scope must be format-v7-bounded-direct-controlled-write")
state = matrix.get("state")
if state not in ("DRAFT", "READY_FOR_APPROVAL"):
    fail("matrix.state must be DRAFT or READY_FOR_APPROVAL")

claims = matrix.get("claims")
if not isinstance(claims, dict):
    fail("matrix.claims must be an object")
    claims = {}
for key in ("rc_eligible", "real_media_qualified", "controller_independent_wear"):
    require_false(claims, key, "matrix.claims")

execution = matrix.get("execution_policy")
if not isinstance(execution, dict):
    fail("matrix.execution_policy must be an object")
    execution = {}
require_true(execution, "approval_record_required", "matrix.execution_policy")
require_false(execution, "raw_device_execution_without_approval", "matrix.execution_policy")
require_false(execution, "physical_power_cut_without_approval", "matrix.execution_policy")

impact = matrix.get("destructive_impact")
if not isinstance(impact, dict):
    fail("matrix.destructive_impact must be an object")
    impact = {}
for key in (
    "whole_device_overwrite",
    "all_existing_data_lost",
    "disposable_media_only",
    "host_or_system_disk_power_cut_forbidden",
):
    require_true(impact, key, "matrix.destructive_impact")

required_artifacts = {
    "artifact-sha256",
    "command-log",
    "device-identity-after",
    "device-identity-before",
    "environment",
    "fsck-after",
    "fsck-before",
    "kafsdump-after",
    "kafsdump-before",
    "mount-log",
    "power-cut-log",
    "workload-log",
}
retention = matrix.get("retention")
if not isinstance(retention, dict):
    fail("matrix.retention must be an object")
    retention = {}
minimum_days = retention.get("minimum_days")
if not isinstance(minimum_days, int) or isinstance(minimum_days, bool) or minimum_days < 1:
    fail("matrix.retention.minimum_days must be a positive integer")
require_string(retention, "retain_through", "matrix.retention")
require_true(retention, "immutable_copy_required", "matrix.retention")
artifact_names = set(require_string_list(retention, "required_artifacts", "matrix.retention"))
for name in sorted(required_artifacts - artifact_names):
    fail(f"matrix.retention.required_artifacts is missing: {name}")

review = matrix.get("independent_review")
if not isinstance(review, dict):
    fail("matrix.independent_review must be an object")
    review = {}
require_true(review, "reviewer_must_differ_from_operator", "matrix.independent_review")
require_true(review, "raw_evidence_required", "matrix.independent_review")
decisions = set(require_string_list(review, "allowed_decisions", "matrix.independent_review"))
if decisions != {"ACCEPT", "REJECT", "INCONCLUSIVE"}:
    fail("matrix.independent_review.allowed_decisions must be ACCEPT, REJECT, INCONCLUSIVE")

required_workloads = {
    "create_inline_write",
    "direct_contiguous_growth",
    "direct_multi_block_overwrite",
    "direct_partial_overwrite",
    "direct_shrink_truncate",
    "directory_direct_append_and_growth",
    "directory_inline_append_and_growth",
    "open_truncate",
}
required_boundaries = {"journal_publish", "metadata_apply", "checkpoint_copy", "journal_reclaim"}
plan = matrix.get("test_plan")
if not isinstance(plan, dict):
    fail("matrix.test_plan must be an object")
    plan = {}
if plan.get("mount_mode") != "--controlled-write-mount":
    fail("matrix.test_plan.mount_mode must be --controlled-write-mount")
if plan.get("mount_options") != (
    "rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full"
):
    fail("matrix.test_plan.mount_options must preserve the controlled-write safe option set")
require_true(plan, "interruption_cross_product_required", "matrix.test_plan")
workloads = set(require_string_list(plan, "normal_workloads", "matrix.test_plan"))
for name in sorted(required_workloads - workloads):
    fail(f"matrix.test_plan.normal_workloads is missing: {name}")
boundaries = set(
    require_string_list(plan, "controlled_interruption_boundaries", "matrix.test_plan")
)
for name in sorted(required_boundaries - boundaries):
    fail(f"matrix.test_plan.controlled_interruption_boundaries is missing: {name}")
offline_checks = set(require_string_list(plan, "offline_checks", "matrix.test_plan"))
if offline_checks != {"fsck.kafs --check", "kafsdump --json"}:
    fail("matrix.test_plan.offline_checks must be fsck.kafs --check and kafsdump --json")
result_states = set(require_string_list(plan, "result_states", "matrix.test_plan"))
if result_states != {"PASS", "FAIL", "SKIP", "INCONCLUSIVE"}:
    fail("matrix.test_plan.result_states must be PASS, FAIL, SKIP, INCONCLUSIVE")

discovery = matrix.get("discovery")
if not isinstance(discovery, dict):
    fail("matrix.discovery must be an object")
    discovery = {}
require_string(discovery, "observed_at_utc", "matrix.discovery")
require_string(discovery, "host_class", "matrix.discovery")
require_string(discovery, "status", "matrix.discovery")
eligible_count = discovery.get("eligible_device_count")
if not isinstance(eligible_count, int) or isinstance(eligible_count, bool) or eligible_count < 0:
    fail("matrix.discovery.eligible_device_count must be a non-negative integer")

blocked_by = require_string_list(matrix, "blocked_by", "matrix")
samples = matrix.get("samples")
if not isinstance(samples, list):
    fail("matrix.samples must be an array")
    samples = []

if state == "DRAFT":
    if not blocked_by:
        fail("DRAFT matrix.blocked_by must name at least one unresolved identity or approval")
else:
    if blocked_by:
        fail("READY_FOR_APPROVAL matrix.blocked_by must be empty")
    if discovery.get("status") != "ELIGIBLE_REAL_MEDIA_IDENTIFIED":
        fail("READY_FOR_APPROVAL discovery.status must be ELIGIBLE_REAL_MEDIA_IDENTIFIED")
    if not isinstance(eligible_count, int) or eligible_count < 1:
        fail("READY_FOR_APPROVAL discovery must contain at least one eligible device")
    host = require_fields(
        matrix.get("target_host"),
        ("host_id", "kernel", "libfuse", "native_or_passthrough_environment"),
        "matrix.target_host",
    )
    if not host:
        fail("READY_FOR_APPROVAL matrix.target_host must be exact")
    cycles = plan.get("cycles_per_workload")
    if not isinstance(cycles, int) or isinstance(cycles, bool) or cycles < 1:
        fail("READY_FOR_APPROVAL test_plan.cycles_per_workload must be a positive integer")
    if not samples:
        fail("READY_FOR_APPROVAL matrix.samples must contain at least one sample")
    elif eligible_count != len(samples):
        fail("READY_FOR_APPROVAL discovery.eligible_device_count must equal sample count")

sample_ids: set[str] = set()
card_unit_ids: set[str] = set()
stable_device_paths: set[str] = set()
for index, sample in enumerate(samples):
    prefix = f"matrix.samples[{index}]"
    if not isinstance(sample, dict):
        fail(f"{prefix} must be an object")
        continue
    sample_id = require_string(sample, "sample_id", prefix)
    if sample_id in sample_ids:
        fail(f"duplicate sample_id: {sample_id}")
    sample_ids.add(sample_id)
    card = require_fields(
        sample.get("card"),
        ("manufacturer", "model", "unit_id", "unit_id_source"),
        f"{prefix}.card",
    )
    if card.get("unit_id_source") not in ("cid", "serial", "inventory-label"):
        fail(f"{prefix}.card.unit_id_source must be cid, serial, or inventory-label")
    card_unit_id = card.get("unit_id")
    if isinstance(card_unit_id, str) and card_unit_id:
        if card_unit_id in card_unit_ids:
            fail(f"duplicate card unit_id: {card_unit_id}")
        card_unit_ids.add(card_unit_id)
    card_capacity = card.get("capacity_bytes")
    if not isinstance(card_capacity, int) or isinstance(card_capacity, bool) or card_capacity < 1:
        fail(f"{prefix}.card.capacity_bytes must be a positive integer")
    reader = require_fields(
        sample.get("reader_controller"),
        ("manufacturer", "model", "stable_id", "transport"),
        f"{prefix}.reader_controller",
    )
    device = require_fields(
        sample.get("device"), ("stable_path", "kernel_path", "major_minor"), f"{prefix}.device"
    )
    stable_path = device.get("stable_path")
    if isinstance(stable_path, str) and stable_path:
        if not stable_path.startswith(("/dev/disk/by-id/", "/dev/disk/by-path/")):
            fail(f"{prefix}.device.stable_path must use /dev/disk/by-id or /dev/disk/by-path")
        if stable_path in stable_device_paths:
            fail(f"duplicate device stable_path: {stable_path}")
        stable_device_paths.add(stable_path)
    expected_size = device.get("expected_size_bytes")
    if not isinstance(expected_size, int) or isinstance(expected_size, bool) or expected_size < 1:
        fail(f"{prefix}.device.expected_size_bytes must be a positive integer")
    require_true(device, "whole_device", f"{prefix}.device")
    require_true(device, "disposable", f"{prefix}.device")
    require_false(device, "system_or_host_storage", f"{prefix}.device")
    require_false(device, "mounted", f"{prefix}.device")
    require_false(device, "swap", f"{prefix}.device")
    power = require_fields(
        sample.get("power_cut"),
        ("method", "apparatus_id", "trigger_protocol", "power_domain"),
        f"{prefix}.power_cut",
    )
    require_true(power, "host_storage_isolated", f"{prefix}.power_cut")
    if reader and power and power.get("power_domain") in ("host", "system-disk", "unknown"):
        fail(f"{prefix}.power_cut.power_domain must isolate the reader/card from host storage")

if require_approved and state != "READY_FOR_APPROVAL":
    fail("--require-approved requires matrix.state READY_FOR_APPROVAL")

if require_approved and approval_path is not None:
    approval = load_json(approval_path, "approval")
    if approval.get("schema") != "KAFS.V7RealMediaQualificationApproval.v1":
        fail("approval.schema must be KAFS.V7RealMediaQualificationApproval.v1")
    if approval.get("state") != "APPROVED":
        fail("approval.state must be APPROVED")
    if approval.get("matrix_id") != matrix_id:
        fail("approval.matrix_id does not match matrix.matrix_id")
    if approval.get("matrix_sha256") != matrix_digest:
        fail("approval.matrix_sha256 does not match the matrix bytes")
    require_string(approval, "approved_by", "approval")
    approved_at = require_string(approval, "approved_at_utc", "approval")
    valid_until = require_string(approval, "valid_until_utc", "approval")
    actions = set(require_string_list(approval, "authorized_actions", "approval"))
    if actions != {"format_whole_device", "controlled_power_interruption"}:
        fail("approval.authorized_actions must be format_whole_device and controlled_power_interruption")
    require_true(approval, "destructive_impact_acknowledged", "approval")
    require_true(approval, "disposable_media_confirmed", "approval")
    confirmation = approval.get("confirmation")
    expected_confirmation = f"AUTHORIZE T48 {matrix_id} {matrix_digest}"
    if confirmation != expected_confirmation:
        fail("approval.confirmation does not bind the T48 matrix id and digest")
    try:
        approved = datetime.fromisoformat(approved_at.replace("Z", "+00:00"))
        expires = datetime.fromisoformat(valid_until.replace("Z", "+00:00"))
        if approved.tzinfo is None or expires.tzinfo is None:
            raise ValueError("timezone missing")
        if approved > datetime.now(timezone.utc):
            fail("approval.approved_at_utc must not be in the future")
        if expires <= approved:
            fail("approval.valid_until_utc must be later than approved_at_utc")
        if expires <= datetime.now(timezone.utc):
            fail("approval.valid_until_utc has expired")
    except ValueError:
        fail("approval timestamps must be ISO-8601 values with timezone")

if errors:
    for error in errors:
        print(f"FAIL: {error}", file=sys.stderr)
    print(f"v7 real-media approval gate FAIL ({len(errors)} failures)", file=sys.stderr)
    raise SystemExit(1)

if require_approved:
    status = "APPROVED"
elif state == "READY_FOR_APPROVAL":
    status = "READY_FOR_APPROVAL"
else:
    status = "DRAFT_VALID"
print(f"v7 real-media approval gate {status}")
print(f"matrix: {matrix_path}")
print(f"matrix_sha256: {matrix_digest}")
print("NOTE: validation does not access or authorize a device by itself")
PY
