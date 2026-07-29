#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/v7-real-media-qualification-evidence-gate.sh \
    --evidence-dir DIR --matrix FILE --approval FILE --validate-only
  scripts/v7-real-media-qualification-evidence-gate.sh \
    --evidence-dir DIR --matrix FILE --approval FILE \
    --review FILE --validate-only --require-review

Validate an immutable format-v7 real-media evidence bundle and, when required,
an independent review decision bound to the exact evidence SHA-256. This gate
never opens a device, mounts or formats media, or controls power.
EOF
}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
APPROVAL_GATE=${KAFS_V7_REAL_MEDIA_APPROVAL_GATE:-$SCRIPT_DIR/v7-real-media-qualification-approval-gate.sh}
EVIDENCE_DIR=""
MATRIX=""
APPROVAL=""
REVIEW=""
VALIDATE_ONLY=0
REQUIRE_REVIEW=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --evidence-dir)
      [[ $# -ge 2 ]] || { usage >&2; exit 2; }
      EVIDENCE_DIR="$2"
      shift 2
      ;;
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
    --review)
      [[ $# -ge 2 ]] || { usage >&2; exit 2; }
      REVIEW="$2"
      shift 2
      ;;
    --validate-only)
      VALIDATE_ONLY=1
      shift
      ;;
    --require-review)
      REQUIRE_REVIEW=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      echo "real-media evidence gate: unknown option: $1" >&2
      exit 2
      ;;
  esac
done

[[ "$VALIDATE_ONLY" -eq 1 ]] || { usage >&2; exit 2; }
[[ -n "$EVIDENCE_DIR" && -d "$EVIDENCE_DIR" ]] || {
  echo "real-media evidence gate: evidence directory not found: ${EVIDENCE_DIR:-<missing>}" >&2
  exit 2
}
[[ -n "$MATRIX" && -f "$MATRIX" ]] || {
  echo "real-media evidence gate: matrix not found: ${MATRIX:-<missing>}" >&2
  exit 2
}
[[ -n "$APPROVAL" && -f "$APPROVAL" ]] || {
  echo "real-media evidence gate: approval not found: ${APPROVAL:-<missing>}" >&2
  exit 2
}
if [[ "$REQUIRE_REVIEW" -eq 1 ]]; then
  [[ -n "$REVIEW" && -f "$REVIEW" ]] || {
    echo "real-media evidence gate: --review FILE is required with --require-review" >&2
    exit 2
  }
elif [[ -n "$REVIEW" ]]; then
  echo "real-media evidence gate: --review requires --require-review" >&2
  exit 2
fi
[[ -x "$APPROVAL_GATE" ]] || {
  echo "real-media evidence gate: approval gate is not executable: $APPROVAL_GATE" >&2
  exit 2
}
command -v python3 >/dev/null 2>&1 || {
  echo "real-media evidence gate: python3 is required" >&2
  exit 2
}

EVIDENCE_DIR=$(cd "$EVIDENCE_DIR" && pwd)
EVIDENCE="$EVIDENCE_DIR/evidence.json"
[[ -f "$EVIDENCE" ]] || {
  echo "real-media evidence gate: evidence.json not found below $EVIDENCE_DIR" >&2
  exit 2
}

AS_OF=$(python3 - "$EVIDENCE" <<'PY'
import json
from pathlib import Path
import sys

try:
    value = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as exc:
    print(f"real-media evidence gate: cannot read evidence start time: {exc}", file=sys.stderr)
    raise SystemExit(2)
started = value.get("started_at_utc") if isinstance(value, dict) else None
if not isinstance(started, str) or not started:
    print("real-media evidence gate: started_at_utc must be a non-empty string", file=sys.stderr)
    raise SystemExit(2)
print(started)
PY
)

"$APPROVAL_GATE" \
  --matrix "$MATRIX" \
  --approval "$APPROVAL" \
  --validate-only \
  --require-approved \
  --as-of "$AS_OF" >/dev/null

python3 - "$EVIDENCE_DIR" "$MATRIX" "$APPROVAL" "$REVIEW" "$REQUIRE_REVIEW" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys


evidence_root = Path(sys.argv[1]).resolve()
matrix_path = Path(sys.argv[2])
approval_path = Path(sys.argv[3])
review_path = Path(sys.argv[4]) if sys.argv[4] else None
require_review = sys.argv[5] == "1"
evidence_path = evidence_root / "evidence.json"
errors: list[str] = []


def fail(message: str) -> None:
    errors.append(message)


def load_json(path: Path, label: str) -> tuple[dict, bytes]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read {label}: {exc}")
        return {}, b""
    if not isinstance(value, dict):
        fail(f"{label} must contain a JSON object")
        return {}, raw
    return value, raw


def require_string(obj: dict, key: str, prefix: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value.strip():
        fail(f"{prefix}.{key} must be a non-empty string")
        return ""
    return value


def require_false(obj: dict, key: str, prefix: str) -> None:
    if obj.get(key) is not False:
        fail(f"{prefix}.{key} must be false")


def require_true(obj: dict, key: str, prefix: str) -> None:
    if obj.get(key) is not True:
        fail(f"{prefix}.{key} must be true")


def require_digest(obj: dict, key: str, prefix: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        fail(f"{prefix}.{key} must be a lowercase SHA-256 digest")
        return ""
    return value


def require_string_list(obj: dict, key: str, prefix: str, allow_empty: bool = False) -> list[str]:
    value = obj.get(key)
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        fail(f"{prefix}.{key} must be an array of non-empty strings")
        return []
    if not allow_empty and not value:
        fail(f"{prefix}.{key} must not be empty")
    if len(value) != len(set(value)):
        fail(f"{prefix}.{key} must not contain duplicates")
    return value


def parse_timestamp(value: object, label: str):
    if not isinstance(value, str) or not value:
        fail(f"{label} must be a non-empty ISO-8601 timestamp")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("timezone missing")
        return parsed.astimezone(timezone.utc)
    except ValueError:
        fail(f"{label} must be an ISO-8601 value with timezone")
        return None


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


matrix, matrix_raw = load_json(matrix_path, "matrix")
approval, approval_raw = load_json(approval_path, "approval")
evidence, evidence_raw = load_json(evidence_path, "evidence")
matrix_digest = sha256(matrix_raw)
approval_digest = sha256(approval_raw)
evidence_digest = sha256(evidence_raw)

if evidence.get("schema") != "KAFS.V7RealMediaQualificationEvidence.v1":
    fail("evidence.schema must be KAFS.V7RealMediaQualificationEvidence.v1")
if evidence.get("scope") != "format-v7-bounded-controlled-write-real-media":
    fail("evidence.scope must be format-v7-bounded-controlled-write-real-media")
if evidence.get("state") != "COMPLETE":
    fail("evidence.state must be COMPLETE")
run_id = require_string(evidence, "run_id", "evidence")

binding = evidence.get("binding")
if not isinstance(binding, dict):
    fail("evidence.binding must be an object")
    binding = {}
matrix_id = require_string(matrix, "matrix_id", "matrix")
if binding.get("matrix_id") != matrix_id:
    fail("evidence.binding.matrix_id does not match matrix.matrix_id")
if require_digest(binding, "matrix_sha256", "evidence.binding") != matrix_digest:
    fail("evidence.binding.matrix_sha256 does not match the matrix bytes")
if require_digest(binding, "approval_sha256", "evidence.binding") != approval_digest:
    fail("evidence.binding.approval_sha256 does not match the approval bytes")

operator = evidence.get("operator")
if not isinstance(operator, dict):
    fail("evidence.operator must be an object")
    operator = {}
operator_id = require_string(operator, "id", "evidence.operator")

started = parse_timestamp(evidence.get("started_at_utc"), "evidence.started_at_utc")
completed = parse_timestamp(evidence.get("completed_at_utc"), "evidence.completed_at_utc")
if started is not None and completed is not None and completed < started:
    fail("evidence.completed_at_utc must not precede started_at_utc")
current_time = datetime.now(timezone.utc)
if started is not None and started > current_time:
    fail("evidence.started_at_utc must not be in the future")
if completed is not None and completed > current_time:
    fail("evidence.completed_at_utc must not be in the future")

claims = evidence.get("claims")
if not isinstance(claims, dict):
    fail("evidence.claims must be an object")
    claims = {}
for key in ("rc_eligible", "real_media_qualified", "controller_independent_wear"):
    require_false(claims, key, "evidence.claims")

target_host = matrix.get("target_host")
if not isinstance(target_host, dict):
    fail("matrix.target_host must be an object")
    target_host = {}
if evidence.get("environment") != target_host:
    fail("evidence.environment does not exactly match matrix.target_host")

retention = evidence.get("retention")
if not isinstance(retention, dict):
    fail("evidence.retention must be an object")
    retention = {}
require_true(retention, "immutable_copy_created", "evidence.retention")
matrix_retention = matrix.get("retention")
if not isinstance(matrix_retention, dict):
    fail("matrix.retention must be an object")
    matrix_retention = {}
if retention.get("minimum_days") != matrix_retention.get("minimum_days"):
    fail("evidence.retention.minimum_days does not match the matrix")
if retention.get("retain_through") != matrix_retention.get("retain_through"):
    fail("evidence.retention.retain_through does not match the matrix")

required_artifact_kinds = set(
    require_string_list(matrix_retention, "required_artifacts", "matrix.retention")
)
artifacts = evidence.get("artifacts")
artifact_by_path: dict[str, dict] = {}
artifact_kinds: set[str] = set()
if not isinstance(artifacts, list):
    fail("evidence.artifacts must be an array")
    artifacts = []
for index, artifact in enumerate(artifacts):
    prefix = f"evidence.artifacts[{index}]"
    if not isinstance(artifact, dict):
        fail(f"{prefix} must be an object")
        continue
    relative = require_string(artifact, "path", prefix)
    kind = require_string(artifact, "kind", prefix)
    expected_digest = require_digest(artifact, "sha256", prefix)
    expected_size = artifact.get("bytes")
    if not isinstance(expected_size, int) or isinstance(expected_size, bool) or expected_size < 0:
        fail(f"{prefix}.bytes must be a non-negative integer")
    if not relative:
        continue
    pure = PurePosixPath(relative)
    if pure.is_absolute() or ".." in pure.parts:
        fail(f"artifact path must stay below the evidence directory: {relative}")
        continue
    if relative == "evidence.json":
        fail("evidence.json must not be self-listed as an artifact")
        continue
    if relative in artifact_by_path:
        fail(f"duplicate artifact path: {relative}")
        continue
    artifact_by_path[relative] = artifact
    artifact_kinds.add(kind)
    path = evidence_root / Path(*pure.parts)
    if not path.is_file():
        fail(f"artifact is missing: {relative}")
        continue
    try:
        path.resolve(strict=True).relative_to(evidence_root)
    except (OSError, ValueError):
        fail(f"artifact resolves outside the evidence directory: {relative}")
        continue
    raw = path.read_bytes()
    if expected_digest != sha256(raw):
        fail(f"artifact digest mismatch: {relative}")
    if expected_size != len(raw):
        fail(f"artifact size mismatch: {relative}")
for kind in sorted(required_artifact_kinds - artifact_kinds):
    fail(f"required artifact kind is missing: {kind}")

plan = matrix.get("test_plan")
if not isinstance(plan, dict):
    fail("matrix.test_plan must be an object")
    plan = {}
workloads = require_string_list(plan, "normal_workloads", "matrix.test_plan")
boundaries = ["normal"] + require_string_list(
    plan, "controlled_interruption_boundaries", "matrix.test_plan"
)
allowed_states = set(require_string_list(plan, "result_states", "matrix.test_plan"))
cycles = plan.get("cycles_per_workload")
if not isinstance(cycles, int) or isinstance(cycles, bool) or cycles < 1:
    fail("matrix.test_plan.cycles_per_workload must be a positive integer")
    cycles = 0
expected_result_keys = {
    (workload, boundary, cycle)
    for workload in workloads
    for boundary in boundaries
    for cycle in range(1, cycles + 1)
}

matrix_samples = matrix.get("samples")
if not isinstance(matrix_samples, list):
    fail("matrix.samples must be an array")
    matrix_samples = []
matrix_sample_by_id = {
    sample.get("sample_id"): sample
    for sample in matrix_samples
    if isinstance(sample, dict) and isinstance(sample.get("sample_id"), str)
}
evidence_samples = evidence.get("samples")
if not isinstance(evidence_samples, list):
    fail("evidence.samples must be an array")
    evidence_samples = []
evidence_sample_ids: set[str] = set()
all_result_statuses: list[str] = []
review_decision = ""
for sample_index, sample_evidence in enumerate(evidence_samples):
    prefix = f"evidence.samples[{sample_index}]"
    if not isinstance(sample_evidence, dict):
        fail(f"{prefix} must be an object")
        continue
    sample_id = require_string(sample_evidence, "sample_id", prefix)
    if sample_id in evidence_sample_ids:
        fail(f"duplicate evidence sample_id: {sample_id}")
    evidence_sample_ids.add(sample_id)
    matrix_sample = matrix_sample_by_id.get(sample_id)
    if matrix_sample is None:
        fail(f"evidence sample is not present in the matrix: {sample_id}")
        continue
    expected_identity = {
        key: matrix_sample.get(key)
        for key in ("card", "reader_controller", "device", "power_cut")
    }
    if sample_evidence.get("identity_before") != expected_identity:
        fail(f"evidence sample {sample_id} identity_before does not match the matrix")
    if sample_evidence.get("identity_after") != expected_identity:
        fail(f"evidence sample {sample_id} identity_after does not match the matrix")
    results = sample_evidence.get("results")
    if not isinstance(results, list):
        fail(f"{prefix}.results must be an array")
        results = []
    actual_result_keys: set[tuple[str, str, int]] = set()
    for result_index, result in enumerate(results):
        result_prefix = f"{prefix}.results[{result_index}]"
        if not isinstance(result, dict):
            fail(f"{result_prefix} must be an object")
            continue
        workload = require_string(result, "workload", result_prefix)
        boundary = require_string(result, "boundary", result_prefix)
        cycle = result.get("cycle")
        if not isinstance(cycle, int) or isinstance(cycle, bool) or cycle < 1:
            fail(f"{result_prefix}.cycle must be a positive integer")
            cycle = 0
        result_key = (workload, boundary, cycle)
        if result_key in actual_result_keys:
            fail(f"duplicate result for sample {sample_id}: {result_key}")
        actual_result_keys.add(result_key)
        status = result.get("status")
        if status not in allowed_states:
            fail(f"{result_prefix}.status is not allowed by the matrix")
        elif isinstance(status, str):
            all_result_statuses.append(status)
        allowed_outcome = result.get("allowed_outcome")
        if not isinstance(allowed_outcome, bool):
            fail(f"{result_prefix}.allowed_outcome must be a boolean")
        elif (status == "PASS") != allowed_outcome:
            fail(f"{result_prefix}.status and allowed_outcome disagree")
        require_string(result, "observed_outcome", result_prefix)
        evidence_paths = require_string_list(result, "evidence", result_prefix)
        referenced_kinds: set[str] = set()
        for relative in evidence_paths:
            artifact = artifact_by_path.get(relative)
            if artifact is None:
                fail(f"{result_prefix} references unknown artifact: {relative}")
                continue
            kind = artifact.get("kind")
            if isinstance(kind, str):
                referenced_kinds.add(kind)
        required_result_kinds = {
            "command-log",
            "workload-log",
            "mount-log",
            "fsck-after",
            "kafsdump-after",
        }
        if boundary != "normal":
            required_result_kinds.add("power-cut-log")
        for kind in sorted(required_result_kinds - referenced_kinds):
            fail(f"{result_prefix} does not reference artifact kind: {kind}")
    for missing in sorted(expected_result_keys - actual_result_keys):
        fail(f"sample {sample_id} is missing result: {missing}")
    for unexpected in sorted(actual_result_keys - expected_result_keys):
        fail(f"sample {sample_id} has unexpected result: {unexpected}")

for sample_id in sorted(matrix_sample_by_id.keys() - evidence_sample_ids):
    fail(f"matrix sample is missing from evidence: {sample_id}")

if require_review and review_path is not None:
    review, _ = load_json(review_path, "review")
    if review.get("schema") != "KAFS.V7RealMediaQualificationReview.v1":
        fail("review.schema must be KAFS.V7RealMediaQualificationReview.v1")
    if review.get("state") != "FINAL":
        fail("review.state must be FINAL")
    require_string(review, "review_id", "review")
    if review.get("run_id") != run_id:
        fail("review.run_id does not match evidence.run_id")
    review_binding = review.get("binding")
    if not isinstance(review_binding, dict):
        fail("review.binding must be an object")
        review_binding = {}
    if review_binding.get("matrix_id") != matrix_id:
        fail("review.binding.matrix_id does not match the matrix")
    if require_digest(review_binding, "matrix_sha256", "review.binding") != matrix_digest:
        fail("review.binding.matrix_sha256 does not match the matrix bytes")
    if require_digest(review_binding, "approval_sha256", "review.binding") != approval_digest:
        fail("review.binding.approval_sha256 does not match the approval bytes")
    if require_digest(review_binding, "evidence_sha256", "review.binding") != evidence_digest:
        fail("review.binding.evidence_sha256 does not match evidence.json")
    if review.get("operator_id") != operator_id:
        fail("review.operator_id does not match evidence.operator.id")
    reviewer = review.get("reviewer")
    if not isinstance(reviewer, dict):
        fail("review.reviewer must be an object")
        reviewer = {}
    reviewer_id = require_string(reviewer, "id", "review.reviewer")
    if reviewer_id and reviewer_id == operator_id:
        fail("reviewer must differ from the evidence operator")
    decision = review.get("decision")
    allowed_decisions = set(
        require_string_list(matrix.get("independent_review", {}), "allowed_decisions", "matrix.independent_review")
    )
    if decision not in allowed_decisions:
        fail("review.decision is not allowed by the matrix")
    elif isinstance(decision, str):
        review_decision = decision
    reviewed_at = parse_timestamp(review.get("reviewed_at_utc"), "review.reviewed_at_utc")
    if completed is not None and reviewed_at is not None and reviewed_at < completed:
        fail("review.reviewed_at_utc must not precede evidence completion")
    if reviewed_at is not None and reviewed_at > current_time:
        fail("review.reviewed_at_utc must not be in the future")
    require_true(review, "raw_evidence_reviewed", "review")
    checks = review.get("checks")
    required_checks = {
        "approval_binding_verified",
        "identity_continuity_verified",
        "coverage_complete",
        "artifact_digests_verified",
        "allowed_outcomes_verified",
        "claim_boundary_verified",
    }
    if not isinstance(checks, dict):
        fail("review.checks must be an object")
        checks = {}
    for check in sorted(required_checks):
        if not isinstance(checks.get(check), bool):
            fail(f"review.checks.{check} must be a boolean")
    findings = require_string_list(review, "findings", "review", allow_empty=True)
    review_claims = review.get("claims")
    if not isinstance(review_claims, dict):
        fail("review.claims must be an object")
        review_claims = {}
    for key in ("rc_eligible", "real_media_qualified", "controller_independent_wear"):
        require_false(review_claims, key, "review.claims")
    expected_confirmation = f"REVIEW T58 {run_id} {evidence_digest} {decision}"
    if review.get("confirmation") != expected_confirmation:
        fail("review.confirmation does not bind the run, evidence digest, and decision")
    if decision == "ACCEPT":
        if any(status != "PASS" for status in all_result_statuses):
            fail("ACCEPT requires every evidence result to be PASS")
        for check in sorted(required_checks):
            if checks.get(check) is not True:
                fail(f"ACCEPT requires review.checks.{check} to be true")
    elif not findings:
        fail("REJECT or INCONCLUSIVE review must contain at least one finding")

if errors:
    for error in errors:
        print(f"FAIL: {error}", file=sys.stderr)
    print(f"v7 real-media evidence gate FAIL ({len(errors)} failures)", file=sys.stderr)
    raise SystemExit(1)

status = "REVIEW_VALID" if require_review else "EVIDENCE_VALID"
print(f"v7 real-media evidence gate {status}")
print(f"run_id: {run_id}")
print(f"matrix_sha256: {matrix_digest}")
print(f"approval_sha256: {approval_digest}")
print(f"evidence_sha256: {evidence_digest}")
if require_review:
    print(f"review_decision: {review_decision}")
print("NOTE: validation does not access or authorize a device by itself")
PY
