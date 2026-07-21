#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/v7-controlled-write-qualification-gate.sh --report-dir DIR --validate-only

Validate a non-destructive format-v7 controlled-write qualification report.
The gate never mounts, formats, or modifies an image or device.
EOF
}

REPORT_DIR=""
VALIDATE_ONLY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --report-dir)
      [[ $# -ge 2 ]] || { usage >&2; exit 2; }
      REPORT_DIR="$2"
      shift 2
      ;;
    --validate-only)
      VALIDATE_ONLY=1
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
[[ -n "$REPORT_DIR" && -d "$REPORT_DIR" ]] || {
  echo "qualification gate: report directory not found: ${REPORT_DIR:-<missing>}" >&2
  exit 2
}
command -v python3 >/dev/null 2>&1 || {
  echo "qualification gate: python3 is required" >&2
  exit 2
}

REPORT_DIR=$(cd "$REPORT_DIR" && pwd)

python3 - "$REPORT_DIR" <<'PY'
from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
import sys


report_dir = Path(sys.argv[1])
manifest_path = report_dir / "qualification.json"
errors: list[str] = []


def fail(message: str) -> None:
    errors.append(message)


try:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as exc:
    manifest = {}
    fail(f"cannot read qualification.json: {exc}")

if manifest.get("schema") != "KAFS.V7ControlledWriteQualification.v1":
    fail("schema must be KAFS.V7ControlledWriteQualification.v1")
if manifest.get("scope") != "non-destructive-file-image":
    fail("scope must be non-destructive-file-image")
if manifest.get("status") != "PASS":
    fail("report status must be PASS")

claims = manifest.get("claims")
if not isinstance(claims, dict):
    fail("claims must be an object")
    claims = {}
for key in ("rc_eligible", "real_media_qualified", "controller_independent_wear"):
    if claims.get(key) is not False:
        fail(f"claims.{key} must be false")

sample = manifest.get("sample")
if not isinstance(sample, dict):
    fail("sample must be an object")
    sample = {}
for key in ("id", "media_class", "backing", "controller", "power_interruption"):
    if not isinstance(sample.get(key), str) or not sample[key]:
        fail(f"sample.{key} must be a non-empty string")
if sample.get("media_class") != "file-image":
    fail("sample.media_class must be file-image")
if sample.get("backing") != "temporary-sparse-image":
    fail("sample.backing must be temporary-sparse-image")
if sample.get("controller") != "not-applicable":
    fail("sample.controller must be not-applicable")
if sample.get("power_interruption") != "process-fault-injection":
    fail("sample.power_interruption must be process-fault-injection")
if sample.get("device_path") not in (None, ""):
    fail("sample.device_path is forbidden for the non-destructive slice")

environment = manifest.get("environment")
if not isinstance(environment, dict):
    fail("environment must be an object")
    environment = {}
for key in ("generated_at_utc", "git_commit", "uname", "kernel", "libfuse"):
    if not isinstance(environment.get(key), str) or not environment[key]:
        fail(f"environment.{key} must be a non-empty string")
if environment.get("libfuse") == "unknown":
    fail("environment.libfuse must identify the installed version")
if not isinstance(environment.get("git_dirty"), bool):
    fail("environment.git_dirty must be a boolean")
if manifest.get("workload_engine") != "tests/v7_inspection_mount_smoketest":
    fail("workload_engine must be tests/v7_inspection_mount_smoketest")

required_cases = {
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
}

artifacts = manifest.get("artifacts")
artifact_by_path: dict[str, dict] = {}
report_root = report_dir.resolve()
if not isinstance(artifacts, list):
    fail("artifacts must be an array")
    artifacts = []
for artifact in artifacts:
    if not isinstance(artifact, dict):
        fail("each artifact must be an object")
        continue
    relative = artifact.get("path")
    expected_digest = artifact.get("sha256")
    expected_size = artifact.get("bytes")
    if not isinstance(relative, str) or not relative:
        fail("artifact.path must be a non-empty string")
        continue
    pure = PurePosixPath(relative)
    if pure.is_absolute() or ".." in pure.parts:
        fail(f"artifact path must stay below the report directory: {relative}")
        continue
    if relative in artifact_by_path:
        fail(f"duplicate artifact path: {relative}")
        continue
    artifact_by_path[relative] = artifact
    path = report_dir / Path(*pure.parts)
    if not path.is_file():
        fail(f"artifact is missing: {relative}")
        continue
    try:
        path.resolve(strict=True).relative_to(report_root)
    except (OSError, ValueError):
        fail(f"artifact resolves outside the report directory: {relative}")
        continue
    data = path.read_bytes()
    actual_digest = hashlib.sha256(data).hexdigest()
    if not isinstance(expected_digest, str) or actual_digest != expected_digest:
        fail(f"artifact digest mismatch: {relative}")
    if not isinstance(expected_size, int) or expected_size != len(data):
        fail(f"artifact size mismatch: {relative}")

for relative in ("artifacts/executables.sha256", "artifacts/workload.command"):
    if relative not in artifact_by_path:
        fail(f"required artifact is missing from the manifest: {relative}")

results = manifest.get("results")
case_by_id: dict[str, dict] = {}
if not isinstance(results, list):
    fail("results must be an array")
    results = []
for result in results:
    if not isinstance(result, dict):
        fail("each result must be an object")
        continue
    case_id = result.get("id")
    if not isinstance(case_id, str) or not case_id:
        fail("result.id must be a non-empty string")
        continue
    if case_id in case_by_id:
        fail(f"duplicate result id: {case_id}")
        continue
    case_by_id[case_id] = result
    if result.get("status") != "PASS":
        fail(f"result {case_id} is not PASS")
    evidence = result.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        fail(f"result {case_id} must name evidence artifacts")
        continue
    for relative in evidence:
        if relative not in artifact_by_path:
            fail(f"result {case_id} references unknown artifact: {relative}")

for case_id in sorted(required_cases - case_by_id.keys()):
    fail(f"required result is missing: {case_id}")

limitations = manifest.get("limitations")
required_limitations = {
    "not-real-media-qualification",
    "no-physical-power-cut",
    "no-nand-ftl-independence-claim",
    "not-rc-approval",
}
if not isinstance(limitations, list):
    fail("limitations must be an array")
else:
    missing = required_limitations - set(limitations)
    for limitation in sorted(missing):
        fail(f"required limitation is missing: {limitation}")

summary = {
    "schema": "KAFS.V7ControlledWriteQualificationGate.v1",
    "status": "PASS" if not errors else "FAIL",
    "failures": errors,
    "report": str(manifest_path),
}
(report_dir / "qualification-gate.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)

if errors:
    for error in errors:
        print(f"FAIL: {error}", file=sys.stderr)
    print(f"v7 controlled-write qualification gate FAIL ({len(errors)} failures)", file=sys.stderr)
    raise SystemExit(1)

print("v7 controlled-write qualification gate PASS")
print(f"report: {manifest_path}")
print("NOTE: this is non-destructive file-image evidence, not RC approval")
PY
