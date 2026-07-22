#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/v7-vhdx-evidence-audit-gate.sh --run-dir DIR --validate-only

Validate one complete four-fault VHDX host-recovery evidence run. The command
only reads retained files. It never opens a VHDX, mounts a filesystem, writes
an image, or terminates/restarts WSL.
EOF
}

RUN_DIR=""
VALIDATE_ONLY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-dir)
      [[ $# -ge 2 ]] || { usage >&2; exit 2; }
      RUN_DIR="$2"
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
      echo "VHDX evidence audit gate: unknown option: $1" >&2
      exit 2
      ;;
  esac
done

[[ "$VALIDATE_ONLY" -eq 1 ]] || { usage >&2; exit 2; }
[[ -n "$RUN_DIR" && -d "$RUN_DIR" && ! -L "$RUN_DIR" ]] || {
  echo "VHDX evidence audit gate: run directory is invalid: ${RUN_DIR:-<missing>}" >&2
  exit 2
}
command -v python3 >/dev/null 2>&1 || {
  echo "VHDX evidence audit gate: python3 is required" >&2
  exit 2
}

python3 - "$RUN_DIR" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys


run_root = Path(sys.argv[1]).resolve()
expected_faults = (
    "journal_publish",
    "checkpoint_copy",
    "metadata_apply",
    "journal_reclaim",
)
false_claims = (
    "raw_vhdx_access",
    "real_media_qualified",
    "physical_power_interruption",
    "controller_independent_wear_qualified",
    "release_candidate_qualified",
)
errors: list[str] = []
now = datetime.now(timezone.utc)


def fail(message: str) -> None:
    errors.append(message)


def load_json(path: Path, label: str) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
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


def require_exact_bool(obj: dict, key: str, expected: bool, prefix: str) -> None:
    if obj.get(key) is not expected:
        fail(f"{prefix}.{key} must be {str(expected).lower()}")


def require_positive_int(obj: dict, key: str, prefix: str) -> int:
    value = obj.get(key)
    if type(value) is not int or value <= 0:
        fail(f"{prefix}.{key} must be a positive integer")
        return 0
    return value


def require_zero_int(obj: dict, key: str, prefix: str) -> None:
    value = obj.get(key)
    if type(value) is not int or value != 0:
        fail(f"{prefix}.{key} must be integer zero")


def require_false_claims(obj: dict, prefix: str) -> None:
    for key in false_claims:
        require_exact_bool(obj, key, False, prefix)


def parse_timestamp(value: object, label: str):
    if not isinstance(value, str) or not value:
        fail(f"{label} must be a non-empty ISO-8601 timestamp")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("timezone missing")
        parsed = parsed.astimezone(timezone.utc)
    except ValueError:
        fail(f"{label} must be an ISO-8601 value with timezone")
        return None
    if parsed > now:
        fail(f"{label} must not be in the future")
    return parsed


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def parse_inventory(state_dir: Path, fault: str) -> set[str]:
    inventory = state_dir / "artifacts.sha256"
    if not inventory.is_file() or inventory.is_symlink():
        fail(f"{fault}: artifacts.sha256 is missing or not a regular file")
        return set()
    try:
        lines = inventory.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        fail(f"{fault}: cannot read artifacts.sha256: {exc}")
        return set()
    if not lines:
        fail(f"{fault}: artifacts.sha256 must not be empty")
    listed: dict[str, str] = {}
    for line_number, line in enumerate(lines, 1):
        match = re.fullmatch(r"([0-9a-f]{64})  \./([^/\x00]+)", line)
        if match is None:
            fail(f"{fault}: malformed artifacts.sha256 line {line_number}")
            continue
        digest, name = match.groups()
        if name == "artifacts.sha256":
            fail(f"{fault}: artifacts.sha256 must not list itself")
            continue
        if name in listed:
            fail(f"{fault}: duplicate artifact inventory entry: {name}")
            continue
        listed[name] = digest

    actual: set[str] = set()
    try:
        children = list(state_dir.iterdir())
        state_device = state_dir.stat().st_dev
    except OSError as exc:
        fail(f"{fault}: cannot enumerate state directory: {exc}")
        return set(listed)
    for child in children:
        if child.is_symlink():
            fail(f"{fault}: symbolic links are not allowed: {child.name}")
            continue
        if child.is_file() and child.name != "artifacts.sha256":
            actual.add(child.name)
        elif child.is_dir():
            try:
                if child.stat().st_dev != state_device:
                    fail(f"{fault}: retained directory crosses a filesystem boundary: {child.name}")
                    continue
                if next(child.iterdir(), None) is not None:
                    fail(f"{fault}: retained directory must be empty after unmount: {child.name}")
            except OSError as exc:
                fail(f"{fault}: cannot enumerate retained directory {child.name}: {exc}")
        elif child.name != "artifacts.sha256":
            fail(f"{fault}: unsupported retained entry type: {child.name}")
    if set(listed) != actual:
        for name in sorted(actual - set(listed)):
            fail(f"{fault}: regular artifact is absent from inventory: {name}")
        for name in sorted(set(listed) - actual):
            fail(f"{fault}: inventoried artifact is absent: {name}")
    for name, expected_digest in listed.items():
        path = state_dir / name
        if not path.is_file() or path.is_symlink():
            continue
        try:
            actual_digest = sha256_path(path)
        except OSError as exc:
            fail(f"{fault}: cannot hash artifact {name}: {exc}")
            continue
        if actual_digest != expected_digest:
            fail(f"{fault}: artifact digest mismatch: {name}")
    return set(listed)


def parse_meta(path: Path, fault: str) -> None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        fail(f"{fault}: cannot read vhdx-recovery.meta: {exc}")
        return
    fields: dict[str, str] = {}
    for line in lines:
        if "=" not in line:
            fail(f"{fault}: malformed vhdx-recovery.meta")
            continue
        key, value = line.split("=", 1)
        if key in fields:
            fail(f"{fault}: duplicate vhdx-recovery.meta field: {key}")
        fields[key] = value
    if set(fields) != {"schema", "fault", "ino", "block_size"}:
        fail(f"{fault}: vhdx-recovery.meta fields are incomplete or unexpected")
    if fields.get("schema") != "KAFS.V7VhdxRecoveryState.v1":
        fail(f"{fault}: vhdx-recovery.meta schema mismatch")
    if fields.get("fault") != fault:
        fail(f"{fault}: vhdx-recovery.meta fault mismatch")
    for key in ("ino", "block_size"):
        try:
            value = int(fields.get(key, ""), 10)
        except ValueError:
            value = 0
        if value <= 0:
            fail(f"{fault}: vhdx-recovery.meta {key} must be positive")


def parse_recovery_log(path: Path, fault: str) -> None:
    try:
        lines = path.read_text(encoding="utf-8", errors="strict").splitlines()
    except (OSError, UnicodeError) as exc:
        fail(f"{fault}: cannot read recovery diagnostic: {exc}")
        return
    prefix = "kafs-v7-recovery "
    diagnostics = [line[len(prefix):] for line in lines if line.startswith(prefix)]
    if len(diagnostics) != 1:
        fail(f"{fault}: recovery log must contain exactly one diagnostic")
        return
    fields: dict[str, str] = {}
    for token in diagnostics[0].split():
        if "=" not in token:
            fail(f"{fault}: malformed recovery diagnostic token")
            continue
        key, value = token.split("=", 1)
        if key in fields:
            fail(f"{fault}: duplicate recovery diagnostic field: {key}")
        fields[key] = value
    numeric_fields = {
        "initial_checkpoint_generation",
        "initial_checkpoint_sequence",
        "initial_nonempty_segments",
        "applied_targets",
        "applied_mutations",
        "already_applied_mutations",
        "checkpoint_publications",
        "checkpoint_resumes",
        "reclaimed_segments",
        "already_empty_segments",
        "final_checkpoint_generation",
        "final_checkpoint_sequence",
        "final_nonempty_segments",
    }
    required = {"status", "format", "trigger", "resume_from"} | numeric_fields
    if set(fields) != required:
        fail(f"{fault}: recovery diagnostic fields are incomplete or unexpected")
    if fields.get("status") != "completed" or fields.get("format") != "v7":
        fail(f"{fault}: recovery diagnostic is not completed format v7")
    if fields.get("trigger") != "controlled-admission" or fields.get("resume_from") != fault:
        fail(f"{fault}: recovery diagnostic trigger/resume boundary mismatch")
    numbers: dict[str, int] = {}
    for key in numeric_fields:
        try:
            value = int(fields.get(key, ""), 10)
        except ValueError:
            value = -1
        if value < 0:
            fail(f"{fault}: recovery diagnostic {key} must be non-negative")
        numbers[key] = value
    if numbers.get("initial_nonempty_segments", 0) < 1:
        fail(f"{fault}: recovery diagnostic did not observe pending journal state")
    if numbers.get("final_nonempty_segments") != 0:
        fail(f"{fault}: recovery diagnostic left nonempty journal segments")
    if fault == "journal_publish" and (
        numbers.get("applied_targets") != 3
        or numbers.get("applied_mutations") != 3
        or numbers.get("already_applied_mutations") != 0
    ):
        fail(f"{fault}: recovery diagnostic mutation counts mismatch")
    elif fault == "metadata_apply" and (
        numbers.get("applied_targets") != 0
        or numbers.get("applied_mutations") != 0
        or numbers.get("already_applied_mutations") != 3
    ):
        fail(f"{fault}: recovery diagnostic metadata-apply counts mismatch")
    elif fault == "checkpoint_copy" and (
        numbers.get("checkpoint_publications") != 1
        or numbers.get("checkpoint_resumes") != 1
    ):
        fail(f"{fault}: recovery diagnostic checkpoint counts mismatch")
    elif fault == "journal_reclaim" and (
        numbers.get("checkpoint_publications") != 0
        or numbers.get("checkpoint_resumes") != 0
        or numbers.get("reclaimed_segments") != 1
    ):
        fail(f"{fault}: recovery diagnostic reclaim counts mismatch")


run_id = run_root.name
if re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}", run_id) is None:
    fail("run directory basename must be the controller-generated run id")

try:
    root_entries = {entry.name: entry for entry in run_root.iterdir()}
except OSError as exc:
    fail(f"cannot enumerate run directory: {exc}")
    root_entries = {}
for name in sorted(set(root_entries) - set(expected_faults)):
    fail(f"unexpected run entry: {name}")
for fault in expected_faults:
    entry = root_entries.get(fault)
    if entry is None:
        fail(f"missing fault directory: {fault}")
    elif entry.is_symlink() or not entry.is_dir():
        fail(f"fault entry is not a real directory: {fault}")

common_arm_identity = None
common_host_identity = None
intervals: list[tuple[datetime, datetime, str]] = []
for fault in expected_faults:
    state_dir = root_entries.get(fault)
    if state_dir is None or state_dir.is_symlink() or not state_dir.is_dir():
        continue
    try:
        state_dir.resolve(strict=True).relative_to(run_root)
    except (OSError, ValueError):
        fail(f"{fault}: state directory resolves outside the run directory")
        continue

    listed = parse_inventory(state_dir, fault)
    recovery_log_name = f"v7-vhdx-{fault}-recovery.log"
    required_files = {
        "arm-context.json",
        "pause.marker",
        "host-controller.json",
        "vhdx-recovery.meta",
        "vhdx-recovery.img",
        recovery_log_name,
        "vhdx-verify.ok",
        "fsck-full-check.stdout",
        "fsck-full-check.stderr",
        "kafsdump.json",
        "kafsdump.stderr",
        "verification-manifest.json",
    }
    for name in sorted(required_files - listed):
        fail(f"{fault}: required retained artifact is missing: {name}")

    arm = load_json(state_dir / "arm-context.json", f"{fault} arm-context.json")
    if arm.get("schema") != "KAFS.V7VhdxArmContext.v1" or arm.get("fault") != fault:
        fail(f"{fault}: arm context schema/fault mismatch")
    arm_created = parse_timestamp(arm.get("created_utc"), f"{fault} arm-context.created_utc")
    arm_distro = require_string(arm, "distro", f"{fault} arm-context")
    arm_kernel = require_string(arm, "kernel", f"{fault} arm-context")
    filesystem_source = require_string(arm, "filesystem_source", f"{fault} arm-context")
    if arm.get("filesystem_type") != "ext4":
        fail(f"{fault}: arm context filesystem_type must be ext4")
    git_head = require_string(arm, "git_head", f"{fault} arm-context")
    if re.fullmatch(r"[0-9a-f]{40}", git_head) is None:
        fail(f"{fault}: arm context git_head must be an exact commit id")
    require_exact_bool(arm, "tracked_worktree_dirty", False, f"{fault} arm-context")
    require_exact_bool(arm, "dedicated_regular_file_image", True, f"{fault} arm-context")
    require_false_claims(arm, f"{fault} arm-context")
    arm_identity = (arm_distro, arm_kernel, filesystem_source, git_head)
    if common_arm_identity is None:
        common_arm_identity = arm_identity
    elif arm_identity != common_arm_identity:
        fail(f"{fault}: WSL/Git arm identity drifted within the run")

    host = load_json(state_dir / "host-controller.json", f"{fault} host-controller.json")
    if host.get("schema") != "KAFS.V7VhdxHostController.v1" or host.get("fault") != fault:
        fail(f"{fault}: host controller schema/fault mismatch")
    if host.get("controller") != "wsl.exe --terminate":
        fail(f"{fault}: host controller is not wsl.exe --terminate evidence")
    require_zero_int(host, "terminate_exit_code", f"{fault} host-controller")
    require_zero_int(host, "restart_exit_code", f"{fault} host-controller")
    host_distro = require_string(host, "distro", f"{fault} host-controller")
    vhdx_path = require_string(host, "vhdx_path", f"{fault} host-controller")
    if vhdx_path and not vhdx_path.lower().endswith(".vhdx"):
        fail(f"{fault}: host-controller.vhdx_path is not a VHDX")
    require_positive_int(host, "vhdx_length_before", f"{fault} host-controller")
    require_positive_int(host, "vhdx_length_after", f"{fault} host-controller")
    marker_time = parse_timestamp(
        host.get("marker_observed_utc"), f"{fault} host-controller.marker_observed_utc"
    )
    terminated_time = parse_timestamp(
        host.get("terminated_utc"), f"{fault} host-controller.terminated_utc"
    )
    restarted_time = parse_timestamp(
        host.get("restarted_utc"), f"{fault} host-controller.restarted_utc"
    )
    if marker_time is not None and terminated_time is not None and marker_time > terminated_time:
        fail(f"{fault}: terminate time precedes marker observation")
    if terminated_time is not None and restarted_time is not None and terminated_time > restarted_time:
        fail(f"{fault}: restart time precedes termination")
    if arm_created is not None and marker_time is not None and arm_created > marker_time:
        fail(f"{fault}: marker observation precedes arm context")
    if marker_time is not None and restarted_time is not None:
        intervals.append((marker_time, restarted_time, fault))
    require_false_claims(host, f"{fault} host-controller")
    if host_distro != arm_distro:
        fail(f"{fault}: host and arm distro identities differ")
    host_identity = (host_distro, vhdx_path)
    if common_host_identity is None:
        common_host_identity = host_identity
    elif host_identity != common_host_identity:
        fail(f"{fault}: host distro/VHDX identity drifted within the run")

    try:
        pause_marker = (state_dir / "pause.marker").read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as exc:
        fail(f"{fault}: cannot read pause.marker: {exc}")
        pause_marker = ""
    if pause_marker != fault:
        fail(f"{fault}: pause.marker does not match the fault")
    try:
        verify_marker = (state_dir / "vhdx-verify.ok").read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as exc:
        fail(f"{fault}: cannot read vhdx-verify.ok: {exc}")
        verify_marker = ""
    if verify_marker != fault:
        fail(f"{fault}: vhdx-verify.ok does not match the fault")

    parse_meta(state_dir / "vhdx-recovery.meta", fault)
    parse_recovery_log(state_dir / recovery_log_name, fault)

    manifest = load_json(
        state_dir / "verification-manifest.json", f"{fault} verification-manifest.json"
    )
    if manifest.get("schema") != "KAFS.V7VhdxRecoveryEvidence.v1" or manifest.get("fault") != fault:
        fail(f"{fault}: verification manifest schema/fault mismatch")
    verified_time = parse_timestamp(
        manifest.get("verified_utc"), f"{fault} verification-manifest.verified_utc"
    )
    if restarted_time is not None and verified_time is not None and verified_time < restarted_time:
        fail(f"{fault}: verification completed before WSL restart")
    for key in (
        "host_terminate_restart_observed",
        "fsck_full_check_passed",
        "kafsdump_completed",
        "payload_and_recovery_diagnostic_passed",
        "dedicated_regular_file_image",
    ):
        require_exact_bool(manifest, key, True, f"{fault} verification-manifest")
    require_false_claims(manifest, f"{fault} verification-manifest")
    image_digest = manifest.get("image_sha256_after_recovery")
    if not isinstance(image_digest, str) or re.fullmatch(r"[0-9a-f]{64}", image_digest) is None:
        fail(f"{fault}: verification manifest image digest is invalid")
    else:
        try:
            if sha256_path(state_dir / "vhdx-recovery.img") != image_digest:
                fail(f"{fault}: recovered image digest does not match the manifest")
        except OSError as exc:
            fail(f"{fault}: cannot hash recovered image: {exc}")

    dump = load_json(state_dir / "kafsdump.json", f"{fault} kafsdump.json")
    superblock = dump.get("superblock")
    layout = dump.get("layout_descriptor")
    journal = dump.get("journal_segments")
    if not isinstance(superblock, dict) or superblock.get("format_version") != 7:
        fail(f"{fault}: kafsdump does not report format v7")
    if not isinstance(layout, dict) or (
        layout.get("status") != "ok"
        or layout.get("available") is not True
        or layout.get("selected") is not True
    ):
        fail(f"{fault}: kafsdump layout descriptor is not selected and valid")
    if not isinstance(journal, dict) or (
        journal.get("status") != "ok"
        or journal.get("selected") is not True
        or journal.get("nonempty_segment_count") != 0
    ):
        fail(f"{fault}: kafsdump journal is not clean and selected")

intervals.sort()
for previous, current in zip(intervals, intervals[1:]):
    if previous[1] > current[0]:
        fail(f"fault intervals overlap: {previous[2]} and {current[2]}")

if errors:
    for error in errors:
        print(f"FAIL: {error}", file=sys.stderr)
    print(f"v7 VHDX evidence audit FAIL ({len(errors)} failures)", file=sys.stderr)
    raise SystemExit(1)

print(f"KAFS_V7_VHDX_EVIDENCE_AUDIT PASS run_id={run_id}")
print("faults: " + ",".join(expected_faults))
if common_host_identity is not None:
    print(f"distro: {common_host_identity[0]}")
    print(f"vhdx_path: {common_host_identity[1]}")
if common_arm_identity is not None:
    print(f"git_head: {common_arm_identity[3]}")
print("NOTE: audit validation does not access a VHDX or qualify real media")
PY
