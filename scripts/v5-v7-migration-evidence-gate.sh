#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/v5-v7-migration-evidence-gate.sh \
    --evidence-dir DIR \
    --require-decision ACCEPT|RESUME_REQUIRED|ROLLBACK \
    --validate-only

Validate one versioned v5-to-v7 migration lifecycle evidence bundle. The gate
only reads retained JSON and SHA-256 inventory files. It never opens or writes
a KAFS image, mounts a filesystem, imports data, or authorizes cutover.
Prefix a path value that begins with '-' with './'.
EOF
}

usage_error() {
  echo "migration evidence gate: $*" >&2
  usage >&2
  exit 2
}

require_option_value() {
  local option=$1
  [[ $# -ge 2 ]] || usage_error "missing value for $option"
  [[ -n "$2" && "$2" != -* ]] || usage_error "missing value for $option"
}

EVIDENCE_DIR=""
REQUIRED_DECISION=""
VALIDATE_ONLY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --evidence-dir)
      require_option_value "$@"
      EVIDENCE_DIR="$2"
      shift 2
      ;;
    --require-decision)
      require_option_value "$@"
      REQUIRED_DECISION="$2"
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
      echo "migration evidence gate: unknown option: $1" >&2
      exit 2
      ;;
  esac
done

[[ "$VALIDATE_ONLY" -eq 1 ]] || { usage >&2; exit 2; }
case "$REQUIRED_DECISION" in
  ACCEPT|RESUME_REQUIRED|ROLLBACK) ;;
  *)
    echo "migration evidence gate: --require-decision must be ACCEPT, RESUME_REQUIRED, or ROLLBACK" >&2
    exit 2
    ;;
esac
[[ -n "$EVIDENCE_DIR" && -d "$EVIDENCE_DIR" && ! -L "$EVIDENCE_DIR" ]] || {
  echo "migration evidence gate: evidence directory is invalid: ${EVIDENCE_DIR:-<missing>}" >&2
  exit 2
}
command -v python3 >/dev/null 2>&1 || {
  echo "migration evidence gate: python3 is required" >&2
  exit 2
}

python3 - "$EVIDENCE_DIR" "$REQUIRED_DECISION" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys


root = Path(sys.argv[1]).resolve()
required_decision = sys.argv[2]
required_files = {
    "plan.json",
    "source.json",
    "ledger.json",
    "destination.json",
    "decision.json",
}
false_claims = (
    "production_cutover_authorized",
    "rc_eligible",
    "real_media_qualified",
    "physical_media_qualified",
    "release_candidate_qualified",
)
allowed_transitions = (
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
)
errors: list[str] = []
now = datetime.now(timezone.utc)
u16_max = (1 << 16) - 1
u32_max = (1 << 32) - 1
u64_max = (1 << 64) - 1


def fail(message: str) -> None:
    errors.append(message)


def require_keys(obj: dict, expected: set[str], prefix: str) -> None:
    actual = set(obj)
    for key in sorted(expected - actual):
        fail(f"{prefix} is missing field: {key}")
    for key in sorted(actual - expected):
        fail(f"{prefix} has unexpected field: {key}")


def load_json(name: str) -> tuple[dict, bytes]:
    path = root / name
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read {name}: {exc}")
        return {}, b""
    if not isinstance(value, dict):
        fail(f"{name} must contain a JSON object")
        return {}, raw
    return value, raw


def digest_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def require_string(obj: dict, key: str, prefix: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value.strip():
        fail(f"{prefix}.{key} must be a non-empty string")
        return ""
    return value


def require_digest_value(value: object, label: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        fail(f"{label} must be a lowercase SHA-256 digest")
        return ""
    return value


def require_digest(obj: dict, key: str, prefix: str) -> str:
    return require_digest_value(obj.get(key), f"{prefix}.{key}")


def require_int(value: object, label: str, minimum: int = 0, maximum: int | None = None) -> int:
    if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
        bound = f" between {minimum} and {maximum}" if maximum is not None else f" >= {minimum}"
        fail(f"{label} must be an integer{bound}")
        return 0
    return value


def require_false_claims(obj: object, prefix: str) -> None:
    if not isinstance(obj, dict):
        fail(f"{prefix} must be an object")
        return
    require_keys(obj, set(false_claims), prefix)
    for key in false_claims:
        if obj.get(key) is not False:
            fail(f"{prefix}.{key} must be false")


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
        fail(f"{label} must be an ISO-8601 timestamp with timezone")
        return None
    if parsed > now:
        fail(f"{label} must not be in the future")
    return parsed


def validate_bundle_files() -> None:
    inventory = root / "artifacts.sha256"
    try:
        children = list(root.iterdir())
    except OSError as exc:
        fail(f"cannot enumerate evidence directory: {exc}")
        return
    actual_files: set[str] = set()
    for path in children:
        if path.is_symlink():
            fail(f"symbolic links are not allowed: {path.name}")
        elif path.is_file():
            actual_files.add(path.name)
        else:
            fail(f"unexpected non-file evidence entry: {path.name}")
    expected_all = required_files | {"artifacts.sha256"}
    for name in sorted(actual_files - expected_all):
        fail(f"unexpected evidence file: {name}")
    for name in sorted(expected_all - actual_files):
        fail(f"required evidence file is missing: {name}")
    if not inventory.is_file() or inventory.is_symlink():
        return
    try:
        lines = inventory.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        fail(f"cannot read artifacts.sha256: {exc}")
        return
    listed: dict[str, str] = {}
    for line_number, line in enumerate(lines, 1):
        match = re.fullmatch(r"([0-9a-f]{64})  \./([^/\x00]+)", line)
        if match is None:
            fail(f"malformed artifacts.sha256 line {line_number}")
            continue
        digest, name = match.groups()
        if name in listed:
            fail(f"duplicate artifact inventory entry: {name}")
        elif name == "artifacts.sha256":
            fail("artifacts.sha256 must not list itself")
        else:
            listed[name] = digest
    if set(listed) != required_files:
        for name in sorted(required_files - set(listed)):
            fail(f"artifact inventory is missing: {name}")
        for name in sorted(set(listed) - required_files):
            fail(f"artifact inventory has unexpected file: {name}")
    for name, expected in listed.items():
        path = root / name
        if not path.is_file() or path.is_symlink():
            continue
        try:
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError as exc:
            fail(f"cannot hash {name}: {exc}")
            continue
        if actual != expected:
            fail(f"artifact digest mismatch: {name}")


def normalized_path(value: object, label: str) -> str:
    if value == ".":
        return "."
    if not isinstance(value, str) or not value or "\x00" in value:
        fail(f"{label} must be a non-empty relative POSIX path")
        return ""
    pure = PurePosixPath(value)
    if pure.is_absolute() or any(part in ("", ".", "..") for part in pure.parts) or str(pure) != value:
        fail(f"{label} must be a normalized relative POSIX path")
        return ""
    return value


def validate_objects(value: object, prefix: str) -> tuple[list[dict], dict[str, dict]]:
    if not isinstance(value, list) or not value:
        fail(f"{prefix} must be a non-empty array")
        return [], {}
    objects: list[dict] = []
    by_id: dict[str, dict] = {}
    previous = ""
    expected_keys = {
        "object_id", "type", "permissions", "uid", "gid", "atime_ns", "mtime_ns",
        "size_bytes", "link_count", "payload_sha256",
    }
    for index, item in enumerate(value):
        label = f"{prefix}[{index}]"
        if not isinstance(item, dict):
            fail(f"{label} must be an object")
            continue
        require_keys(item, expected_keys, label)
        object_id = require_string(item, "object_id", label)
        if object_id and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", object_id) is None:
            fail(f"{label}.object_id is invalid")
        if object_id <= previous:
            fail(f"{prefix} must be strictly sorted by object_id")
        previous = object_id
        if object_id in by_id:
            fail(f"duplicate object_id: {object_id}")
        kind = item.get("type")
        if kind not in ("directory", "regular", "symlink"):
            fail(f"{label}.type is unsupported")
        permissions = item.get("permissions")
        if not isinstance(permissions, str) or re.fullmatch(r"[0-7]{4}", permissions) is None:
            fail(f"{label}.permissions must be four octal digits")
        require_int(item.get("uid"), f"{label}.uid", 0, u16_max)
        require_int(item.get("gid"), f"{label}.gid", 0, u16_max)
        require_int(item.get("atime_ns"), f"{label}.atime_ns", 0, u64_max)
        require_int(item.get("mtime_ns"), f"{label}.mtime_ns", 0, u64_max)
        size = require_int(item.get("size_bytes"), f"{label}.size_bytes", 0, u64_max)
        links = require_int(item.get("link_count"), f"{label}.link_count", 1, u16_max)
        payload = item.get("payload_sha256")
        if kind == "directory":
            if size != 0 or payload is not None or links != 1:
                fail(f"{label} directory must have size zero, null payload, and one namespace link")
        else:
            require_digest_value(payload, f"{label}.payload_sha256")
            if kind == "symlink" and (size == 0 or links != 1):
                fail(f"{label} symlink must be non-empty and have one namespace link")
        objects.append(item)
        if object_id:
            by_id[object_id] = item
    return objects, by_id


def validate_entries(value: object, objects: dict[str, dict], prefix: str) -> list[dict]:
    if not isinstance(value, list) or not value:
        fail(f"{prefix} must be a non-empty array")
        return []
    entries: list[dict] = []
    by_path: dict[str, dict] = {}
    previous = ""
    refs: dict[str, int] = {object_id: 0 for object_id in objects}
    for index, item in enumerate(value):
        label = f"{prefix}[{index}]"
        if not isinstance(item, dict):
            fail(f"{label} must be an object")
            continue
        require_keys(item, {"path", "object_id"}, label)
        path = normalized_path(item.get("path"), f"{label}.path")
        object_id = require_string(item, "object_id", label)
        if path <= previous:
            fail(f"{prefix} must be strictly sorted by path")
        previous = path
        if path in by_path:
            fail(f"duplicate namespace path: {path}")
        if object_id not in objects:
            fail(f"{label}.object_id does not reference an object")
        else:
            refs[object_id] += 1
        entries.append(item)
        if path:
            by_path[path] = item
    root_entry = by_path.get(".")
    if root_entry is None or objects.get(root_entry.get("object_id"), {}).get("type") != "directory":
        fail(f"{prefix} must contain a directory root entry at '.'")
    for path, item in by_path.items():
        if path == ".":
            continue
        parent = str(PurePosixPath(path).parent)
        parent_entry = by_path.get(parent)
        if parent_entry is None:
            fail(f"namespace parent is missing for {path}: {parent}")
        elif objects.get(parent_entry.get("object_id"), {}).get("type") != "directory":
            fail(f"namespace parent is not a directory for {path}: {parent}")
    for object_id, obj in objects.items():
        count = refs.get(object_id, 0)
        if count == 0:
            fail(f"unreferenced inventory object: {object_id}")
        if count != obj.get("link_count"):
            fail(f"object link_count does not match namespace references: {object_id}")
        if obj.get("type") != "regular" and count != 1:
            fail(f"only regular objects may have multiple namespace paths: {object_id}")
    return entries


def object_semantics(objects: list[dict]) -> dict[str, dict]:
    return {item["object_id"]: item for item in objects if isinstance(item.get("object_id"), str)}


validate_bundle_files()
plan, plan_raw = load_json("plan.json")
source, source_raw = load_json("source.json")
ledger, ledger_raw = load_json("ledger.json")
destination, destination_raw = load_json("destination.json")
decision, _ = load_json("decision.json")
plan_digest = digest_bytes(plan_raw)
source_digest = digest_bytes(source_raw)
ledger_digest = digest_bytes(ledger_raw)
destination_digest = digest_bytes(destination_raw)

require_keys(plan, {"schema", "migration_id", "created_at_utc", "source", "destination", "policies", "claims"}, "plan")
if plan.get("schema") != "KAFS.V5V7MigrationPlan.v1":
    fail("plan.schema must be KAFS.V5V7MigrationPlan.v1")
migration_id = require_string(plan, "migration_id", "plan")
if migration_id and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", migration_id) is None:
    fail("plan.migration_id is invalid")
plan_created = parse_timestamp(plan.get("created_at_utc"), "plan.created_at_utc")
plan_source = plan.get("source")
if not isinstance(plan_source, dict):
    fail("plan.source must be an object")
    plan_source = {}
require_keys(plan_source, {"image_id", "format_version", "image_size_bytes", "image_sha256", "fsck_full_check_passed"}, "plan.source")
source_image_id = require_string(plan_source, "image_id", "plan.source")
if plan_source.get("format_version") != 5:
    fail("plan.source.format_version must be 5")
require_int(plan_source.get("image_size_bytes"), "plan.source.image_size_bytes", 1, u64_max)
require_digest(plan_source, "image_sha256", "plan.source")
if plan_source.get("fsck_full_check_passed") is not True:
    fail("plan.source.fsck_full_check_passed must be true")
plan_destination = plan.get("destination")
if not isinstance(plan_destination, dict):
    fail("plan.destination must be an object")
    plan_destination = {}
require_keys(plan_destination, {"image_id", "format_version", "image_size_bytes", "inode_count", "group_count", "block_size"}, "plan.destination")
destination_image_id = require_string(plan_destination, "image_id", "plan.destination")
if plan_destination.get("format_version") != 7:
    fail("plan.destination.format_version must be 7")
require_int(plan_destination.get("image_size_bytes"), "plan.destination.image_size_bytes", 1, u64_max)
require_int(plan_destination.get("inode_count"), "plan.destination.inode_count", 2, u32_max)
group_count = require_int(plan_destination.get("group_count"), "plan.destination.group_count", 1, u32_max)
if group_count and group_count & (group_count - 1):
    fail("plan.destination.group_count must be a power of two")
block_size = require_int(plan_destination.get("block_size"), "plan.destination.block_size", 4096, u32_max)
if block_size and block_size & (block_size - 1):
    fail("plan.destination.block_size must be a power of two")
policies = plan.get("policies")
if not isinstance(policies, dict):
    fail("plan.policies must be an object")
    policies = {}
require_keys(policies, {"source_write_frozen", "preserve_types", "preserve_metadata", "special_file_policy", "sparse_file_policy", "partial_destination_policy", "rollback_policy", "idempotence_policy", "phase_transitions"}, "plan.policies")
if policies.get("source_write_frozen") is not True:
    fail("plan.policies.source_write_frozen must be true")
if policies.get("preserve_types") != ["directory", "regular", "symlink"]:
    fail("plan.policies.preserve_types must be directory, regular, symlink")
if policies.get("preserve_metadata") != ["permissions", "uid", "gid", "atime_ns", "mtime_ns", "hardlinks"]:
    fail("plan.policies.preserve_metadata is incomplete or out of order")
expected_policies = {
    "special_file_policy": "REJECT_BEFORE_DESTINATION_WRITE",
    "sparse_file_policy": "REJECT_BEFORE_DESTINATION_WRITE",
    "partial_destination_policy": "NEVER_ACCEPT",
    "rollback_policy": "PRESERVE_FAILED_DESTINATION_AND_SELECT_UNCHANGED_SOURCE",
    "idempotence_policy": "SAME_BINDINGS_SAME_SEMANTIC_INVENTORY",
}
for key, expected in expected_policies.items():
    if policies.get(key) != expected:
        fail(f"plan.policies.{key} must be {expected}")
if policies.get("phase_transitions") != list(allowed_transitions):
    fail("plan.policies.phase_transitions does not match the v1 lifecycle")
require_false_claims(plan.get("claims"), "plan.claims")

require_keys(source, {"schema", "migration_id", "captured_at_utc", "plan_sha256", "image", "objects", "entries", "claims"}, "source")
if source.get("schema") != "KAFS.V5MigrationSourceInventory.v1":
    fail("source.schema must be KAFS.V5MigrationSourceInventory.v1")
if source.get("migration_id") != migration_id:
    fail("source.migration_id does not match plan")
source_captured = parse_timestamp(source.get("captured_at_utc"), "source.captured_at_utc")
if plan_created is not None and source_captured is not None and source_captured < plan_created:
    fail("source capture precedes plan creation")
if require_digest(source, "plan_sha256", "source") != plan_digest:
    fail("source.plan_sha256 does not match plan.json")
source_image = source.get("image")
if not isinstance(source_image, dict):
    fail("source.image must be an object")
    source_image = {}
require_keys(source_image, {"image_id", "format_version", "image_size_bytes", "image_sha256", "fsck_full_check_passed"}, "source.image")
if source_image != plan_source:
    fail("source.image does not exactly match plan.source")
source_objects, source_by_id = validate_objects(source.get("objects"), "source.objects")
source_entries = validate_entries(source.get("entries"), source_by_id, "source.entries")
if type(plan_destination.get("inode_count")) is int and plan_destination["inode_count"] < len(source_objects):
    fail("plan.destination.inode_count cannot hold the source object inventory")
require_false_claims(source.get("claims"), "source.claims")

require_keys(ledger, {"schema", "migration_id", "plan_sha256", "source_sha256", "attempt", "source_image_sha256_before", "source_image_sha256_after", "destination_image_id", "phase_history", "objects", "state", "updated_at_utc", "claims"}, "ledger")
if ledger.get("schema") != "KAFS.V5V7MigrationCopyLedger.v1":
    fail("ledger.schema must be KAFS.V5V7MigrationCopyLedger.v1")
if ledger.get("migration_id") != migration_id:
    fail("ledger.migration_id does not match plan")
if require_digest(ledger, "plan_sha256", "ledger") != plan_digest:
    fail("ledger.plan_sha256 does not match plan.json")
if require_digest(ledger, "source_sha256", "ledger") != source_digest:
    fail("ledger.source_sha256 does not match source.json")
attempt = require_int(ledger.get("attempt"), "ledger.attempt", 1, u32_max)
source_before = require_digest(ledger, "source_image_sha256_before", "ledger")
source_after = require_digest(ledger, "source_image_sha256_after", "ledger")
if source_before != plan_source.get("image_sha256") or source_after != source_before:
    fail("ledger source image digests do not prove source immutability")
if ledger.get("destination_image_id") != destination_image_id:
    fail("ledger.destination_image_id does not match plan")
history = ledger.get("phase_history")
history_phases: list[str] = []
history_attempts: list[int] = []
last_time = None
last_attempt = 0
if not isinstance(history, list) or not history:
    fail("ledger.phase_history must be a non-empty array")
    history = []
for index, phase_record in enumerate(history):
    label = f"ledger.phase_history[{index}]"
    if not isinstance(phase_record, dict):
        fail(f"{label} must be an object")
        continue
    require_keys(phase_record, {"phase", "at_utc", "attempt"}, label)
    phase = require_string(phase_record, "phase", label)
    phase_attempt = require_int(phase_record.get("attempt"), f"{label}.attempt", 1, u32_max)
    phase_time = parse_timestamp(phase_record.get("at_utc"), f"{label}.at_utc")
    if last_time is not None and phase_time is not None and phase_time < last_time:
        fail("ledger.phase_history timestamps are not ordered")
    if phase_attempt < last_attempt or phase_attempt > last_attempt + 1:
        fail("ledger.phase_history attempts are not monotonic")
    if index == 0 and (phase != "PLANNED" or phase_attempt != 1):
        fail("ledger.phase_history must start with PLANNED attempt 1")
    history_phases.append(phase)
    history_attempts.append(phase_attempt)
    last_time = phase_time if phase_time is not None else last_time
    last_attempt = phase_attempt
for index, (previous, current) in enumerate(zip(history_phases, history_phases[1:])):
    transition = f"{previous}->{current}"
    if transition not in allowed_transitions:
        fail(f"illegal migration phase transition: {transition}")
    previous_attempt = history_attempts[index]
    current_attempt = history_attempts[index + 1]
    if transition == "RESUME_REQUIRED->COPYING":
        if current_attempt != previous_attempt + 1:
            fail("resume transition must increment the attempt exactly once")
    elif current_attempt != previous_attempt:
        fail(f"attempt changed outside resume transition: {transition}")
if last_attempt != attempt:
    fail("ledger.attempt does not match phase_history")
ledger_updated = parse_timestamp(ledger.get("updated_at_utc"), "ledger.updated_at_utc")
if last_time is not None and ledger_updated is not None and ledger_updated < last_time:
    fail("ledger.updated_at_utc precedes the last phase")
ledger_state = ledger.get("state")
if ledger_state not in ("COMPLETE", "INTERRUPTED", "ROLLED_BACK"):
    fail("ledger.state is invalid")
ledger_objects = ledger.get("objects")
ledger_by_id: dict[str, dict] = {}
if not isinstance(ledger_objects, list):
    fail("ledger.objects must be an array")
    ledger_objects = []
previous_object_id = ""
for index, item in enumerate(ledger_objects):
    label = f"ledger.objects[{index}]"
    if not isinstance(item, dict):
        fail(f"{label} must be an object")
        continue
    require_keys(item, {"object_id", "status", "bytes_total", "bytes_copied", "payload_sha256"}, label)
    object_id = require_string(item, "object_id", label)
    if object_id <= previous_object_id:
        fail("ledger.objects must be strictly sorted by object_id")
    previous_object_id = object_id
    source_object = source_by_id.get(object_id)
    if source_object is None:
        fail(f"ledger object is not present in source: {object_id}")
        continue
    status = item.get("status")
    if status not in ("PENDING", "IN_PROGRESS", "COMPLETE", "ROLLED_BACK"):
        fail(f"{label}.status is invalid")
    total = require_int(item.get("bytes_total"), f"{label}.bytes_total", 0, u64_max)
    copied = require_int(item.get("bytes_copied"), f"{label}.bytes_copied", 0, u64_max)
    if total != source_object.get("size_bytes"):
        fail(f"{label}.bytes_total does not match the source object")
    if copied > total:
        fail(f"{label}.bytes_copied exceeds bytes_total")
    if status == "PENDING" and copied != 0:
        fail(f"{label} pending object has copied bytes")
    if status == "IN_PROGRESS" and (total == 0 or copied >= total):
        fail(f"{label} in-progress object must be partially copied")
    if status == "COMPLETE" and copied != total:
        fail(f"{label} complete object bytes are incomplete")
    if item.get("payload_sha256") != source_object.get("payload_sha256"):
        fail(f"{label}.payload_sha256 does not match the source object")
    if object_id in ledger_by_id:
        fail(f"duplicate ledger object: {object_id}")
    ledger_by_id[object_id] = item
if set(ledger_by_id) != set(source_by_id):
    for object_id in sorted(set(source_by_id) - set(ledger_by_id)):
        fail(f"source object is missing from ledger: {object_id}")
require_false_claims(ledger.get("claims"), "ledger.claims")

require_keys(destination, {"schema", "migration_id", "captured_at_utc", "plan_sha256", "source_sha256", "ledger_sha256", "image", "state", "objects", "entries", "fsck_full_check_passed", "kafsdump_completed", "admission_ready", "claims"}, "destination")
if destination.get("schema") != "KAFS.V7MigrationDestinationInventory.v1":
    fail("destination.schema must be KAFS.V7MigrationDestinationInventory.v1")
if destination.get("migration_id") != migration_id:
    fail("destination.migration_id does not match plan")
destination_captured = parse_timestamp(destination.get("captured_at_utc"), "destination.captured_at_utc")
if ledger_updated is not None and destination_captured is not None and destination_captured < ledger_updated:
    fail("destination capture precedes ledger update")
if require_digest(destination, "plan_sha256", "destination") != plan_digest:
    fail("destination.plan_sha256 does not match plan.json")
if require_digest(destination, "source_sha256", "destination") != source_digest:
    fail("destination.source_sha256 does not match source.json")
if require_digest(destination, "ledger_sha256", "destination") != ledger_digest:
    fail("destination.ledger_sha256 does not match ledger.json")
destination_image = destination.get("image")
if not isinstance(destination_image, dict):
    fail("destination.image must be an object")
    destination_image = {}
require_keys(destination_image, {"image_id", "format_version", "image_size_bytes", "inode_count", "group_count", "block_size", "image_sha256"}, "destination.image")
for key in ("image_id", "format_version", "image_size_bytes", "inode_count", "group_count", "block_size"):
    if destination_image.get(key) != plan_destination.get(key):
        fail(f"destination.image.{key} does not match plan.destination")
require_digest(destination_image, "image_sha256", "destination.image")
destination_state = destination.get("state")
if destination_state not in ("COMPLETE", "PARTIAL", "PRESERVED_FAILED"):
    fail("destination.state is invalid")
destination_objects, destination_by_id = validate_objects(destination.get("objects"), "destination.objects")
destination_entries = validate_entries(destination.get("entries"), destination_by_id, "destination.entries")
source_semantics = object_semantics(source_objects)
destination_semantics = object_semantics(destination_objects)
for object_id, item in destination_semantics.items():
    if source_semantics.get(object_id) != item:
        fail(f"destination object differs from source semantics: {object_id}")
source_entry_map = {item.get("path"): item for item in source_entries}
for item in destination_entries:
    if source_entry_map.get(item.get("path")) != item:
        fail(f"destination namespace entry differs from source: {item.get('path')}")
complete_ledger_ids = {object_id for object_id, item in ledger_by_id.items() if item.get("status") == "COMPLETE"}
if destination_state == "PARTIAL" and set(destination_by_id) != complete_ledger_ids:
    fail("partial destination objects do not exactly match completed ledger objects")
for key in ("fsck_full_check_passed", "kafsdump_completed", "admission_ready"):
    if not isinstance(destination.get(key), bool):
        fail(f"destination.{key} must be a boolean")
require_false_claims(destination.get("claims"), "destination.claims")

require_keys(decision, {"schema", "migration_id", "operator_id", "decided_at_utc", "decision", "bindings", "source_immutable", "destination_complete", "resume_from_object_id", "rollback_source_selected", "failure_reason", "claims", "confirmation"}, "decision")
if decision.get("schema") != "KAFS.V5V7MigrationDecision.v1":
    fail("decision.schema must be KAFS.V5V7MigrationDecision.v1")
if decision.get("migration_id") != migration_id:
    fail("decision.migration_id does not match plan")
require_string(decision, "operator_id", "decision")
decided_at = parse_timestamp(decision.get("decided_at_utc"), "decision.decided_at_utc")
if destination_captured is not None and decided_at is not None and decided_at < destination_captured:
    fail("decision time precedes destination capture")
actual_decision = decision.get("decision")
if actual_decision != required_decision:
    fail(f"decision.decision must match required decision {required_decision}")
bindings = decision.get("bindings")
if not isinstance(bindings, dict):
    fail("decision.bindings must be an object")
    bindings = {}
require_keys(bindings, {"plan_sha256", "source_sha256", "ledger_sha256", "destination_sha256"}, "decision.bindings")
binding_values = {
    "plan_sha256": plan_digest,
    "source_sha256": source_digest,
    "ledger_sha256": ledger_digest,
    "destination_sha256": destination_digest,
}
for key, expected in binding_values.items():
    if require_digest(bindings, key, "decision.bindings") != expected:
        fail(f"decision.bindings.{key} does not match retained bytes")
if decision.get("source_immutable") is not True:
    fail("decision.source_immutable must be true")
if not isinstance(decision.get("destination_complete"), bool):
    fail("decision.destination_complete must be a boolean")
if not isinstance(decision.get("rollback_source_selected"), bool):
    fail("decision.rollback_source_selected must be a boolean")
failure_reason = decision.get("failure_reason")
if not isinstance(failure_reason, str):
    fail("decision.failure_reason must be a string")
require_false_claims(decision.get("claims"), "decision.claims")
expected_confirmation = f"MIGRATION T59A {migration_id} {actual_decision}"
if decision.get("confirmation") != expected_confirmation:
    fail("decision.confirmation does not match the exact migration decision")

noncomplete = [item for item in ledger_objects if isinstance(item, dict) and item.get("status") != "COMPLETE"]
first_noncomplete = noncomplete[0].get("object_id") if noncomplete else None
if actual_decision == "ACCEPT":
    if ledger_state != "COMPLETE" or history_phases[-1:] != ["ACCEPTED"]:
        fail("ACCEPT requires a COMPLETE ledger ending at ACCEPTED")
    if noncomplete:
        fail("ACCEPT requires every ledger object to be complete")
    if destination_state != "COMPLETE" or set(destination_by_id) != set(source_by_id):
        fail("ACCEPT requires a complete destination object inventory")
    if destination_entries != source_entries:
        fail("ACCEPT requires exact destination namespace equivalence")
    if any(destination.get(key) is not True for key in ("fsck_full_check_passed", "kafsdump_completed", "admission_ready")):
        fail("ACCEPT requires fsck, kafsdump, and admission-ready PASS")
    if decision.get("destination_complete") is not True:
        fail("ACCEPT requires destination_complete true")
    if decision.get("resume_from_object_id") is not None:
        fail("ACCEPT must not name a resume object")
    if decision.get("rollback_source_selected") is not False or failure_reason != "":
        fail("ACCEPT must not select rollback or record a failure reason")
elif actual_decision == "RESUME_REQUIRED":
    if ledger_state != "INTERRUPTED" or history_phases[-1:] != ["RESUME_REQUIRED"]:
        fail("RESUME_REQUIRED requires an INTERRUPTED ledger ending at RESUME_REQUIRED")
    if not noncomplete:
        fail("RESUME_REQUIRED requires incomplete ledger work")
    if destination_state != "PARTIAL":
        fail("RESUME_REQUIRED requires a PARTIAL destination")
    if any(destination.get(key) is not False for key in ("fsck_full_check_passed", "kafsdump_completed", "admission_ready")):
        fail("RESUME_REQUIRED destination must not claim validation or admission")
    if decision.get("destination_complete") is not False:
        fail("RESUME_REQUIRED requires destination_complete false")
    if decision.get("resume_from_object_id") != first_noncomplete:
        fail("RESUME_REQUIRED must name the first incomplete ledger object")
    if decision.get("rollback_source_selected") is not False or not failure_reason:
        fail("RESUME_REQUIRED requires a reason and must not select rollback")
elif actual_decision == "ROLLBACK":
    if ledger_state != "ROLLED_BACK" or history_phases[-1:] != ["ROLLED_BACK"]:
        fail("ROLLBACK requires a ROLLED_BACK ledger ending at ROLLED_BACK")
    if destination_state != "PRESERVED_FAILED":
        fail("ROLLBACK requires a PRESERVED_FAILED destination")
    if destination.get("admission_ready") is not False:
        fail("ROLLBACK destination must not be admission ready")
    if decision.get("destination_complete") is not False:
        fail("ROLLBACK requires destination_complete false")
    if decision.get("resume_from_object_id") is not None:
        fail("ROLLBACK must not name a resume object")
    if decision.get("rollback_source_selected") is not True or not failure_reason:
        fail("ROLLBACK must select the unchanged source and record a reason")

if errors:
    for error in errors:
        print(f"FAIL: {error}", file=sys.stderr)
    print(f"v5-to-v7 migration evidence FAIL ({len(errors)} failures)", file=sys.stderr)
    raise SystemExit(1)

print(
    f"KAFS_V5_V7_MIGRATION_EVIDENCE PASS migration_id={migration_id} "
    f"decision={actual_decision}"
)
print(f"source_objects: {len(source_objects)}")
print(f"source_entries: {len(source_entries)}")
print(f"attempt: {attempt}")
print("NOTE: contract validation does not import data or authorize production cutover")
PY
