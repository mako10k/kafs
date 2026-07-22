#!/usr/bin/env bash
set -euo pipefail

gate=${KAFS_TEST_V5_V7_MIGRATION_EVIDENCE_GATE:-../scripts/v5-v7-migration-evidence-gate.sh}
[[ -x "$gate" ]] || {
  echo "v5-to-v7 migration evidence gate is not executable: $gate" >&2
  exit 1
}

workdir=$(mktemp -d "${TMPDIR:-/tmp}/kafs-v5-v7-migration-evidence.XXXXXX")
cleanup() {
  find "$workdir" -depth -delete
}
trap cleanup EXIT

bundle="$workdir/accepted"
mkdir "$bundle"

python3 - "$bundle" <<'PY'
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys


root = Path(sys.argv[1])
base = datetime(2026, 7, 21, tzinfo=timezone.utc)


def timestamp(minutes):
    return (base + timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")


def payload(value):
    return hashlib.sha256(value).hexdigest()


claims = {
    "production_cutover_authorized": False,
    "rc_eligible": False,
    "real_media_qualified": False,
    "physical_media_qualified": False,
    "release_candidate_qualified": False,
}
source_image_sha = payload(b"synthetic immutable v5 source image")
plan = {
    "schema": "KAFS.V5V7MigrationPlan.v1",
    "migration_id": "T59A-SYNTHETIC-001",
    "created_at_utc": timestamp(0),
    "source": {
        "image_id": "source-v5.img",
        "format_version": 5,
        "image_size_bytes": 64 * 1024 * 1024,
        "image_sha256": source_image_sha,
        "fsck_full_check_passed": True,
    },
    "destination": {
        "image_id": "destination-v7.img",
        "format_version": 7,
        "image_size_bytes": 128 * 1024 * 1024,
        "inode_count": 4096,
        "group_count": 4,
        "block_size": 4096,
    },
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
        "phase_transitions": [
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
        ],
    },
    "claims": claims,
}
objects = [
    {
        "object_id": "obj-dir",
        "type": "directory",
        "permissions": "0750",
        "uid": 1000,
        "gid": 1000,
        "atime_ns": 1000000001,
        "mtime_ns": 1000000002,
        "size_bytes": 0,
        "link_count": 1,
        "payload_sha256": None,
    },
    {
        "object_id": "obj-empty",
        "type": "regular",
        "permissions": "0640",
        "uid": 1000,
        "gid": 1000,
        "atime_ns": 1000000003,
        "mtime_ns": 1000000004,
        "size_bytes": 0,
        "link_count": 1,
        "payload_sha256": payload(b""),
    },
    {
        "object_id": "obj-file",
        "type": "regular",
        "permissions": "0644",
        "uid": 1000,
        "gid": 1000,
        "atime_ns": 1000000005,
        "mtime_ns": 1000000006,
        "size_bytes": 13,
        "link_count": 1,
        "payload_sha256": payload(b"alpha payload"),
    },
    {
        "object_id": "obj-hard",
        "type": "regular",
        "permissions": "0600",
        "uid": 1001,
        "gid": 1002,
        "atime_ns": 1000000007,
        "mtime_ns": 1000000008,
        "size_bytes": 5,
        "link_count": 2,
        "payload_sha256": payload(b"hard\n"),
    },
    {
        "object_id": "obj-link",
        "type": "symlink",
        "permissions": "0777",
        "uid": 1000,
        "gid": 1000,
        "atime_ns": 1000000009,
        "mtime_ns": 1000000010,
        "size_bytes": 5,
        "link_count": 1,
        "payload_sha256": payload(b"alpha"),
    },
    {
        "object_id": "obj-root",
        "type": "directory",
        "permissions": "0755",
        "uid": 0,
        "gid": 0,
        "atime_ns": 1000000011,
        "mtime_ns": 1000000012,
        "size_bytes": 0,
        "link_count": 1,
        "payload_sha256": None,
    },
]
entries = [
    {"path": ".", "object_id": "obj-root"},
    {"path": "alpha", "object_id": "obj-file"},
    {"path": "dir", "object_id": "obj-dir"},
    {"path": "dir/empty", "object_id": "obj-empty"},
    {"path": "hard-a", "object_id": "obj-hard"},
    {"path": "hard-b", "object_id": "obj-hard"},
    {"path": "link", "object_id": "obj-link"},
]
(root / "plan.json").write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
plan_sha = hashlib.sha256((root / "plan.json").read_bytes()).hexdigest()
source = {
    "schema": "KAFS.V5MigrationSourceInventory.v1",
    "migration_id": plan["migration_id"],
    "captured_at_utc": timestamp(1),
    "plan_sha256": plan_sha,
    "image": plan["source"],
    "objects": objects,
    "entries": entries,
    "claims": claims,
}
(root / "source.json").write_text(json.dumps(source, indent=2, sort_keys=True) + "\n")
source_sha = hashlib.sha256((root / "source.json").read_bytes()).hexdigest()
phases = [
    ("PLANNED", 2, 1),
    ("SOURCE_CAPTURED", 3, 1),
    ("DESTINATION_CREATED", 4, 1),
    ("COPYING", 5, 1),
    ("RESUME_REQUIRED", 6, 1),
    ("COPYING", 7, 2),
    ("VERIFYING", 8, 2),
    ("ACCEPTED", 9, 2),
]
ledger = {
    "schema": "KAFS.V5V7MigrationCopyLedger.v1",
    "migration_id": plan["migration_id"],
    "plan_sha256": plan_sha,
    "source_sha256": source_sha,
    "attempt": 2,
    "source_image_sha256_before": source_image_sha,
    "source_image_sha256_after": source_image_sha,
    "destination_image_id": plan["destination"]["image_id"],
    "phase_history": [
        {"phase": phase, "at_utc": timestamp(minute), "attempt": attempt}
        for phase, minute, attempt in phases
    ],
    "objects": [
        {
            "object_id": item["object_id"],
            "status": "COMPLETE",
            "bytes_total": item["size_bytes"],
            "bytes_copied": item["size_bytes"],
            "payload_sha256": item["payload_sha256"],
        }
        for item in objects
    ],
    "state": "COMPLETE",
    "updated_at_utc": timestamp(9),
    "claims": claims,
}
(root / "ledger.json").write_text(json.dumps(ledger, indent=2, sort_keys=True) + "\n")
ledger_sha = hashlib.sha256((root / "ledger.json").read_bytes()).hexdigest()
destination = {
    "schema": "KAFS.V7MigrationDestinationInventory.v1",
    "migration_id": plan["migration_id"],
    "captured_at_utc": timestamp(10),
    "plan_sha256": plan_sha,
    "source_sha256": source_sha,
    "ledger_sha256": ledger_sha,
    "image": {
        **plan["destination"],
        "image_sha256": payload(b"synthetic accepted v7 destination image"),
    },
    "state": "COMPLETE",
    "objects": objects,
    "entries": entries,
    "fsck_full_check_passed": True,
    "kafsdump_completed": True,
    "admission_ready": True,
    "claims": claims,
}
(root / "destination.json").write_text(
    json.dumps(destination, indent=2, sort_keys=True) + "\n"
)
destination_sha = hashlib.sha256((root / "destination.json").read_bytes()).hexdigest()
decision = {
    "schema": "KAFS.V5V7MigrationDecision.v1",
    "migration_id": plan["migration_id"],
    "operator_id": "synthetic-migration-operator",
    "decided_at_utc": timestamp(11),
    "decision": "ACCEPT",
    "bindings": {
        "plan_sha256": plan_sha,
        "source_sha256": source_sha,
        "ledger_sha256": ledger_sha,
        "destination_sha256": destination_sha,
    },
    "source_immutable": True,
    "destination_complete": True,
    "resume_from_object_id": None,
    "rollback_source_selected": False,
    "failure_reason": "",
    "claims": claims,
    "confirmation": f"MIGRATION T59A {plan['migration_id']} ACCEPT",
}
(root / "decision.json").write_text(json.dumps(decision, indent=2, sort_keys=True) + "\n")
inventory = []
for name in ("decision.json", "destination.json", "ledger.json", "plan.json", "source.json"):
    inventory.append(f"{hashlib.sha256((root / name).read_bytes()).hexdigest()}  ./{name}")
(root / "artifacts.sha256").write_text("\n".join(inventory) + "\n")
PY

"$gate" --evidence-dir "$bundle" --require-decision ACCEPT --validate-only >/dev/null

expect_failure() {
  local label=$1
  shift
  if "$@" >/dev/null 2>&1; then
    echo "migration evidence gate unexpectedly accepted: $label" >&2
    exit 1
  fi
}

clone_bundle() {
  local name=$1
  local target="$workdir/$name"
  cp -a -- "$bundle" "$target"
  printf '%s\n' "$target"
}

rebind_bundle() {
  local target=$1
  python3 - "$target" <<'PY'
import hashlib
import json
from pathlib import Path
import sys


root = Path(sys.argv[1])


def read(name):
    return json.loads((root / name).read_text(encoding="utf-8"))


def write(name, value):
    (root / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def digest(name):
    return hashlib.sha256((root / name).read_bytes()).hexdigest()


source = read("source.json")
source["plan_sha256"] = digest("plan.json")
write("source.json", source)
ledger = read("ledger.json")
ledger["plan_sha256"] = digest("plan.json")
ledger["source_sha256"] = digest("source.json")
write("ledger.json", ledger)
destination = read("destination.json")
destination["plan_sha256"] = digest("plan.json")
destination["source_sha256"] = digest("source.json")
destination["ledger_sha256"] = digest("ledger.json")
write("destination.json", destination)
decision = read("decision.json")
decision["bindings"] = {
    "plan_sha256": digest("plan.json"),
    "source_sha256": digest("source.json"),
    "ledger_sha256": digest("ledger.json"),
    "destination_sha256": digest("destination.json"),
}
write("decision.json", decision)
names = ("decision.json", "destination.json", "ledger.json", "plan.json", "source.json")
(root / "artifacts.sha256").write_text(
    "\n".join(f"{digest(name)}  ./{name}" for name in names) + "\n",
    encoding="utf-8",
)
PY
}

resume=$(clone_bundle resume)
python3 - "$resume" <<'PY'
import json
from pathlib import Path
import sys


root = Path(sys.argv[1])
ledger_path = root / "ledger.json"
ledger = json.loads(ledger_path.read_text())
ledger["attempt"] = 1
ledger["phase_history"] = ledger["phase_history"][:5]
ledger["state"] = "INTERRUPTED"
ledger["updated_at_utc"] = ledger["phase_history"][-1]["at_utc"]
complete = {"obj-dir", "obj-root"}
for item in ledger["objects"]:
    if item["object_id"] not in complete:
        item["status"] = "PENDING"
        item["bytes_copied"] = 0
ledger_path.write_text(json.dumps(ledger, indent=2, sort_keys=True) + "\n")
destination_path = root / "destination.json"
destination = json.loads(destination_path.read_text())
destination["state"] = "PARTIAL"
destination["objects"] = [
    item for item in destination["objects"] if item["object_id"] in complete
]
destination["entries"] = [
    item for item in destination["entries"] if item["object_id"] in complete
]
destination["fsck_full_check_passed"] = False
destination["kafsdump_completed"] = False
destination["admission_ready"] = False
destination_path.write_text(json.dumps(destination, indent=2, sort_keys=True) + "\n")
decision_path = root / "decision.json"
decision = json.loads(decision_path.read_text())
decision["decision"] = "RESUME_REQUIRED"
decision["destination_complete"] = False
decision["resume_from_object_id"] = "obj-empty"
decision["failure_reason"] = "synthetic interruption after directory copy"
decision["confirmation"] = f"MIGRATION T59A {decision['migration_id']} RESUME_REQUIRED"
decision_path.write_text(json.dumps(decision, indent=2, sort_keys=True) + "\n")
PY
rebind_bundle "$resume"
"$gate" --evidence-dir "$resume" --require-decision RESUME_REQUIRED --validate-only >/dev/null

rollback="$workdir/rollback"
cp -a -- "$resume" "$rollback"
python3 - "$rollback" <<'PY'
import json
from pathlib import Path
import sys


root = Path(sys.argv[1])
ledger_path = root / "ledger.json"
ledger = json.loads(ledger_path.read_text())
ledger["state"] = "ROLLED_BACK"
ledger["phase_history"].append(
    {"phase": "ROLLED_BACK", "at_utc": "2026-07-21T00:07:00Z", "attempt": 1}
)
ledger["updated_at_utc"] = "2026-07-21T00:07:00Z"
ledger_path.write_text(json.dumps(ledger, indent=2, sort_keys=True) + "\n")
destination_path = root / "destination.json"
destination = json.loads(destination_path.read_text())
destination["state"] = "PRESERVED_FAILED"
destination["captured_at_utc"] = "2026-07-21T00:08:00Z"
destination_path.write_text(json.dumps(destination, indent=2, sort_keys=True) + "\n")
decision_path = root / "decision.json"
decision = json.loads(decision_path.read_text())
decision["decision"] = "ROLLBACK"
decision["resume_from_object_id"] = None
decision["rollback_source_selected"] = True
decision["failure_reason"] = "synthetic operator rollback preserves failed destination"
decision["confirmation"] = f"MIGRATION T59A {decision['migration_id']} ROLLBACK"
decision_path.write_text(json.dumps(decision, indent=2, sort_keys=True) + "\n")
PY
rebind_bundle "$rollback"
"$gate" --evidence-dir "$rollback" --require-decision ROLLBACK --validate-only >/dev/null

expect_failure required-decision-mismatch "$gate" --evidence-dir "$bundle" \
  --require-decision ROLLBACK --validate-only

source_mutation=$(clone_bundle source-mutation)
python3 - "$source_mutation/ledger.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text())
data["source_image_sha256_after"] = "0" * 64
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
PY
rebind_bundle "$source_mutation"
expect_failure source-mutation "$gate" --evidence-dir "$source_mutation" \
  --require-decision ACCEPT --validate-only

identity_drift=$(clone_bundle identity-drift)
python3 - "$identity_drift/source.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text())
data["image"]["image_id"] = "other-source-v5.img"
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
PY
rebind_bundle "$identity_drift"
expect_failure identity-drift "$gate" --evidence-dir "$identity_drift" \
  --require-decision ACCEPT --validate-only

missing_object=$(clone_bundle missing-object)
python3 - "$missing_object/destination.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text())
data["objects"] = [item for item in data["objects"] if item["object_id"] != "obj-hard"]
data["entries"] = [item for item in data["entries"] if item["object_id"] != "obj-hard"]
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
PY
rebind_bundle "$missing_object"
expect_failure missing-object "$gate" --evidence-dir "$missing_object" \
  --require-decision ACCEPT --validate-only

payload_mismatch=$(clone_bundle payload-mismatch)
python3 - "$payload_mismatch/destination.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text())
data["objects"][2]["payload_sha256"] = "0" * 64
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
PY
rebind_bundle "$payload_mismatch"
expect_failure payload-mismatch "$gate" --evidence-dir "$payload_mismatch" \
  --require-decision ACCEPT --validate-only

illegal_transition=$(clone_bundle illegal-transition)
python3 - "$illegal_transition/ledger.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text())
data["phase_history"][4]["phase"] = "VERIFYING"
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
PY
rebind_bundle "$illegal_transition"
expect_failure illegal-transition "$gate" --evidence-dir "$illegal_transition" \
  --require-decision ACCEPT --validate-only

attempt_drift=$(clone_bundle attempt-drift)
python3 - "$attempt_drift/ledger.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text())
data["phase_history"][6]["attempt"] = 3
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
PY
rebind_bundle "$attempt_drift"
expect_failure attempt-drift "$gate" --evidence-dir "$attempt_drift" \
  --require-decision ACCEPT --validate-only

partial_accept=$(clone_bundle partial-accept)
python3 - "$partial_accept/destination.json" "$partial_accept/decision.json" <<'PY'
import json
from pathlib import Path
import sys
destination_path = Path(sys.argv[1])
decision_path = Path(sys.argv[2])
destination = json.loads(destination_path.read_text())
destination["state"] = "PARTIAL"
destination["admission_ready"] = False
destination_path.write_text(json.dumps(destination, indent=2, sort_keys=True) + "\n")
decision = json.loads(decision_path.read_text())
decision["destination_complete"] = False
decision_path.write_text(json.dumps(decision, indent=2, sort_keys=True) + "\n")
PY
rebind_bundle "$partial_accept"
expect_failure partial-accept "$gate" --evidence-dir "$partial_accept" \
  --require-decision ACCEPT --validate-only

claim=$(clone_bundle claim-escalation)
python3 - "$claim/decision.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text())
data["claims"]["production_cutover_authorized"] = True
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
PY
rebind_bundle "$claim"
expect_failure claim-escalation "$gate" --evidence-dir "$claim" \
  --require-decision ACCEPT --validate-only

wrong_resume="$workdir/wrong-resume"
cp -a -- "$resume" "$wrong_resume"
python3 - "$wrong_resume/decision.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text())
data["resume_from_object_id"] = "obj-file"
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
PY
rebind_bundle "$wrong_resume"
expect_failure wrong-resume "$gate" --evidence-dir "$wrong_resume" \
  --require-decision RESUME_REQUIRED --validate-only

bad_rollback="$workdir/bad-rollback"
cp -a -- "$rollback" "$bad_rollback"
python3 - "$bad_rollback/decision.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text())
data["failure_reason"] = ""
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
PY
rebind_bundle "$bad_rollback"
expect_failure rollback-without-reason "$gate" --evidence-dir "$bad_rollback" \
  --require-decision ROLLBACK --validate-only

link_count=$(clone_bundle link-count)
python3 - "$link_count/source.json" "$link_count/destination.json" <<'PY'
import json
from pathlib import Path
import sys
for value in sys.argv[1:]:
    path = Path(value)
    data = json.loads(path.read_text())
    data["objects"][3]["link_count"] = 1
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
PY
rebind_bundle "$link_count"
expect_failure link-count "$gate" --evidence-dir "$link_count" \
  --require-decision ACCEPT --validate-only

tampered=$(clone_bundle tampered)
printf 'tampered\n' >>"$tampered/plan.json"
expect_failure artifact-tamper "$gate" --evidence-dir "$tampered" \
  --require-decision ACCEPT --validate-only

unexpected=$(clone_bundle unexpected)
printf 'unexpected\n' >"$unexpected/unlisted.log"
expect_failure unexpected-file "$gate" --evidence-dir "$unexpected" \
  --require-decision ACCEPT --validate-only

symlinked=$(clone_bundle symlinked)
find "$symlinked/decision.json" -delete
ln -s "$bundle/decision.json" "$symlinked/decision.json"
expect_failure symlinked-artifact "$gate" --evidence-dir "$symlinked" \
  --require-decision ACCEPT --validate-only

echo "v5-to-v7 migration evidence gate regression: PASS"
