#!/usr/bin/env bash
set -euo pipefail

gate=${KAFS_TEST_V7_REAL_MEDIA_EVIDENCE_GATE:-../scripts/v7-real-media-qualification-evidence-gate.sh}
approval_gate=${KAFS_TEST_V7_REAL_MEDIA_APPROVAL_GATE:-../scripts/v7-real-media-qualification-approval-gate.sh}
draft_source=${KAFS_TEST_V7_REAL_MEDIA_MATRIX:-../docs/sd-card-wear-v7-real-media-qualification-matrix.json}
[[ -x "$gate" ]] || {
  echo "real-media evidence gate is not executable: $gate" >&2
  exit 1
}
[[ -x "$approval_gate" ]] || {
  echo "real-media approval gate is not executable: $approval_gate" >&2
  exit 1
}
[[ -f "$draft_source" ]] || {
  echo "real-media matrix draft is missing: $draft_source" >&2
  exit 1
}
export KAFS_V7_REAL_MEDIA_APPROVAL_GATE="$approval_gate"

workdir=$(mktemp -d "${TMPDIR:-/tmp}/kafs-v7-real-media-evidence.XXXXXX")
cleanup() {
  find "$workdir" -depth -delete
}
trap cleanup EXIT

matrix="$workdir/matrix.json"
approval="$workdir/approval.json"
bundle="$workdir/bundle"
review="$workdir/review.json"
mkdir -p "$bundle/artifacts"

python3 - "$draft_source" "$matrix" "$approval" "$bundle" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

draft = Path(sys.argv[1])
matrix_path = Path(sys.argv[2])
approval_path = Path(sys.argv[3])
bundle = Path(sys.argv[4])

matrix = json.loads(draft.read_text(encoding="utf-8"))
matrix["state"] = "READY_FOR_APPROVAL"
matrix["matrix_id"] = "T58-SYNTHETIC-MATRIX"
matrix["blocked_by"] = []
matrix["discovery"]["eligible_device_count"] = 1
matrix["discovery"]["status"] = "ELIGIBLE_REAL_MEDIA_IDENTIFIED"
matrix["target_host"] = {
    "host_id": "synthetic-qualification-host",
    "kernel": "synthetic-kernel",
    "libfuse": "synthetic-libfuse",
    "native_or_passthrough_environment": "native-linux",
}
matrix["test_plan"]["cycles_per_workload"] = 2
sample = {
    "sample_id": "synthetic-card-1",
    "card": {
        "manufacturer": "synthetic-vendor",
        "model": "synthetic-model",
        "unit_id": "synthetic-unit-1",
        "unit_id_source": "inventory-label",
        "capacity_bytes": 68719476736,
    },
    "reader_controller": {
        "manufacturer": "synthetic-reader-vendor",
        "model": "synthetic-reader",
        "stable_id": "synthetic-reader-1",
        "transport": "usb",
    },
    "device": {
        "stable_path": "/dev/disk/by-id/synthetic-card-1",
        "kernel_path": "/dev/sdz",
        "major_minor": "65:144",
        "expected_size_bytes": 68719476736,
        "whole_device": True,
        "disposable": True,
        "system_or_host_storage": False,
        "mounted": False,
        "swap": False,
    },
    "power_cut": {
        "method": "synthetic-isolated-switch",
        "apparatus_id": "synthetic-switch-1",
        "trigger_protocol": "synthetic-trigger",
        "power_domain": "reader-and-card-only",
        "host_storage_isolated": True,
    },
}
matrix["samples"] = [sample]
matrix_path.write_text(json.dumps(matrix, indent=2, sort_keys=True) + "\n", encoding="utf-8")

matrix_raw = matrix_path.read_bytes()
matrix_digest = hashlib.sha256(matrix_raw).hexdigest()
approval = {
    "schema": "KAFS.V7RealMediaQualificationApproval.v1",
    "state": "APPROVED",
    "matrix_id": matrix["matrix_id"],
    "matrix_sha256": matrix_digest,
    "approved_by": "synthetic-approver",
    "approved_at_utc": "2026-07-21T00:00:00Z",
    "valid_until_utc": "2026-07-23T00:00:00Z",
    "authorized_actions": ["format_whole_device", "controlled_power_interruption"],
    "destructive_impact_acknowledged": True,
    "disposable_media_confirmed": True,
    "confirmation": f"AUTHORIZE T48 {matrix['matrix_id']} {matrix_digest}",
}
approval_path.write_text(json.dumps(approval, indent=2, sort_keys=True) + "\n", encoding="utf-8")

artifact_paths = {}
for kind in matrix["retention"]["required_artifacts"]:
    path = bundle / "artifacts" / f"{kind}.txt"
    path.write_text(f"synthetic {kind} evidence\n", encoding="utf-8")
    artifact_paths[kind] = path.relative_to(bundle).as_posix()

artifacts = []
for kind, relative in sorted(artifact_paths.items()):
    raw = (bundle / relative).read_bytes()
    artifacts.append(
        {
            "path": relative,
            "kind": kind,
            "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
    )

normal_evidence = [
    artifact_paths["command-log"],
    artifact_paths["workload-log"],
    artifact_paths["mount-log"],
    artifact_paths["fsck-after"],
    artifact_paths["kafsdump-after"],
]
interrupt_evidence = normal_evidence + [artifact_paths["power-cut-log"]]
boundaries = ["normal"] + matrix["test_plan"]["controlled_interruption_boundaries"]
results = []
for workload in matrix["test_plan"]["normal_workloads"]:
    for boundary in boundaries:
        for cycle in range(1, matrix["test_plan"]["cycles_per_workload"] + 1):
            results.append(
                {
                    "workload": workload,
                    "boundary": boundary,
                    "cycle": cycle,
                    "status": "PASS",
                    "allowed_outcome": True,
                    "observed_outcome": "published-state",
                    "evidence": interrupt_evidence if boundary != "normal" else normal_evidence,
                }
            )

identity = {
    key: sample[key]
    for key in ("card", "reader_controller", "device", "power_cut")
}
evidence = {
    "schema": "KAFS.V7RealMediaQualificationEvidence.v1",
    "scope": "format-v7-bounded-controlled-write-real-media",
    "state": "COMPLETE",
    "run_id": "T58-SYNTHETIC-RUN",
    "binding": {
        "matrix_id": matrix["matrix_id"],
        "matrix_sha256": matrix_digest,
        "approval_sha256": hashlib.sha256(approval_path.read_bytes()).hexdigest(),
    },
    "operator": {"id": "synthetic-operator"},
    "started_at_utc": "2026-07-21T12:00:00Z",
    "completed_at_utc": "2026-07-21T13:00:00Z",
    "environment": matrix["target_host"],
    "claims": {
        "rc_eligible": False,
        "real_media_qualified": False,
        "controller_independent_wear": False,
    },
    "retention": {
        "immutable_copy_created": True,
        "minimum_days": matrix["retention"]["minimum_days"],
        "retain_through": matrix["retention"]["retain_through"],
    },
    "artifacts": artifacts,
    "samples": [
        {
            "sample_id": sample["sample_id"],
            "identity_before": identity,
            "identity_after": identity,
            "results": results,
        }
    ],
}
(bundle / "evidence.json").write_text(
    json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY

make_review() {
  local evidence_dir=$1
  local target=$2
  local decision=$3
  local finding=$4
  python3 - "$matrix" "$approval" "$evidence_dir/evidence.json" "$target" "$decision" "$finding" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

matrix_path = Path(sys.argv[1])
approval_path = Path(sys.argv[2])
evidence_path = Path(sys.argv[3])
target = Path(sys.argv[4])
decision = sys.argv[5]
finding = sys.argv[6]
matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
evidence_digest = hashlib.sha256(evidence_path.read_bytes()).hexdigest()
review = {
    "schema": "KAFS.V7RealMediaQualificationReview.v1",
    "state": "FINAL",
    "review_id": f"T58-SYNTHETIC-{decision}",
    "run_id": evidence["run_id"],
    "binding": {
        "matrix_id": matrix["matrix_id"],
        "matrix_sha256": hashlib.sha256(matrix_path.read_bytes()).hexdigest(),
        "approval_sha256": hashlib.sha256(approval_path.read_bytes()).hexdigest(),
        "evidence_sha256": evidence_digest,
    },
    "operator_id": evidence["operator"]["id"],
    "reviewer": {"id": "synthetic-independent-reviewer"},
    "decision": decision,
    "reviewed_at_utc": "2026-07-21T14:00:00Z",
    "raw_evidence_reviewed": True,
    "checks": {
        "approval_binding_verified": True,
        "identity_continuity_verified": True,
        "coverage_complete": True,
        "artifact_digests_verified": True,
        "allowed_outcomes_verified": True,
        "claim_boundary_verified": True,
    },
    "findings": [finding] if finding else [],
    "claims": {
        "rc_eligible": False,
        "real_media_qualified": False,
        "controller_independent_wear": False,
    },
    "confirmation": f"REVIEW T58 {evidence['run_id']} {evidence_digest} {decision}",
}
target.write_text(json.dumps(review, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

make_review "$bundle" "$review" ACCEPT ""
"$gate" --evidence-dir "$bundle" --matrix "$matrix" --approval "$approval" \
  --validate-only >/dev/null
"$gate" --evidence-dir "$bundle" --matrix "$matrix" --approval "$approval" \
  --review "$review" --validate-only --require-review >/dev/null

expect_failure() {
  local label=$1
  shift
  if "$@" >/dev/null 2>&1; then
    echo "real-media evidence gate unexpectedly accepted: $label" >&2
    exit 1
  fi
}

clone_bundle() {
  local name=$1
  local target="$workdir/$name"
  cp -a -- "$bundle" "$target"
  printf '%s\n' "$target"
}

missing_cycle=$(clone_bundle missing-cycle)
python3 - "$missing_cycle/evidence.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["samples"][0]["results"].pop()
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
expect_failure missing-cycle "$gate" --evidence-dir "$missing_cycle" --matrix "$matrix" \
  --approval "$approval" --validate-only

identity_drift=$(clone_bundle identity-drift)
python3 - "$identity_drift/evidence.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["samples"][0]["identity_after"]["device"]["kernel_path"] = "/dev/sdy"
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
expect_failure identity-drift "$gate" --evidence-dir "$identity_drift" --matrix "$matrix" \
  --approval "$approval" --validate-only

wrong_matrix=$(clone_bundle wrong-matrix-digest)
python3 - "$wrong_matrix/evidence.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["binding"]["matrix_sha256"] = "0" * 64
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
expect_failure wrong-matrix-digest "$gate" --evidence-dir "$wrong_matrix" --matrix "$matrix" \
  --approval "$approval" --validate-only

wrong_approval=$(clone_bundle wrong-approval-digest)
python3 - "$wrong_approval/evidence.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["binding"]["approval_sha256"] = "0" * 64
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
expect_failure wrong-approval-digest "$gate" --evidence-dir "$wrong_approval" --matrix "$matrix" \
  --approval "$approval" --validate-only

tampered=$(clone_bundle tampered-artifact)
printf 'tampered\n' >>"$tampered/artifacts/workload-log.txt"
expect_failure artifact-digest "$gate" --evidence-dir "$tampered" --matrix "$matrix" \
  --approval "$approval" --validate-only

claim=$(clone_bundle claim-escalation)
python3 - "$claim/evidence.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["claims"]["real_media_qualified"] = True
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
expect_failure claim-escalation "$gate" --evidence-dir "$claim" --matrix "$matrix" \
  --approval "$approval" --validate-only

self_review="$workdir/self-review.json"
cp -- "$review" "$self_review"
python3 - "$self_review" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["reviewer"]["id"] = data["operator_id"]
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
expect_failure self-review "$gate" --evidence-dir "$bundle" --matrix "$matrix" \
  --approval "$approval" --review "$self_review" --validate-only --require-review

false_check="$workdir/false-check.json"
cp -- "$review" "$false_check"
python3 - "$false_check" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["checks"]["coverage_complete"] = False
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
expect_failure accept-false-check "$gate" --evidence-dir "$bundle" --matrix "$matrix" \
  --approval "$approval" --review "$false_check" --validate-only --require-review

inconclusive=$(clone_bundle inconclusive)
python3 - "$inconclusive/evidence.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
result = data["samples"][0]["results"][0]
result["status"] = "INCONCLUSIVE"
result["allowed_outcome"] = False
result["observed_outcome"] = "timing evidence incomplete"
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
inconclusive_review="$workdir/inconclusive-review.json"
make_review "$inconclusive" "$inconclusive_review" INCONCLUSIVE "timing evidence incomplete"
"$gate" --evidence-dir "$inconclusive" --matrix "$matrix" --approval "$approval" \
  --review "$inconclusive_review" --validate-only --require-review >/dev/null

false_accept="$workdir/false-accept.json"
make_review "$inconclusive" "$false_accept" ACCEPT ""
expect_failure false-accept "$gate" --evidence-dir "$inconclusive" --matrix "$matrix" \
  --approval "$approval" --review "$false_accept" --validate-only --require-review

wrong_evidence="$workdir/wrong-evidence-digest.json"
cp -- "$review" "$wrong_evidence"
python3 - "$wrong_evidence" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["binding"]["evidence_sha256"] = "0" * 64
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
expect_failure wrong-evidence-digest "$gate" --evidence-dir "$bundle" --matrix "$matrix" \
  --approval "$approval" --review "$wrong_evidence" --validate-only --require-review

echo "v7 real-media evidence gate regression: PASS"
