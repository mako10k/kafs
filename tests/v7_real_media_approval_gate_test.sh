#!/usr/bin/env bash
set -euo pipefail

gate=${KAFS_TEST_V7_REAL_MEDIA_APPROVAL_GATE:-../scripts/v7-real-media-qualification-approval-gate.sh}
draft_source=${KAFS_TEST_V7_REAL_MEDIA_MATRIX:-../docs/sd-card-wear-v7-real-media-qualification-matrix.json}
[[ -x "$gate" ]] || {
  echo "real-media approval gate is not executable: $gate" >&2
  exit 1
}
[[ -f "$draft_source" ]] || {
  echo "real-media matrix draft is missing: $draft_source" >&2
  exit 1
}

workdir=$(mktemp -d "${TMPDIR:-/tmp}/kafs-v7-real-media-approval.XXXXXX")
cleanup() {
  find "$workdir" -depth -delete
}
trap cleanup EXIT

draft="$workdir/draft.json"
ready="$workdir/ready.json"
approval="$workdir/approval.json"
cp -- "$draft_source" "$draft"

"$gate" --matrix "$draft" --validate-only >/dev/null

python3 - "$draft" "$ready" <<'PY'
import json
from pathlib import Path
import sys

source = Path(sys.argv[1])
target = Path(sys.argv[2])
data = json.loads(source.read_text(encoding="utf-8"))
data["state"] = "READY_FOR_APPROVAL"
data["matrix_id"] = "T48-TEST-READY"
data["blocked_by"] = []
data["discovery"]["eligible_device_count"] = 1
data["discovery"]["status"] = "ELIGIBLE_REAL_MEDIA_IDENTIFIED"
data["target_host"] = {
    "host_id": "qualification-host-1",
    "kernel": "test-kernel",
    "libfuse": "test-libfuse",
    "native_or_passthrough_environment": "native-linux",
}
data["test_plan"]["cycles_per_workload"] = 2
data["samples"] = [
    {
        "sample_id": "card-unit-1",
        "card": {
            "manufacturer": "test-card-vendor",
            "model": "test-card-model",
            "unit_id": "test-card-unit-id",
            "unit_id_source": "inventory-label",
            "capacity_bytes": 68719476736,
        },
        "reader_controller": {
            "manufacturer": "test-reader-vendor",
            "model": "test-reader-model",
            "stable_id": "test-reader-unit-id",
            "transport": "usb",
        },
        "device": {
            "stable_path": "/dev/disk/by-id/test-card-unit-1",
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
            "method": "isolated-reader-power-switch",
            "apparatus_id": "test-switch-1",
            "trigger_protocol": "operator-observed-transaction-marker",
            "power_domain": "reader-and-card-only",
            "host_storage_isolated": True,
        },
    }
]
target.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

"$gate" --matrix "$ready" --validate-only >/dev/null

python3 - "$ready" "$approval" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

matrix = Path(sys.argv[1])
approval = Path(sys.argv[2])
digest = hashlib.sha256(matrix.read_bytes()).hexdigest()
data = json.loads(matrix.read_text(encoding="utf-8"))
record = {
    "schema": "KAFS.V7RealMediaQualificationApproval.v1",
    "state": "APPROVED",
    "matrix_id": data["matrix_id"],
    "matrix_sha256": digest,
    "approved_by": "synthetic-operator",
    "approved_at_utc": "2000-01-01T00:00:00Z",
    "valid_until_utc": "2999-12-31T23:59:59Z",
    "authorized_actions": [
        "format_whole_device",
        "controlled_power_interruption",
    ],
    "destructive_impact_acknowledged": True,
    "disposable_media_confirmed": True,
    "confirmation": f"AUTHORIZE T48 {data['matrix_id']} {digest}",
}
approval.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

"$gate" --matrix "$ready" --approval "$approval" --validate-only --require-approved \
  >/dev/null

expect_failure() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "real-media approval gate unexpectedly accepted: $label" >&2
    exit 1
  fi
}

"$gate" --matrix "$ready" --approval "$approval" --validate-only \
  --require-approved --as-of 2026-07-22T00:00:00Z >/dev/null
expect_failure as-of-without-approval "$gate" --matrix "$ready" --validate-only \
  --as-of 2026-07-22T00:00:00Z
expect_failure as-of-before-approval "$gate" --matrix "$ready" --approval "$approval" \
  --validate-only --require-approved --as-of 1999-12-31T23:59:59Z
expect_failure invalid-as-of "$gate" --matrix "$ready" --approval "$approval" \
  --validate-only --require-approved --as-of not-a-time
expect_failure future-as-of "$gate" --matrix "$ready" --approval "$approval" \
  --validate-only --require-approved --as-of 2998-01-01T00:00:00Z

unsafe_draft="$workdir/unsafe-draft.json"
python3 - "$draft" "$unsafe_draft" <<'PY'
import json
from pathlib import Path
import sys
source = Path(sys.argv[1])
target = Path(sys.argv[2])
data = json.loads(source.read_text(encoding="utf-8"))
data["execution_policy"]["raw_device_execution_without_approval"] = True
target.write_text(json.dumps(data) + "\n", encoding="utf-8")
PY
expect_failure unsafe-draft "$gate" --matrix "$unsafe_draft" --validate-only

missing_promotion="$workdir/missing-promotion.json"
python3 - "$ready" "$missing_promotion" <<'PY'
import json
from pathlib import Path
import sys
source = Path(sys.argv[1])
target = Path(sys.argv[2])
data = json.loads(source.read_text(encoding="utf-8"))
data["test_plan"]["normal_workloads"].remove("regular_file_inline_to_direct_promotion")
target.write_text(json.dumps(data) + "\n", encoding="utf-8")
PY
expect_failure missing-promotion-workload "$gate" --matrix "$missing_promotion" --validate-only

missing_single="$workdir/missing-single.json"
python3 - "$ready" "$missing_single" <<'PY'
import json
from pathlib import Path
import sys
source = Path(sys.argv[1])
target = Path(sys.argv[2])
data = json.loads(source.read_text(encoding="utf-8"))
data["test_plan"]["normal_workloads"].remove("regular_file_single_indirect_lifecycle")
target.write_text(json.dumps(data) + "\n", encoding="utf-8")
PY
expect_failure missing-single-workload "$gate" --matrix "$missing_single" --validate-only

unstable_path="$workdir/unstable-path.json"
python3 - "$ready" "$unstable_path" <<'PY'
import json
from pathlib import Path
import sys
source = Path(sys.argv[1])
target = Path(sys.argv[2])
data = json.loads(source.read_text(encoding="utf-8"))
data["samples"][0]["device"]["stable_path"] = "/dev/sdz"
target.write_text(json.dumps(data) + "\n", encoding="utf-8")
PY
expect_failure unstable-device-path "$gate" --matrix "$unstable_path" --validate-only

system_target="$workdir/system-target.json"
python3 - "$ready" "$system_target" <<'PY'
import json
from pathlib import Path
import sys
source = Path(sys.argv[1])
target = Path(sys.argv[2])
data = json.loads(source.read_text(encoding="utf-8"))
data["samples"][0]["device"]["system_or_host_storage"] = True
target.write_text(json.dumps(data) + "\n", encoding="utf-8")
PY
expect_failure system-target "$gate" --matrix "$system_target" --validate-only

wrong_digest="$workdir/wrong-digest.json"
python3 - "$approval" "$wrong_digest" <<'PY'
import json
from pathlib import Path
import sys
source = Path(sys.argv[1])
target = Path(sys.argv[2])
data = json.loads(source.read_text(encoding="utf-8"))
data["matrix_sha256"] = "0" * 64
target.write_text(json.dumps(data) + "\n", encoding="utf-8")
PY
expect_failure wrong-digest "$gate" --matrix "$ready" --approval "$wrong_digest" \
  --validate-only --require-approved

expired="$workdir/expired.json"
python3 - "$approval" "$expired" <<'PY'
import json
from pathlib import Path
import sys
source = Path(sys.argv[1])
target = Path(sys.argv[2])
data = json.loads(source.read_text(encoding="utf-8"))
data["valid_until_utc"] = "2000-01-01T00:00:00Z"
target.write_text(json.dumps(data) + "\n", encoding="utf-8")
PY
expect_failure expired "$gate" --matrix "$ready" --approval "$expired" \
  --validate-only --require-approved

expect_failure draft-require-approved "$gate" --matrix "$draft" --approval "$approval" \
  --validate-only --require-approved

echo "v7 real-media approval gate regression: PASS"
