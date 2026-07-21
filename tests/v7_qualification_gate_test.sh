#!/usr/bin/env bash
set -euo pipefail

gate=${KAFS_TEST_V7_QUALIFICATION_GATE:-../scripts/v7-controlled-write-qualification-gate.sh}
[[ -x "$gate" ]] || {
  echo "qualification gate is not executable: $gate" >&2
  exit 1
}

workdir=$(mktemp -d "${TMPDIR:-/tmp}/kafs-v7-qualification-gate.XXXXXX")
cleanup() {
  find "$workdir" -depth -delete
}
trap cleanup EXIT

base="$workdir/pass"
mkdir -p "$base/artifacts"
printf 'synthetic qualification evidence\n' >"$base/artifacts/evidence.txt"
printf 'synthetic workload command\n' >"$base/artifacts/workload.command"
printf 'synthetic executable digests\n' >"$base/artifacts/executables.sha256"

python3 - "$base" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

report = Path(sys.argv[1])
case_ids = [
    "format_and_seed",
    "inspection_mount",
    "direct_partial_overwrite",
    "direct_multi_block_overwrite",
    "direct_growth",
    "direct_truncate",
    "create_inline_write",
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
manifest = {
    "schema": "KAFS.V7ControlledWriteQualification.v1",
    "scope": "non-destructive-file-image",
    "status": "PASS",
    "claims": {
        "rc_eligible": False,
        "real_media_qualified": False,
        "controller_independent_wear": False,
    },
    "sample": {
        "id": "synthetic-file-image",
        "media_class": "file-image",
        "backing": "temporary-sparse-image",
        "controller": "not-applicable",
        "power_interruption": "process-fault-injection",
    },
    "environment": {
        "generated_at_utc": "2026-07-21T00:00:00Z",
        "git_commit": "synthetic",
        "uname": "synthetic",
        "kernel": "synthetic",
        "libfuse": "synthetic",
        "git_dirty": False,
    },
    "workload_engine": "tests/v7_inspection_mount_smoketest",
    "results": [
        {"id": case_id, "status": "PASS", "evidence": ["artifacts/evidence.txt"]}
        for case_id in case_ids
    ],
    "artifacts": [],
    "limitations": [
        "not-real-media-qualification",
        "no-physical-power-cut",
        "no-nand-ftl-independence-claim",
        "not-rc-approval",
    ],
}
for path in sorted((report / "artifacts").iterdir()):
    data = path.read_bytes()
    manifest["artifacts"].append(
        {
            "path": path.relative_to(report).as_posix(),
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    )
(report / "qualification.json").write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY

"$gate" --report-dir "$base" --validate-only >/dev/null

expect_failure() {
  local name="$1"
  local report="$workdir/$name"
  cp -a "$base" "$report"
  shift
  "$@" "$report"
  if "$gate" --report-dir "$report" --validate-only >/dev/null 2>&1; then
    echo "qualification gate unexpectedly accepted: $name" >&2
    exit 1
  fi
}

set_case_skip() {
  python3 - "$1/qualification.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["results"][0]["status"] = "SKIP"
path.write_text(json.dumps(data) + "\n", encoding="utf-8")
PY
}

remove_sample_id() {
  python3 - "$1/qualification.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
del data["sample"]["id"]
path.write_text(json.dumps(data) + "\n", encoding="utf-8")
PY
}

enable_rc_claim() {
  python3 - "$1/qualification.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["claims"]["rc_eligible"] = True
path.write_text(json.dumps(data) + "\n", encoding="utf-8")
PY
}

corrupt_artifact() {
  printf 'corruption\n' >>"$1/artifacts/evidence.txt"
}

add_device_path() {
  python3 - "$1/qualification.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["sample"]["device_path"] = "/dev/example"
path.write_text(json.dumps(data) + "\n", encoding="utf-8")
PY
}

expect_failure skipped-case set_case_skip
expect_failure missing-sample remove_sample_id
expect_failure rc-claim enable_rc_claim
expect_failure artifact-digest corrupt_artifact
expect_failure device-path add_device_path

echo "v7 qualification gate regression: PASS"
