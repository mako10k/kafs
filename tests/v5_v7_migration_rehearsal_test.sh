#!/usr/bin/env bash
set -euo pipefail

runner=${KAFS_TEST_V5_V7_MIGRATION_REHEARSAL:-../scripts/v5-v7-migration-rehearsal.sh}
[[ -x "$runner" ]] || {
  echo "v5-to-v7 migration rehearsal runner is not executable: $runner" >&2
  exit 1
}
runner=$(cd "$(dirname "$runner")" && printf '%s/%s\n' "$(pwd)" "$(basename "$runner")")

workdir=$(mktemp -d "${TMPDIR:-/tmp}/kafs-v5-v7-migration-rehearsal.XXXXXX")
cleanup() {
  find "$workdir" -depth -delete
}
trap cleanup EXIT

set +e
missing_value_output=$(cd "$workdir" && "$runner" --report-dir --keep-workdir 2>&1)
missing_value_rc=$?
set -e
if [[ "$missing_value_rc" -ne 2 ]] ||
  ! grep -Fq "missing value for --report-dir" <<<"$missing_value_output"; then
  echo "rehearsal runner did not reject an option token used as a value" >&2
  exit 1
fi
[[ ! -e "$workdir/--keep-workdir" ]]

stub="$workdir/workload-status-stub.sh"
printf '%s\n' '#!/bin/sh' 'exit "${KAFS_TEST_WORKLOAD_STATUS:?}"' >"$stub"
chmod 0700 "$stub"
for expected_status in 77 1; do
  report="$workdir/status-$expected_status"
  set +e
  KAFS_TEST_V5_V7_IMPORT_WORKLOAD="$stub" KAFS_TEST_WORKLOAD_STATUS="$expected_status" \
    "$runner" --report-dir "$report" --json \
    >"$workdir/status-$expected_status.stdout.json" \
    2>"$workdir/status-$expected_status.stderr"
  actual_status=$?
  set -e
  [[ "$actual_status" -eq "$expected_status" ]]
  cmp "$workdir/status-$expected_status.stdout.json" "$report/result.json"
  python3 - "$report/result.json" "$expected_status" <<'PY'
import json
from pathlib import Path
import sys


result = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
exit_status = int(sys.argv[2])
expected = "SKIP" if exit_status == 77 else "FAIL"
if (
    result["schema"] != "KAFS.V5V7MigrationRehearsalResult.v1"
    or result["status"] != expected
    or result["exit_status"] != exit_status
    or result["report_retained"] is not True
    or result["rehearsal_manifest"] is not None
    or result["recovery_strategy"] != "full-replay-from-frozen-source"
):
    raise SystemExit(f"rehearsal {expected} result does not match the v1 contract")
PY
done

set +e
human_skip=$(KAFS_TEST_V5_V7_IMPORT_WORKLOAD="$stub" KAFS_TEST_WORKLOAD_STATUS=77 \
  "$runner" --report-dir "$workdir/human-skip" 2>"$workdir/human-skip.stderr")
human_skip_status=$?
set -e
[[ "$human_skip_status" -eq 77 ]]
grep -Fqx "v5-to-v7 migration rehearsal SKIP" <<<"$human_skip"
grep -Fqx "report: $workdir/human-skip" <<<"$human_skip"
[[ -s "$workdir/human-skip/result.json" ]]

set +e
"$runner" --report-dir "$workdir/report" \
  --timeout-ms "${KAFS_TEST_MOUNT_TIMEOUT_MS:-15000}" --json \
  >"$workdir/result.stdout.json" 2>"$workdir/result.stderr"
rc=$?
set -e
[[ -s "$workdir/report/result.json" ]]
cmp "$workdir/result.stdout.json" "$workdir/report/result.json"
python3 - "$workdir/report/result.json" "$workdir/report" "$rc" <<'PY'
import json
from pathlib import Path
import sys


result = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
report = Path(sys.argv[2]).resolve()
exit_status = int(sys.argv[3])
expected_status = "PASS" if exit_status == 0 else "SKIP" if exit_status == 77 else "FAIL"
if set(result) != {
    "schema", "operation", "status", "exit_status", "report_dir", "report_retained",
    "workdir_retained", "artifacts_dir", "bundles_dir", "rehearsal_manifest",
    "recovery_strategy", "reason", "prerequisites", "claims",
}:
    raise SystemExit("rehearsal result fields do not match the v1 contract")
if (
    result["schema"] != "KAFS.V5V7MigrationRehearsalResult.v1"
    or result["operation"] != "disposable-v5-v7-migration-rehearsal"
    or result["status"] != expected_status
    or result["exit_status"] != exit_status
    or result["report_dir"] != str(report)
    or result["report_retained"] is not True
    or result["workdir_retained"] is not False
    or result["artifacts_dir"] != "artifacts"
    or result["bundles_dir"] != "bundles"
    or result["recovery_strategy"] != "full-replay-from-frozen-source"
    or not isinstance(result["reason"], str)
    or not result["reason"]
    or set(result["prerequisites"]) != {"fuse"}
    or result["prerequisites"]["fuse"] not in {"AVAILABLE", "UNAVAILABLE", "UNKNOWN"}
    or result["claims"] != {
        "production_cutover_authorized": False,
        "real_media_qualified": False,
        "physical_media_qualified": False,
        "release_candidate_qualified": False,
    }
):
    raise SystemExit("rehearsal result does not match the v1 contract")
if expected_status == "PASS" and result["rehearsal_manifest"] != "rehearsal.json":
    raise SystemExit("passed rehearsal result does not name its retained manifest")
if expected_status == "SKIP" and result["rehearsal_manifest"] is not None:
    raise SystemExit("skipped rehearsal result unexpectedly names a completed manifest")
PY
if [[ "$rc" -eq 77 ]]; then
  cat "$workdir/result.stderr" >&2
  exit 77
fi
if [[ "$rc" -ne 0 ]]; then
  cat "$workdir/result.stderr" >&2
  exit "$rc"
fi
[[ -s "$workdir/report/rehearsal.json" ]]
python3 - "$workdir/report/rehearsal.json" <<'PY'
import json
from pathlib import Path
import sys


manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if (
    manifest["schema"] != "KAFS.V5V7MigrationRehearsal.v2"
    or manifest["status"] != "PASS"
    or manifest["fault_after_objects"] != 2
):
    raise SystemExit("rehearsal manifest does not record the passed fault boundary")
if manifest["recovery_strategy"] != "full-replay-from-frozen-source":
    raise SystemExit("rehearsal manifest changed its recovery strategy")
expected = {
    "normal", "replay_required", "replayed_accept", "rollback", "idempotence",
    "pending_ref_rejection", "bitmap_invalid_rejection",
}
if {case["id"] for case in manifest["cases"]} != expected:
    raise SystemExit("rehearsal manifest case matrix is incomplete")
PY
for bundle in normal-accept replay-required replayed-accept rollback; do
  [[ -s "$workdir/report/bundles/$bundle/artifacts.sha256" ]]
done
for image in source-v5 destination-v7 replay-v7 replay-attempt1-preserved \
  rollback-preserved-failed; do
  [[ -s "$workdir/report/artifacts/images/$image.img" ]]
done
[[ ! -e "$workdir/report/work" ]]

echo "v5-to-v7 migration rehearsal regression: PASS"
