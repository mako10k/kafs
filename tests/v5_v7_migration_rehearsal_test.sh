#!/usr/bin/env bash
set -euo pipefail

runner=${KAFS_TEST_V5_V7_MIGRATION_REHEARSAL:-../scripts/v5-v7-migration-rehearsal.sh}
[[ -x "$runner" ]] || {
  echo "v5-to-v7 migration rehearsal runner is not executable: $runner" >&2
  exit 1
}

workdir=$(mktemp -d "${TMPDIR:-/tmp}/kafs-v5-v7-migration-rehearsal.XXXXXX")
cleanup() {
  find "$workdir" -depth -delete
}
trap cleanup EXIT

set +e
output=$("$runner" --report-dir "$workdir/report" \
  --timeout-ms "${KAFS_TEST_MOUNT_TIMEOUT_MS:-15000}" 2>&1)
rc=$?
set -e
if [[ "$rc" -eq 77 ]]; then
  echo "$output"
  exit 77
fi
if [[ "$rc" -ne 0 ]]; then
  echo "$output" >&2
  exit "$rc"
fi
grep -Fqx "KAFS_V5_V7_MIGRATION_REHEARSAL PASS" <<<"$output"
[[ -s "$workdir/report/rehearsal.json" ]]
python3 - "$workdir/report/rehearsal.json" <<'PY'
import json
from pathlib import Path
import sys


manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if manifest["status"] != "PASS" or manifest["fault_after_objects"] != 2:
    raise SystemExit("rehearsal manifest does not record the passed fault boundary")
if manifest["resume_semantics"] != "preserve-attempt-partial-and-replay-from-frozen-source":
    raise SystemExit("rehearsal manifest changed its restart semantics")
expected = {
    "normal", "resume_required", "resumed_accept", "rollback", "idempotence",
    "pending_ref_rejection", "bitmap_invalid_rejection",
}
if {case["id"] for case in manifest["cases"]} != expected:
    raise SystemExit("rehearsal manifest case matrix is incomplete")
PY
for bundle in normal-accept resume-required resumed-accept rollback; do
  [[ -s "$workdir/report/bundles/$bundle/artifacts.sha256" ]]
done
for image in source-v5 destination-v7 resume-v7 resume-attempt1-preserved \
  rollback-preserved-failed; do
  [[ -s "$workdir/report/artifacts/images/$image.img" ]]
done

echo "v5-to-v7 migration rehearsal regression: PASS"
