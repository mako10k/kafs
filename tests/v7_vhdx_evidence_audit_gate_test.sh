#!/usr/bin/env bash
set -euo pipefail

gate=${KAFS_TEST_V7_VHDX_EVIDENCE_AUDIT_GATE:-../scripts/v7-vhdx-evidence-audit-gate.sh}
[[ -x "$gate" ]] || {
  echo "VHDX evidence audit gate is not executable: $gate" >&2
  exit 1
}

workdir=$(mktemp -d "${TMPDIR:-/tmp}/kafs-v7-vhdx-evidence-audit.XXXXXX")
cleanup() {
  find "$workdir" -depth -delete
}
trap cleanup EXIT

run="$workdir/20260721T000000Z-1234abcd"
mkdir -p "$run"

python3 - "$run" <<'PY'
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys

run = Path(sys.argv[1])
faults = (
    "journal_publish",
    "checkpoint_copy",
    "metadata_apply",
    "journal_reclaim",
)
base = datetime(2026, 7, 21, tzinfo=timezone.utc)


def timestamp(value):
    return value.isoformat().replace("+00:00", "Z")


def host_timestamp(value):
    return value.strftime("%Y-%m-%dT%H:%M:%S.%f0Z")


for index, fault in enumerate(faults):
    state = run / fault
    state.mkdir()
    mount_name = "mnt-reclaim-recovery" if fault == "journal_reclaim" else "mnt-controlled-recovery"
    (state / mount_name).mkdir()
    arm_time = base + timedelta(hours=index * 2)
    marker_time = arm_time + timedelta(minutes=10)
    terminated_time = arm_time + timedelta(minutes=11)
    restarted_time = arm_time + timedelta(minutes=12)
    verified_time = arm_time + timedelta(minutes=30)

    arm = {
        "schema": "KAFS.V7VhdxArmContext.v1",
        "created_utc": timestamp(arm_time),
        "fault": fault,
        "distro": "Ubuntu",
        "kernel": "6.6.87.2-microsoft-standard-WSL2",
        "filesystem_source": "/dev/sdd",
        "filesystem_type": "ext4",
        "git_head": "1" * 40,
        "tracked_worktree_dirty": False,
        "dedicated_regular_file_image": True,
        "raw_vhdx_access": False,
        "real_media_qualified": False,
        "physical_power_interruption": False,
        "controller_independent_wear_qualified": False,
        "release_candidate_qualified": False,
    }
    host = {
        "schema": "KAFS.V7VhdxHostController.v1",
        "fault": fault,
        "distro": "Ubuntu",
        "vhdx_path": r"C:\Users\synthetic\Ubuntu\ext4.vhdx",
        "vhdx_length_before": 120680611840 + index * 4096,
        "vhdx_length_after": 120680611840 + (index + 1) * 4096,
        "marker_observed_utc": host_timestamp(marker_time),
        "terminated_utc": host_timestamp(terminated_time),
        "restarted_utc": host_timestamp(restarted_time),
        "terminate_exit_code": 0,
        "restart_exit_code": 0,
        "controller": "wsl.exe --terminate",
        "raw_vhdx_access": False,
        "real_media_qualified": False,
        "physical_power_interruption": False,
        "controller_independent_wear_qualified": False,
        "release_candidate_qualified": False,
    }
    image = f"synthetic recovered image for {fault}\n".encode()
    image_sha = hashlib.sha256(image).hexdigest()
    manifest = {
        "schema": "KAFS.V7VhdxRecoveryEvidence.v1",
        "verified_utc": timestamp(verified_time),
        "fault": fault,
        "image_sha256_after_recovery": image_sha,
        "host_terminate_restart_observed": True,
        "fsck_full_check_passed": True,
        "kafsdump_completed": True,
        "payload_and_recovery_diagnostic_passed": True,
        "dedicated_regular_file_image": True,
        "raw_vhdx_access": False,
        "real_media_qualified": False,
        "physical_power_interruption": False,
        "controller_independent_wear_qualified": False,
        "release_candidate_qualified": False,
    }
    diagnostic = {
        "status": "completed",
        "format": "v7",
        "trigger": "controlled-admission",
        "resume_from": fault,
        "initial_checkpoint_generation": 1,
        "initial_checkpoint_sequence": 1,
        "initial_nonempty_segments": 1,
        "applied_targets": 0,
        "applied_mutations": 0,
        "already_applied_mutations": 0,
        "checkpoint_publications": 0,
        "checkpoint_resumes": 0,
        "reclaimed_segments": 0,
        "already_empty_segments": 0,
        "final_checkpoint_generation": 2,
        "final_checkpoint_sequence": 2,
        "final_nonempty_segments": 0,
    }
    if fault == "journal_publish":
        diagnostic["applied_targets"] = 2
        diagnostic["applied_mutations"] = 2
        diagnostic["already_applied_mutations"] = 1
    elif fault == "metadata_apply":
        diagnostic["already_applied_mutations"] = 3
    elif fault == "checkpoint_copy":
        diagnostic["checkpoint_publications"] = 1
        diagnostic["checkpoint_resumes"] = 1
    else:
        diagnostic["reclaimed_segments"] = 1

    (state / "arm-context.json").write_text(
        json.dumps(arm, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (state / "pause.marker").write_text(fault + "\n", encoding="utf-8")
    (state / "host-controller.json").write_text(
        json.dumps(host, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (state / "vhdx-recovery.meta").write_text(
        "schema=KAFS.V7VhdxRecoveryState.v1\n"
        f"fault={fault}\nino=42\nblock_size=4096\n",
        encoding="utf-8",
    )
    (state / "vhdx-recovery.img").write_bytes(image)
    recovery = "kafs-v7-recovery " + " ".join(
        f"{key}={value}" for key, value in diagnostic.items()
    )
    (state / f"v7-vhdx-{fault}-recovery.log").write_text(recovery + "\n", encoding="utf-8")
    (state / "vhdx-verify.ok").write_text(fault + "\n", encoding="utf-8")
    (state / "fsck-full-check.stdout").write_text("full check PASS\n", encoding="utf-8")
    (state / "fsck-full-check.stderr").write_text("", encoding="utf-8")
    dump = {
        "superblock": {"format_version": 7},
        "layout_descriptor": {"status": "ok", "available": True, "selected": True},
        "journal_segments": {"status": "ok", "selected": True, "nonempty_segment_count": 0},
    }
    (state / "kafsdump.json").write_text(
        json.dumps(dump, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (state / "kafsdump.stderr").write_text("", encoding="utf-8")
    (state / "verification-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    inventory = []
    for path in sorted(state.iterdir(), key=lambda item: item.name):
        if path.is_file() and path.name != "artifacts.sha256":
            inventory.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  ./{path.name}")
    (state / "artifacts.sha256").write_text("\n".join(inventory) + "\n", encoding="utf-8")
PY

"$gate" --run-dir "$run" --validate-only >/dev/null

expect_failure() {
  local label=$1
  shift
  if "$@" >/dev/null 2>&1; then
    echo "VHDX evidence audit unexpectedly accepted: $label" >&2
    exit 1
  fi
}

clone_run() {
  local run_id=$1
  local target="$workdir/$run_id"
  cp -a -- "$run" "$target"
  printf '%s\n' "$target"
}

refresh_inventory() {
  local state_dir=$1
  python3 - "$state_dir" <<'PY'
import hashlib
from pathlib import Path
import sys

state = Path(sys.argv[1])
lines = []
for path in sorted(state.iterdir(), key=lambda item: item.name):
    if path.is_file() and path.name != "artifacts.sha256":
        lines.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  ./{path.name}")
(state / "artifacts.sha256").write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
}

missing=$(clone_run 20260721T010000Z-00000001)
find "$missing/metadata_apply" -depth -delete
expect_failure missing-fault "$gate" --run-dir "$missing" --validate-only

unexpected=$(clone_run 20260721T020000Z-00000002)
mkdir "$unexpected/unexpected_fault"
expect_failure unexpected-fault "$gate" --run-dir "$unexpected" --validate-only

identity=$(clone_run 20260721T030000Z-00000003)
python3 - "$identity/checkpoint_copy/host-controller.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["vhdx_path"] = r"D:\Other\ext4.vhdx"
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
refresh_inventory "$identity/checkpoint_copy"
expect_failure identity-drift "$gate" --run-dir "$identity" --validate-only

claim=$(clone_run 20260721T040000Z-00000004)
python3 - "$claim/journal_publish/verification-manifest.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["real_media_qualified"] = True
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
refresh_inventory "$claim/journal_publish"
expect_failure claim-escalation "$gate" --run-dir "$claim" --validate-only

tampered=$(clone_run 20260721T050000Z-00000005)
printf 'tampered\n' >>"$tampered/journal_reclaim/vhdx-recovery.img"
expect_failure artifact-tamper "$gate" --run-dir "$tampered" --validate-only

unlisted=$(clone_run 20260721T060000Z-00000006)
printf 'unlisted\n' >"$unlisted/metadata_apply/unlisted.log"
expect_failure unlisted-artifact "$gate" --run-dir "$unlisted" --validate-only

nonempty_mount=$(clone_run 20260721T061000Z-0000000d)
printf 'unexpected\n' >"$nonempty_mount/metadata_apply/mnt-controlled-recovery/file"
expect_failure nonempty-mount "$gate" --run-dir "$nonempty_mount" --validate-only

substitute=$(clone_run 20260721T070000Z-00000007)
python3 - "$substitute/journal_publish/host-controller.json" \
  "$substitute/journal_publish/verification-manifest.json" <<'PY'
import json
from pathlib import Path
import sys
host_path = Path(sys.argv[1])
manifest_path = Path(sys.argv[2])
host = json.loads(host_path.read_text(encoding="utf-8"))
host["controller"] = "process-kill substitute"
host_path.write_text(json.dumps(host, indent=2, sort_keys=True) + "\n", encoding="utf-8")
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
manifest["host_terminate_restart_observed"] = False
manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
refresh_inventory "$substitute/journal_publish"
expect_failure process-kill-substitute "$gate" --run-dir "$substitute" --validate-only

incomplete=$(clone_run 20260721T080000Z-00000008)
find "$incomplete/checkpoint_copy/fsck-full-check.stdout" -delete
refresh_inventory "$incomplete/checkpoint_copy"
expect_failure incomplete-state "$gate" --run-dir "$incomplete" --validate-only

marker=$(clone_run 20260721T090000Z-00000009)
printf 'metadata_apply\n' >"$marker/journal_publish/pause.marker"
refresh_inventory "$marker/journal_publish"
expect_failure marker-mismatch "$gate" --run-dir "$marker" --validate-only

diagnostic=$(clone_run 20260721T100000Z-0000000a)
python3 - "$diagnostic/journal_reclaim/v7-vhdx-journal_reclaim-recovery.log" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1])
value = path.read_text(encoding="utf-8").replace(
    "resume_from=journal_reclaim", "resume_from=metadata_apply"
)
path.write_text(value, encoding="utf-8")
PY
refresh_inventory "$diagnostic/journal_reclaim"
expect_failure diagnostic-mismatch "$gate" --run-dir "$diagnostic" --validate-only

mutation=$(clone_run 20260721T101000Z-0000000e)
python3 - "$mutation/journal_publish/v7-vhdx-journal_publish-recovery.log" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1])
value = path.read_text(encoding="utf-8").replace(
    "already_applied_mutations=1", "already_applied_mutations=0"
)
path.write_text(value, encoding="utf-8")
PY
refresh_inventory "$mutation/journal_publish"
expect_failure mutation-count-mismatch "$gate" --run-dir "$mutation" --validate-only

timestamp_without_zone=$(clone_run 20260721T102000Z-0000000f)
python3 - "$timestamp_without_zone/journal_publish/host-controller.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["marker_observed_utc"] = data["marker_observed_utc"].removesuffix("Z")
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
refresh_inventory "$timestamp_without_zone/journal_publish"
expect_failure timestamp-without-zone "$gate" --run-dir "$timestamp_without_zone" --validate-only

submicrosecond_order=$(clone_run 20260721T103000Z-00000010)
python3 - "$submicrosecond_order/journal_publish/host-controller.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["marker_observed_utc"] = "2026-07-21T00:10:00.0000002Z"
data["terminated_utc"] = "2026-07-21T00:10:00.0000001Z"
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
refresh_inventory "$submicrosecond_order/journal_publish"
expect_failure submicrosecond-order "$gate" --run-dir "$submicrosecond_order" --validate-only

dump=$(clone_run 20260721T110000Z-0000000b)
python3 - "$dump/metadata_apply/kafsdump.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
data["superblock"]["format_version"] = 5
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
refresh_inventory "$dump/metadata_apply"
expect_failure dump-mismatch "$gate" --run-dir "$dump" --validate-only

overlap=$(clone_run 20260721T120000Z-0000000c)
python3 - "$overlap/checkpoint_copy/arm-context.json" \
  "$overlap/checkpoint_copy/host-controller.json" \
  "$overlap/checkpoint_copy/verification-manifest.json" <<'PY'
import json
from pathlib import Path
import sys

arm_path, host_path, manifest_path = map(Path, sys.argv[1:])
arm = json.loads(arm_path.read_text(encoding="utf-8"))
arm["created_utc"] = "2026-07-21T00:05:00Z"
arm_path.write_text(json.dumps(arm, indent=2, sort_keys=True) + "\n", encoding="utf-8")
host = json.loads(host_path.read_text(encoding="utf-8"))
host["marker_observed_utc"] = "2026-07-21T00:11:00Z"
host["terminated_utc"] = "2026-07-21T00:11:30Z"
host["restarted_utc"] = "2026-07-21T00:11:45Z"
host_path.write_text(json.dumps(host, indent=2, sort_keys=True) + "\n", encoding="utf-8")
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
manifest["verified_utc"] = "2026-07-21T00:20:00Z"
manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
refresh_inventory "$overlap/checkpoint_copy"
expect_failure overlapping-faults "$gate" --run-dir "$overlap" --validate-only

invalid_id="$workdir/not-a-controller-run-id"
cp -a -- "$run" "$invalid_id"
expect_failure invalid-run-id "$gate" --run-dir "$invalid_id" --validate-only

echo "v7 VHDX evidence audit gate regression: PASS"
