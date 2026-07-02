#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
DEFAULT_REPORT_ROOT="$ROOT_DIR/report/v6-controlled-write-acceptance"

IMAGE=""
YES=0
VALIDATE_ONLY=0
REPORT_ROOT="$DEFAULT_REPORT_ROOT"
REPORT_DIR=""
SMOKE_SCRIPT="${SMOKE_SCRIPT:-$ROOT_DIR/scripts/v6-controlled-write-smoke.sh}"
MOUNT_DIR=""
MOUNT_OPTIONS=""
TIMEOUT_MS=""
KEEP_WORKDIR=0
FAILURES=0
WARNINGS=0

usage() {
  cat <<'USAGEEOF'
Usage:
  scripts/v6-controlled-write-acceptance-gate.sh --image IMAGE --yes [options]
  scripts/v6-controlled-write-acceptance-gate.sh --report-dir DIR --validate-only

Description:
  Run and/or validate the experimental format v6 controlled-write smoke report.
  This is a pre-production evidence gate. Passing this gate does not approve
  production cutover; it only confirms that the controlled-write smoke artifacts
  are complete and internally consistent.

Options:
  --image IMAGE              Existing v6 image to mutate through the smoke helper
  --yes                      Required when --image is used
  --report-root DIR          Parent directory for gate reports
                             (default: report/v6-controlled-write-acceptance)
  --report-dir DIR           Exact report directory to create/use or validate
  --validate-only            Validate an existing --report-dir without running smoke
  --smoke-script PATH        Smoke helper path
                             (default: scripts/v6-controlled-write-smoke.sh)
  --mount-dir DIR            Passed to the smoke helper
  --mount-options OPTS       Passed to the smoke helper
  --timeout-ms N             Passed to the smoke helper
  --keep-workdir             Passed to the smoke helper
  -h, --help                 Show this help

Environment:
  SMOKE_SCRIPT can override the default smoke helper path.
  KAFS_BIN, KAFSDUMP_BIN, FSCK_BIN and the smoke-specific environment variables
  are honored by the smoke helper when this gate runs it.
USAGEEOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

fail() {
  echo "FAIL: $*" >&2
  FAILURES=$((FAILURES + 1))
}

warn() {
  echo "WARN: $*" >&2
  WARNINGS=$((WARNINGS + 1))
}

abs_existing_path() {
  local path="$1"
  local dir
  local base

  dir=$(dirname "$path")
  base=$(basename "$path")
  (cd "$dir" && printf "%s/%s\n" "$(pwd)" "$base")
}

validate_unsigned() {
  local label="$1"
  local value="$2"

  [[ "$value" =~ ^[0-9]+$ ]] || die "$label must be an unsigned integer: $value"
}

require_file() {
  local path="$1"
  local label="$2"

  if [[ ! -f "$path" ]]; then
    fail "missing $label: $path"
    return 1
  fi
  return 0
}

require_status_zero() {
  local path="$1"
  local label="$2"
  local value

  require_file "$path" "$label" || return 1
  value=$(tr -d '[:space:]' <"$path")
  if [[ "$value" != "0" ]]; then
    fail "$label is not zero: ${value:-<empty>}"
    return 1
  fi
  return 0
}

require_contains() {
  local path="$1"
  local needle="$2"
  local label="$3"

  require_file "$path" "$label" || return 1
  if ! grep -Fq -- "$needle" "$path"; then
    fail "$label missing expected text: $needle"
    return 1
  fi
  return 0
}

require_not_contains() {
  local path="$1"
  local needle="$2"
  local label="$3"

  require_file "$path" "$label" || return 1
  if grep -Fq -- "$needle" "$path"; then
    fail "$label contains unsupported evidence marker: $needle"
    return 1
  fi
  return 0
}

require_combined_contains() {
  local label="$1"
  local needle="$2"
  shift 2
  local path

  for path in "$@"; do
    if [[ -f "$path" ]] && grep -Fq -- "$needle" "$path"; then
      return 0
    fi
  done
  fail "$label missing expected text across artifacts: $needle"
  return 1
}

validate_json() {
  local path="$1"
  local label="$2"

  require_file "$path" "$label" || return 1
  if ! python3 - "$path" <<'PYEOF'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    json.load(f)
PYEOF
  then
    fail "$label is not valid JSON: $path"
    return 1
  fi
  return 0
}

manifest_value() {
  local key="$1"
  local manifest="$REPORT_DIR/manifest.env"

  sed -n "s/^${key}=//p" "$manifest" | head -n 1
}

require_mount_token() {
  local opts="$1"
  local token="$2"

  case ",$opts," in
    *,"$token",*) return 0 ;;
  esac
  fail "manifest mount_options missing required token: $token"
  return 1
}

require_any_mount_token() {
  local opts="$1"
  local label="$2"
  shift 2
  local token

  for token in "$@"; do
    case ",$opts," in
      *,"$token",*) return 0 ;;
    esac
  done
  fail "manifest mount_options missing required token: $label"
  return 1
}

reject_mount_token() {
  local opts="$1"
  local token="$2"

  case ",$opts," in
    *,"$token",*)
      fail "manifest mount_options contains refused token: $token"
      return 1
      ;;
  esac
  return 0
}

run_smoke() {
  local -a cmd

  [[ -x "$SMOKE_SCRIPT" ]] || die "smoke script is not executable: $SMOKE_SCRIPT"
  SMOKE_SCRIPT=$(abs_existing_path "$SMOKE_SCRIPT")

  [[ -n "$IMAGE" ]] || die "--image is required unless --validate-only is used"
  [[ "$YES" -eq 1 ]] || die "--yes is required because the smoke mutates IMAGE"
  [[ -f "$IMAGE" ]] || die "image not found: $IMAGE"
  IMAGE=$(abs_existing_path "$IMAGE")

  if [[ -z "$REPORT_DIR" ]]; then
    REPORT_DIR="$REPORT_ROOT/$STAMP"
  fi
  mkdir -p "$REPORT_DIR"
  REPORT_DIR=$(cd "$REPORT_DIR" && pwd)

  cmd=("$SMOKE_SCRIPT" --image "$IMAGE" --yes --report-dir "$REPORT_DIR")
  if [[ -n "$MOUNT_DIR" ]]; then
    cmd+=(--mount-dir "$MOUNT_DIR")
  fi
  if [[ -n "$MOUNT_OPTIONS" ]]; then
    cmd+=(--mount-options "$MOUNT_OPTIONS")
  fi
  if [[ -n "$TIMEOUT_MS" ]]; then
    cmd+=(--timeout-ms "$TIMEOUT_MS")
  fi
  if [[ "$KEEP_WORKDIR" -eq 1 ]]; then
    cmd+=(--keep-workdir)
  fi

  printf "%q " "${cmd[@]}" >"$REPORT_DIR/acceptance-gate-smoke.cmd"
  printf "\n" >>"$REPORT_DIR/acceptance-gate-smoke.cmd"

  set +e
  "${cmd[@]}" >"$REPORT_DIR/acceptance-gate-smoke.stdout" \
    2>"$REPORT_DIR/acceptance-gate-smoke.stderr"
  local rc=$?
  set -e
  printf "%s\n" "$rc" >"$REPORT_DIR/acceptance-gate-smoke.status"
  if [[ "$rc" -ne 0 ]]; then
    fail "smoke helper failed with rc=$rc; validating preserved artifacts"
  fi
}

validate_report() {
  local manifest="$REPORT_DIR/manifest.env"
  local mount_opts
  local before_digest
  local after_digest
  local report_image

  [[ -n "$REPORT_DIR" ]] || die "--report-dir is required for validation"
  [[ -d "$REPORT_DIR" ]] || die "report directory not found: $REPORT_DIR"
  REPORT_DIR=$(cd "$REPORT_DIR" && pwd)

  require_file "$manifest" "manifest"
  require_file "$REPORT_DIR/SUMMARY.md" "smoke summary"
  require_file "$REPORT_DIR/workload.py" "workload script"
  require_file "$REPORT_DIR/mount.cmd" "mount command"
  require_file "$REPORT_DIR/mount.log" "mount log"
  require_file "$REPORT_DIR/workload.stdout" "workload stdout"
  require_file "$REPORT_DIR/workload.stderr" "workload stderr"
  require_file "$REPORT_DIR/unmount.stdout" "unmount stdout"
  require_file "$REPORT_DIR/unmount.stderr" "unmount stderr"
  require_file "$REPORT_DIR/image-before.stat" "before image stat"
  require_file "$REPORT_DIR/image-after.stat" "after image stat"
  require_file "$REPORT_DIR/image-before.sha256" "before image digest"
  require_file "$REPORT_DIR/image-after.sha256" "after image digest"

  require_status_zero "$REPORT_DIR/mount.status" "mount status"
  require_status_zero "$REPORT_DIR/mount-log-admission.status" "mount admission log status"
  require_status_zero "$REPORT_DIR/workload.status" "workload status"
  require_status_zero "$REPORT_DIR/unmount.status" "unmount status"
  require_status_zero "$REPORT_DIR/kafsdump-before.status" "before kafsdump status"
  require_status_zero "$REPORT_DIR/kafsdump-after.status" "after kafsdump status"
  require_status_zero "$REPORT_DIR/fsck-before.status" "before fsck status"
  require_status_zero "$REPORT_DIR/fsck-after.status" "after fsck status"

  validate_json "$REPORT_DIR/kafsdump-before.json" "before kafsdump JSON"
  validate_json "$REPORT_DIR/kafsdump-after.json" "after kafsdump JSON"

  require_contains "$REPORT_DIR/SUMMARY.md" "status: \`PASS\`" "smoke summary"
  require_contains "$REPORT_DIR/mount.log" "format v6 controlled write mount" "mount log"
  require_contains "$REPORT_DIR/workload.stdout" \
    "regular-file create/write/fsync workload passed" "workload stdout"
  require_contains "$REPORT_DIR/mount.cmd" "kafs-v6" "mount command"
  require_contains "$REPORT_DIR/mount.cmd" "--controlled-write-mount" "mount command"
  require_contains "$REPORT_DIR/fsck-before.cmd" "--balanced-check" "before fsck command"
  require_contains "$REPORT_DIR/fsck-after.cmd" "--balanced-check" "after fsck command"

  require_combined_contains "before fsck output" "format v6 fsck policy: detect-only validation" \
    "$REPORT_DIR/fsck-before.stdout" "$REPORT_DIR/fsck-before.stderr"
  require_combined_contains "after fsck output" "format v6 fsck policy: detect-only validation" \
    "$REPORT_DIR/fsck-after.stdout" "$REPORT_DIR/fsck-after.stderr"
  require_combined_contains "after fsck output" "v6 journal segments:" \
    "$REPORT_DIR/fsck-after.stdout" "$REPORT_DIR/fsck-after.stderr"

  mount_opts=$(manifest_value mount_options)
  if [[ -z "$mount_opts" ]]; then
    fail "manifest missing mount_options"
  else
    require_mount_token "$mount_opts" "rw"
    require_any_mount_token "$mount_opts" "no_writeback_cache" "no_writeback_cache" \
      "no-writeback-cache"
    require_any_mount_token "$mount_opts" "no_trim_on_free" "no_trim_on_free" \
      "no-trim-on-free"
    require_any_mount_token "$mount_opts" "bg_dedup_scan=off" "bg_dedup_scan=off" \
      "dedup_scan=off" "no_bg_dedup_scan" "no-bg-dedup-scan"
    require_mount_token "$mount_opts" "fsync_policy=full"
    reject_mount_token "$mount_opts" "ro"
    reject_mount_token "$mount_opts" "v6_write_mount"
    reject_mount_token "$mount_opts" "v6-write-mount"
    reject_mount_token "$mount_opts" "v6_inspection_mount"
    reject_mount_token "$mount_opts" "v6-inspection-mount"
    reject_mount_token "$mount_opts" "writeback_cache"
    reject_mount_token "$mount_opts" "writeback-cache"
    reject_mount_token "$mount_opts" "trim_on_free"
    reject_mount_token "$mount_opts" "trim-on-free"
    reject_mount_token "$mount_opts" "bg_dedup_scan=on"
    reject_mount_token "$mount_opts" "dedup_scan=on"
  fi

  report_image=$(manifest_value image)
  if [[ -z "$report_image" ]]; then
    fail "manifest missing image"
  elif [[ ! -f "$report_image" ]]; then
    warn "manifest image is not present on this host: $report_image"
  fi

  before_digest=$(awk 'NR == 1 { print $1 }' "$REPORT_DIR/image-before.sha256")
  after_digest=$(awk 'NR == 1 { print $1 }' "$REPORT_DIR/image-after.sha256")
  if [[ -z "$before_digest" || -z "$after_digest" ]]; then
    fail "image digest files are empty"
  elif [[ "$before_digest" == "$after_digest" ]]; then
    fail "image digest did not change across controlled write smoke"
  fi

  require_not_contains "$REPORT_DIR/workload.py" "copy_file_range" "workload script"
  require_not_contains "$REPORT_DIR/workload.py" "FICLONE" "workload script"
  require_not_contains "$REPORT_DIR/workload.py" "KAFS_IOCTL_COPY" "workload script"
}

write_gate_summary() {
  local status="PASS"
  if [[ "$FAILURES" -ne 0 ]]; then
    status="FAIL"
  fi

  cat >"$REPORT_DIR/acceptance-gate.SUMMARY.md" <<EOF
# KAFS v6 Controlled Write Acceptance Gate

- status: \`$status\`
- failures: \`$FAILURES\`
- warnings: \`$WARNINGS\`
- report_dir: \`$REPORT_DIR\`

This is a pre-production evidence gate for the experimental controlled write
surface. Passing this gate does not approve production cutover.

## Required Evidence Checked

- controlled write mount admission log
- regular-file create/write/fsync workload success
- before/after \`kafsdump --json\` JSON
- before/after \`fsck.kafs --balanced-check\` success
- before/after image stat and digest
- no copy/reflink workload evidence
EOF

  if [[ "$status" == "PASS" ]]; then
    printf "0\n" >"$REPORT_DIR/acceptance-gate.status"
  else
    printf "1\n" >"$REPORT_DIR/acceptance-gate.status"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image)
      [[ $# -ge 2 ]] || die "missing value for --image"
      IMAGE="$2"
      shift 2
      ;;
    --yes)
      YES=1
      shift
      ;;
    --report-root)
      [[ $# -ge 2 ]] || die "missing value for --report-root"
      REPORT_ROOT="$2"
      shift 2
      ;;
    --report-dir)
      [[ $# -ge 2 ]] || die "missing value for --report-dir"
      REPORT_DIR="$2"
      shift 2
      ;;
    --validate-only)
      VALIDATE_ONLY=1
      shift
      ;;
    --smoke-script)
      [[ $# -ge 2 ]] || die "missing value for --smoke-script"
      SMOKE_SCRIPT="$2"
      shift 2
      ;;
    --mount-dir)
      [[ $# -ge 2 ]] || die "missing value for --mount-dir"
      MOUNT_DIR="$2"
      shift 2
      ;;
    --mount-options)
      [[ $# -ge 2 ]] || die "missing value for --mount-options"
      MOUNT_OPTIONS="$2"
      shift 2
      ;;
    --timeout-ms)
      [[ $# -ge 2 ]] || die "missing value for --timeout-ms"
      TIMEOUT_MS="$2"
      validate_unsigned "--timeout-ms" "$TIMEOUT_MS"
      [[ "$TIMEOUT_MS" -gt 0 ]] || die "--timeout-ms must be greater than zero"
      shift 2
      ;;
    --keep-workdir)
      KEEP_WORKDIR=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      die "unknown option: $1"
      ;;
  esac
done

if [[ "$VALIDATE_ONLY" -eq 1 && -n "$IMAGE" ]]; then
  die "--validate-only cannot be combined with --image"
fi

if [[ "$VALIDATE_ONLY" -eq 0 ]]; then
  run_smoke
else
  [[ -n "$REPORT_DIR" ]] || die "--report-dir is required with --validate-only"
fi

validate_report
write_gate_summary

if [[ "$FAILURES" -ne 0 ]]; then
  echo "v6 controlled write acceptance gate FAIL"
  echo "report: $REPORT_DIR"
  exit 1
fi

echo "v6 controlled write acceptance gate PASS"
echo "report: $REPORT_DIR"
echo "NOTE: this is not production cutover approval"
