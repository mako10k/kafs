#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

usage() {
  cat <<'USAGEEOF'
Usage:
  scripts/metadata-heatmap-report.sh [options]

Description:
  Run a write-heavy KAFS workload and generate a metadata heatmap report from:
  - mounted runtime counters: kafsctl fsstat --json
  - offline layout spans:     kafsdump --json

Options:
  --profile <none|conservative>  SD-card profile to mount with (default: conservative)
  --output-dir DIR               Report output directory (default: report/perf/metadata-heatmap-<timestamp>)
  --image-size-mib N             Test image size in MiB (default: 256)
  --files N                      Number of workload files (default: 240)
  --rounds N                     Append/rename workload rounds (default: 2)
  --keep-workdir                 Keep the temporary image/workdir after exit
  -h, --help                     Show this help
USAGEEOF
}

PROFILE="conservative"
STAMP=$(date +%Y%m%d-%H%M%S)
OUT_DIR="$ROOT_DIR/report/perf/metadata-heatmap-$STAMP"
IMAGE_SIZE_MIB=256
WORKLOAD_FILES=240
WORKLOAD_ROUNDS=2
KEEP_WORKDIR=0
WORKLOAD_TIMEOUT_SEC=${KAFS_HEATMAP_WORKLOAD_TIMEOUT_SEC:-120}
MOUNT_TIMEOUT_MS=${KAFS_HEATMAP_MOUNT_TIMEOUT_MS:-15000}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      [[ $# -ge 2 ]] || die "missing value for --profile"
      PROFILE="$2"
      shift 2
      ;;
    --output-dir)
      [[ $# -ge 2 ]] || die "missing value for --output-dir"
      OUT_DIR="$2"
      shift 2
      ;;
    --image-size-mib)
      [[ $# -ge 2 ]] || die "missing value for --image-size-mib"
      IMAGE_SIZE_MIB="$2"
      shift 2
      ;;
    --files)
      [[ $# -ge 2 ]] || die "missing value for --files"
      WORKLOAD_FILES="$2"
      shift 2
      ;;
    --rounds)
      [[ $# -ge 2 ]] || die "missing value for --rounds"
      WORKLOAD_ROUNDS="$2"
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

case "$PROFILE" in
  none|conservative)
    ;;
  *)
    die "invalid --profile: $PROFILE"
    ;;
esac

for n in "$IMAGE_SIZE_MIB" "$WORKLOAD_FILES" "$WORKLOAD_ROUNDS" "$WORKLOAD_TIMEOUT_SEC" \
  "$MOUNT_TIMEOUT_MS"; do
  [[ "$n" =~ ^[0-9]+$ ]] || die "numeric option is not an unsigned integer: $n"
done

[[ "$IMAGE_SIZE_MIB" -ge 32 ]] || die "--image-size-mib must be at least 32"
[[ "$WORKLOAD_FILES" -ge 1 ]] || die "--files must be at least 1"
[[ "$WORKLOAD_ROUNDS" -ge 1 ]] || die "--rounds must be at least 1"

KAFS_BIN="${KAFS_BIN:-$ROOT_DIR/src/kafs}"
MKFS_BIN="${MKFS_BIN:-$ROOT_DIR/src/mkfs.kafs}"
KAFSCTL_BIN="${KAFSCTL_BIN:-$ROOT_DIR/src/kafsctl}"
KAFSDUMP_BIN="${KAFSDUMP_BIN:-$ROOT_DIR/src/kafsdump}"
FSCK_BIN="${FSCK_BIN:-$ROOT_DIR/src/fsck.kafs}"

[[ -x "$KAFS_BIN" ]] || die "kafs binary missing: $KAFS_BIN"
[[ -x "$MKFS_BIN" ]] || die "mkfs.kafs binary missing: $MKFS_BIN"
[[ -x "$KAFSCTL_BIN" ]] || die "kafsctl binary missing: $KAFSCTL_BIN"
[[ -x "$KAFSDUMP_BIN" ]] || die "kafsdump binary missing: $KAFSDUMP_BIN"
[[ -x "$FSCK_BIN" ]] || die "fsck.kafs binary missing: $FSCK_BIN"
command -v python3 >/dev/null 2>&1 || die "python3 not found"

mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/kafs-metadata-heatmap.XXXXXX")
IMG="$WORKDIR/heatmap.img"
MNT="$WORKDIR/mnt"
LOG="$OUT_DIR/kafs.log"
FSSTAT_JSON="$OUT_DIR/fsstat.json"
KAFSDUMP_JSON="$OUT_DIR/kafsdump.json"
HEATMAP_JSON="$OUT_DIR/metadata-heatmap.json"
SUMMARY_MD="$OUT_DIR/SUMMARY.md"
KAFS_PID=""

is_mounted() {
  grep -Fq " $MNT " /proc/mounts 2>/dev/null
}

unmount_kafs() {
  if is_mounted; then
    fusermount3 -u "$MNT" 2>/dev/null || umount "$MNT" 2>/dev/null || true
  fi
}

cleanup() {
  set +e
  unmount_kafs
  if [[ -n "${KAFS_PID:-}" ]]; then
    kill "$KAFS_PID" 2>/dev/null || true
    wait "$KAFS_PID" 2>/dev/null || true
  fi
  if [[ "$KEEP_WORKDIR" -eq 0 ]]; then
    rm -rf "$WORKDIR" 2>/dev/null || true
  else
    echo "$WORKDIR" >"$OUT_DIR/workdir.txt"
  fi
}
trap cleanup EXIT

echo "=== KAFS metadata heatmap workload ==="
echo "report: $OUT_DIR"
echo "profile: $PROFILE"

mkdir -p "$MNT"
truncate -s "${IMAGE_SIZE_MIB}M" "$IMG"
"$MKFS_BIN" "$IMG" >"$OUT_DIR/mkfs.log" 2>&1

echo "1. Mount KAFS"
export KAFS_IMAGE="$IMG"
"$KAFS_BIN" "$MNT" -f -s -o "sd_card_profile=$PROFILE" >"$LOG" 2>&1 &
KAFS_PID=$!

MOUNTED=0
deadline=$((($(date +%s%3N) + MOUNT_TIMEOUT_MS)))
while [[ "$(date +%s%3N)" -lt "$deadline" ]]; do
  if is_mounted; then
    MOUNTED=1
    break
  fi
  sleep 0.1
done

if [[ "$MOUNTED" -eq 0 ]]; then
  echo "mount failed or timed out after ${MOUNT_TIMEOUT_MS} ms" >&2
  tail -80 "$LOG" >&2 || true
  if [[ ! -e /dev/fuse ]]; then
    echo "SKIP: /dev/fuse is unavailable" >&2
    exit 77
  fi
  exit 1
fi

chmod 777 "$MNT" 2>/dev/null || true

echo "2. Run write-heavy workload"
run_workload() {
  python3 - "$MNT" "$WORKLOAD_FILES" "$WORKLOAD_ROUNDS" <<'PY'
import errno
import os
from pathlib import Path
import sys

root = Path(sys.argv[1])
file_count = int(sys.argv[2])
rounds = int(sys.argv[3])

work = root / "heatmap-workload"
work.mkdir(parents=True, exist_ok=True)


def fsync_path(path):
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        try:
            os.fsync(fd)
        except OSError as exc:
            if exc.errno not in (errno.EINVAL, errno.EBADF):
                raise
    finally:
        os.close(fd)


def write_bytes(path, data, append=False):
    flags = os.O_WRONLY | os.O_CREAT
    if append:
        flags |= os.O_APPEND
    else:
        flags |= os.O_TRUNC
    fd = os.open(path, flags, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)


dirs = []
for i in range(16):
    d = work / f"dir-{i:02d}" / "nested"
    d.mkdir(parents=True, exist_ok=True)
    dirs.append(d)

for i in range(file_count):
    d = dirs[i % len(dirs)]
    payload = (f"file={i}\n".encode("ascii") * 32)
    if i % 5 == 0:
        seed = f"dedup-candidate-{i % 3}\n".encode("ascii")
        payload = (seed * ((8192 + len(seed) - 1) // len(seed)))[:8192]
    write_bytes(d / f"file-{i:04d}.dat", payload)

for r in range(rounds):
    for i in range(0, file_count, 3):
        if i % 5 == 0:
            continue
        d = dirs[i % len(dirs)]
        write_bytes(d / f"file-{i:04d}.dat", f"append round={r} file={i}\n".encode("ascii"),
                    append=True)
    for i in range(r, file_count, 7):
        d = dirs[i % len(dirs)]
        src = d / f"file-{i:04d}.dat"
        dst = d / f"renamed-{r:02d}-{i:04d}.dat"
        if src.exists():
            os.rename(src, dst)
    for i in range(r + 1, file_count, 11):
        d = dirs[i % len(dirs)]
        target = d / f"file-{i:04d}.dat"
        link = d / f"link-{r:02d}-{i:04d}.dat"
        try:
            os.link(target, link)
        except OSError:
            pass

for i in range(0, file_count, 13):
    d = dirs[i % len(dirs)]
    for name in (f"file-{i:04d}.dat", f"renamed-00-{i:04d}.dat"):
        p = d / name
        if p.exists():
            p.unlink()

for d in dirs:
    fsync_path(d)
fsync_path(work)
os.sync()
PY
}

if command -v timeout >/dev/null 2>&1; then
  export MNT WORKLOAD_FILES WORKLOAD_ROUNDS
  timeout --preserve-status "${WORKLOAD_TIMEOUT_SEC}s" bash -c "$(declare -f run_workload); run_workload"
else
  run_workload
fi

sync

echo "3. Capture runtime fsstat JSON"
"$KAFSCTL_BIN" fsstat "$MNT" --json >"$FSSTAT_JSON"

echo "4. Unmount and capture offline kafsdump JSON"
unmount_kafs
sleep 0.5
kill "$KAFS_PID" 2>/dev/null || true
wait "$KAFS_PID" 2>/dev/null || true
KAFS_PID=""

"$FSCK_BIN" "$IMG" >"$OUT_DIR/fsck.log" 2>&1
"$KAFSDUMP_BIN" --json "$IMG" >"$KAFSDUMP_JSON"

echo "5. Build heatmap summary"
python3 - "$FSSTAT_JSON" "$KAFSDUMP_JSON" "$HEATMAP_JSON" "$SUMMARY_MD" "$PROFILE" <<'PY'
import json
from pathlib import Path
import sys

fsstat_path, dump_path, heatmap_path, summary_path, profile = sys.argv[1:]
stats = json.loads(Path(fsstat_path).read_text(encoding="utf-8"))
dump = json.loads(Path(dump_path).read_text(encoding="utf-8"))

required_stats = [
    "version",
    "metadata_write_total",
    "metadata_write_bytes_total",
    "metadata_write_regions",
    "sd_card_profile_str",
    "trim_on_free",
    "atime_policy_str",
    "fsync_policy_str",
    "bg_dedup_enabled",
]
missing = [key for key in required_stats if key not in stats]
if missing:
    raise SystemExit(f"fsstat JSON missing fields: {', '.join(missing)}")

if "metadata_regions" not in dump:
    raise SystemExit("kafsdump JSON missing metadata_regions")
if not isinstance(stats["metadata_write_regions"], list):
    raise SystemExit("fsstat metadata_write_regions is not a list")
if not isinstance(dump["metadata_regions"], list):
    raise SystemExit("kafsdump metadata_regions is not a list")

if profile == "conservative":
    expected = {
        "sd_card_profile_str": "conservative",
        "trim_on_free": 0,
        "atime_policy_str": "no_runtime_updates",
        "fsync_policy_str": "journal_only",
        "bg_dedup_enabled": 0,
    }
else:
    expected = {
        "sd_card_profile_str": "none",
        "atime_policy_str": "no_runtime_updates",
    }
for key, want in expected.items():
    got = stats.get(key)
    if got != want:
        raise SystemExit(f"unexpected {key}: got {got!r}, want {want!r}")

runtime_by_name = {str(region.get("name")): region for region in stats["metadata_write_regions"]}
layout_by_name = {str(region.get("name")): region for region in dump["metadata_regions"]}

required_regions = [
    "superblock_checkpoint",
    "block_bitmap",
    "inode_table",
    "allocator_summary",
    "hrl_index",
    "hrl_entries",
    "journal_header",
    "journal_data",
    "pending_log",
    "tail_metadata",
    "unknown",
]
missing_runtime = [name for name in required_regions if name not in runtime_by_name]
missing_layout = [name for name in required_regions if name not in layout_by_name]
if missing_runtime:
    raise SystemExit(f"fsstat missing metadata regions: {', '.join(missing_runtime)}")
if missing_layout:
    raise SystemExit(f"kafsdump missing metadata regions: {', '.join(missing_layout)}")

metadata_total = int(stats["metadata_write_total"])
metadata_bytes_total = int(stats["metadata_write_bytes_total"])
if metadata_total <= 0 or metadata_bytes_total <= 0:
    raise SystemExit("metadata write counters did not increase")

unknown_writes = int(runtime_by_name["unknown"].get("writes", 0))
unknown_bytes = int(runtime_by_name["unknown"].get("bytes", 0))
if unknown_writes != 0 or unknown_bytes != 0:
    raise SystemExit("metadata writes were classified as unknown")

active_regions = [
    name for name in required_regions
    if name != "unknown" and int(runtime_by_name[name].get("writes", 0)) > 0
]
if len(active_regions) < 3:
    raise SystemExit("write-heavy workload touched fewer than three metadata regions")

regions = []
for name in required_regions:
    runtime = runtime_by_name[name]
    layout = layout_by_name[name]
    writes = int(runtime.get("writes", 0))
    bytes_written = int(runtime.get("bytes", 0))
    write_pct = (writes * 100.0 / metadata_total) if metadata_total else 0.0
    byte_pct = (bytes_written * 100.0 / metadata_bytes_total) if metadata_bytes_total else 0.0
    spans = layout.get("spans", [])
    regions.append({
        "id": int(runtime.get("id", layout.get("id", -1))),
        "name": name,
        "writes": writes,
        "bytes": bytes_written,
        "write_percent": round(write_pct, 3),
        "byte_percent": round(byte_pct, 3),
        "layout_available": bool(layout.get("available", False)),
        "layout_total_size": int(layout.get("total_size", 0)),
        "layout_span_count": len(spans),
        "layout_spans": spans,
    })

regions.sort(key=lambda item: item["id"])
heatmap = {
    "profile": profile,
    "fsstat_version": int(stats["version"]),
    "runtime_config": {
        "sd_card_profile": stats.get("sd_card_profile_str"),
        "trim_on_free": int(stats.get("trim_on_free", 0)),
        "atime_policy": stats.get("atime_policy_str"),
        "fsync_policy": stats.get("fsync_policy_str"),
        "bg_dedup_enabled": int(stats.get("bg_dedup_enabled", 0)),
    },
    "metadata_write_total": metadata_total,
    "metadata_write_bytes_total": metadata_bytes_total,
    "active_region_count": len(active_regions),
    "active_regions": active_regions,
    "regions": regions,
    "source_files": {
        "fsstat_json": str(Path(fsstat_path).name),
        "kafsdump_json": str(Path(dump_path).name),
    },
}
Path(heatmap_path).write_text(json.dumps(heatmap, indent=2, sort_keys=True) + "\n",
                              encoding="utf-8")

lines = [
    "# KAFS metadata heatmap report",
    "",
    f"- profile: `{profile}`",
    f"- fsstat_version: `{stats['version']}`",
    f"- metadata_write_total: `{metadata_total}`",
    f"- metadata_write_bytes_total: `{metadata_bytes_total}`",
    f"- active_regions: `{', '.join(active_regions)}`",
    "",
    "| id | region | writes | bytes | write % | layout bytes | spans |",
    "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
]
for region in regions:
    lines.append(
        f"| {region['id']} | `{region['name']}` | {region['writes']} | {region['bytes']} | "
        f"{region['write_percent']:.3f} | {region['layout_total_size']} | "
        f"{region['layout_span_count']} |"
    )
lines.extend([
    "",
    "## Source files",
    "",
    "- `fsstat.json`: raw `kafsctl fsstat --json` output",
    "- `kafsdump.json`: raw `kafsdump --json` output",
    "- `metadata-heatmap.json`: normalized joined heatmap data",
])
Path(summary_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

cat >"$OUT_DIR/COMMAND.txt" <<EOF
scripts/metadata-heatmap-report.sh --profile $PROFILE --output-dir $OUT_DIR --image-size-mib $IMAGE_SIZE_MIB --files $WORKLOAD_FILES --rounds $WORKLOAD_ROUNDS
EOF

echo "PASS: metadata heatmap report generated"
echo "summary: $SUMMARY_MD"
