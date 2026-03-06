#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

usage() {
  cat <<'USAGEEOF'
Usage:
  scripts/benchmark-native-vs-kafs.sh [--quick|--full] [--repeats N] [--cache-bust-mb N]

Description:
  Compare native directory vs KAFS mount variants across workload patterns:
  - bulk/chunked read/write
  - mixed read/write
  - copy-heavy
  - archive compress/extract
  - high-load mixed workload

Options:
  --cache-bust-mb N   Read N MiB cache-buster file before read-heavy workloads (default: 0)
USAGEEOF
}

PROFILE="quick"
REPEATS=1
CACHE_BUST_MB=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --quick)
      PROFILE="quick"
      shift
      ;;
    --full)
      PROFILE="full"
      shift
      ;;
    --repeats)
      REPEATS="$2"
      shift 2
      ;;
    --cache-bust-mb)
      CACHE_BUST_MB="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if ! [[ "$REPEATS" =~ ^[0-9]+$ ]] || [[ "$REPEATS" -lt 1 ]]; then
  echo "invalid repeats: $REPEATS" >&2
  exit 2
fi
if ! [[ "$CACHE_BUST_MB" =~ ^[0-9]+$ ]]; then
  echo "invalid cache-bust-mb: $CACHE_BUST_MB" >&2
  exit 2
fi

if [[ "$PROFILE" == "quick" ]]; then
  BULK_MB=64
  CHUNK_MB=32
  SMALL_FILES=100
  SMALL_KB=4
  MED_FILES=10
  MED_KB=128
  LARGE_FILES=2
  LARGE_MB=2
  RAND_READ_OPS=2000
  COPY_REPS=1
  HIGHLOAD_COPY_REPS=1
  HIGHLOAD_READ_OPS=1500
else
  BULK_MB=384
  CHUNK_MB=192
  SMALL_FILES=2000
  SMALL_KB=4
  MED_FILES=200
  MED_KB=256
  LARGE_FILES=20
  LARGE_MB=4
  RAND_READ_OPS=50000
  COPY_REPS=3
  HIGHLOAD_COPY_REPS=2
  HIGHLOAD_READ_OPS=5000
fi

WORKLOAD_TIMEOUT_SEC=${WORKLOAD_TIMEOUT_SEC:-90}

if ! command -v /usr/bin/time >/dev/null 2>&1; then
  echo "/usr/bin/time not found" >&2
  exit 2
fi

STAMP=$(date +%Y%m%d-%H%M%S)
OUT_DIR="$ROOT_DIR/report/perf/native-vs-kafs-$STAMP"
mkdir -p "$OUT_DIR"
RESULT_CSV="$OUT_DIR/results.csv"
SUMMARY_MD="$OUT_DIR/SUMMARY.md"
COPY_FASTPATH_CSV="$OUT_DIR/copy_fastpath.csv"

echo "target,repeat,workload,seconds,status" >"$RESULT_CSV"
echo "target,repeat,attempt_blocks,done_blocks,fallback_blocks,skip_unaligned,skip_dst_inline" >"$COPY_FASTPATH_CSV"

KAFS_BIN="$ROOT_DIR/src/kafs"
MKFS_BIN="$ROOT_DIR/src/mkfs.kafs"
KAFSCTL_BIN="$ROOT_DIR/src/kafsctl"

if [[ ! -x "$KAFS_BIN" || ! -x "$MKFS_BIN" ]]; then
  echo "kafs/mkfs.kafs binaries are missing. Run make first." >&2
  exit 2
fi

TMP_BASE=$(mktemp -d "${TMPDIR:-/tmp}/kafs-native-vs.XXXXXX")
CACHE_BUST_FILE="$TMP_BASE/cache-bust.bin"

prepare_cache_bust_file() {
  if [[ "$CACHE_BUST_MB" -le 0 ]]; then
    return 0
  fi
  dd if=/dev/zero of="$CACHE_BUST_FILE" bs=1M count="$CACHE_BUST_MB" status=none
}

run_cache_bust() {
  if [[ "$CACHE_BUST_MB" -le 0 ]]; then
    return 0
  fi
  dd if="$CACHE_BUST_FILE" of=/dev/null bs=4M status=none
}

cleanup_all() {
  set +e
  if [[ -n "${KAFS_PID:-}" ]]; then
    kill "$KAFS_PID" 2>/dev/null || true
    wait "$KAFS_PID" 2>/dev/null || true
  fi
  if [[ -n "${KAFS_MNT:-}" ]]; then
    fusermount3 -u "$KAFS_MNT" 2>/dev/null || umount "$KAFS_MNT" 2>/dev/null || true
  fi
  rm -rf "$TMP_BASE" 2>/dev/null || true
}
trap cleanup_all EXIT

prepare_cache_bust_file

record_result() {
  local target="$1"
  local repeat="$2"
  local workload="$3"
  local seconds="$4"
  local status="$5"
  echo "$target,$repeat,$workload,$seconds,$status" >>"$RESULT_CSV"
}

capture_copy_stats_json() {
  local target="$1"
  local repeat="$2"
  local phase="$3"
  local mount_root="$4"
  local out="$OUT_DIR/${target}.r${repeat}.copy_${phase}.json"

  if [[ "$target" == "native" ]]; then
    return 0
  fi
  if [[ ! -x "$KAFSCTL_BIN" ]]; then
    return 0
  fi
  "$KAFSCTL_BIN" fsstat "$mount_root" --json >"$out" 2>/dev/null || true
}

record_copy_stats_delta() {
  local target="$1"
  local repeat="$2"
  local before_json="$OUT_DIR/${target}.r${repeat}.copy_before.json"
  local after_json="$OUT_DIR/${target}.r${repeat}.copy_after.json"

  if [[ ! -f "$before_json" || ! -f "$after_json" ]]; then
    return 0
  fi

  python3 - "$before_json" "$after_json" "$target" "$repeat" >>"$COPY_FASTPATH_CSV" <<'PY'
import json
import sys

before_p, after_p, target, repeat = sys.argv[1:]
with open(before_p, "r", encoding="utf-8") as f:
    b = json.load(f)
with open(after_p, "r", encoding="utf-8") as f:
    a = json.load(f)

keys = [
    "copy_share_attempt_blocks",
    "copy_share_done_blocks",
    "copy_share_fallback_blocks",
    "copy_share_skip_unaligned",
    "copy_share_skip_dst_inline",
]
vals = [max(0, int(a.get(k, 0)) - int(b.get(k, 0))) for k in keys]
print(f"{target},{repeat},{vals[0]},{vals[1]},{vals[2]},{vals[3]},{vals[4]}")
PY
}

run_timed() {
  local target="$1"
  local repeat="$2"
  local workload="$3"
  shift 3

  local sec_file="$OUT_DIR/.${target}.${repeat}.${workload}.sec"
  local log_file="$OUT_DIR/${target}.r${repeat}.${workload}.log"

  if /usr/bin/time -f "%e" -o "$sec_file" timeout --preserve-status "$WORKLOAD_TIMEOUT_SEC" "$@" >"$log_file" 2>&1; then
    local sec=""
    if [[ -f "$sec_file" ]]; then
      sec=$(grep -Eo '[0-9]+(\.[0-9]+)?' "$sec_file" | tail -n 1 || true)
    fi
    if [[ -z "$sec" ]]; then
      sec="NaN"
    fi
    record_result "$target" "$repeat" "$workload" "$sec" "ok"
  else
    local sec=""
    if [[ -f "$sec_file" ]]; then
      sec=$(grep -Eo '[0-9]+(\.[0-9]+)?' "$sec_file" | tail -n 1 || true)
    fi
    record_result "$target" "$repeat" "$workload" "${sec:-NaN}" "fail"
  fi
}

prepare_dataset() {
  local base_dir="$1"
  local ds="$base_dir/dataset"
  DS="$ds" SMALL_FILES="$SMALL_FILES" SMALL_KB="$SMALL_KB" MED_FILES="$MED_FILES" \
    MED_KB="$MED_KB" LARGE_FILES="$LARGE_FILES" LARGE_MB="$LARGE_MB" \
    python3 - <<'PY'
import os
import random

root = os.environ["DS"]
small_files = int(os.environ["SMALL_FILES"])
small_kb = int(os.environ["SMALL_KB"])
med_files = int(os.environ["MED_FILES"])
med_kb = int(os.environ["MED_KB"])
large_files = int(os.environ["LARGE_FILES"])
large_mb = int(os.environ["LARGE_MB"])

r = random.Random(12345)
os.makedirs(root, exist_ok=True)

def write_file(path, size):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        remain = size
        while remain > 0:
            n = min(remain, 1024 * 1024)
            f.write(r.randbytes(n))
            remain -= n

for i in range(small_files):
    write_file(os.path.join(root, "small", f"s{i:05d}.bin"), small_kb * 1024)
for i in range(med_files):
    write_file(os.path.join(root, "med", f"m{i:05d}.bin"), med_kb * 1024)
for i in range(large_files):
    write_file(os.path.join(root, "large", f"l{i:05d}.bin"), large_mb * 1024 * 1024)
PY
}

run_workloads_for_target() {
  local target="$1"
  local root="$2"

  for r in $(seq 1 "$REPEATS"); do
    local wd="$root/run_$r"
    rm -rf "$wd"
    mkdir -p "$wd"

    run_timed "$target" "$r" "bulk_write" dd if=/dev/zero of="$wd/bulk.bin" bs=1M count="$BULK_MB" conv=fsync status=none

    run_timed "$target" "$r" "chunked_write" env OUT_FILE="$wd/chunked.bin" CHUNK_MB="$CHUNK_MB" python3 - <<'PY'
import os
path = os.environ["OUT_FILE"]
total = int(os.environ["CHUNK_MB"]) * 1024 * 1024
chunk = 4096
buf = b"A" * chunk
with open(path, "wb") as f:
    remain = total
    while remain > 0:
        n = chunk if remain >= chunk else remain
        f.write(buf[:n])
        remain -= n
    f.flush()
    os.fsync(f.fileno())
PY

    run_cache_bust
    run_timed "$target" "$r" "bulk_read" dd if="$wd/bulk.bin" of=/dev/null bs=4M status=none

    run_cache_bust
    run_timed "$target" "$r" "chunked_read" env IN_FILE="$wd/chunked.bin" OPS="$RAND_READ_OPS" python3 - <<'PY'
import os
import random
path = os.environ["IN_FILE"]
ops = int(os.environ["OPS"])
size = os.path.getsize(path)
r = random.Random(123)
chunk = 4096
with open(path, "rb") as f:
    for _ in range(ops):
        off = (r.randrange(max(1, size // chunk))) * chunk
        f.seek(off)
        _ = f.read(chunk)
PY

    run_cache_bust
    run_timed "$target" "$r" "mixed_rw" env WD="$wd" CHUNK_MB="$CHUNK_MB" python3 - <<'PY'
import os
import threading
import random

wd = os.environ["WD"]
chunk_total = int(os.environ["CHUNK_MB"]) * 1024 * 1024
src = os.path.join(wd, "chunked.bin")
dst = os.path.join(wd, "mixed-write.bin")
chunk = 4096
r = random.Random(456)

def writer():
    remain = chunk_total
    with open(dst, "wb") as f:
        while remain > 0:
            n = chunk if remain >= chunk else remain
            f.write(b"Z" * n)
            remain -= n
        f.flush()
        os.fsync(f.fileno())

def reader():
    if not os.path.exists(src):
        return
    size = os.path.getsize(src)
    with open(src, "rb") as f:
        for _ in range(15000):
            off = (r.randrange(max(1, size // chunk))) * chunk
            f.seek(off)
            _ = f.read(chunk)

t1 = threading.Thread(target=writer)
t2 = threading.Thread(target=reader)
t1.start(); t2.start(); t1.join(); t2.join()
PY

    prepare_dataset "$wd"

    run_cache_bust
    capture_copy_stats_json "$target" "$r" "before" "$root"
    run_timed "$target" "$r" "copy_heavy" bash -lc "set -euo pipefail; rm -rf '$wd/copies'; mkdir -p '$wd/copies'; for i in \$(seq 1 '$COPY_REPS'); do cp -a '$wd/dataset' '$wd/copies/ds_'\"\$i\"; done"
    capture_copy_stats_json "$target" "$r" "after" "$root"
    record_copy_stats_delta "$target" "$r"

    run_cache_bust
    run_timed "$target" "$r" "archive_compress_extract" bash -lc "set -euo pipefail; rm -rf '$wd/archive'; mkdir -p '$wd/archive/out'; tar -C '$wd' -cf '$wd/archive/dataset.tar' dataset; gzip -f '$wd/archive/dataset.tar'; tar -C '$wd/archive/out' -xzf '$wd/archive/dataset.tar.gz'"

    run_cache_bust
    run_timed "$target" "$r" "high_load" bash -lc "set -euo pipefail; rm -rf '$wd/high'; mkdir -p '$wd/high'; \
      (find '$wd/dataset' -type f -print0 | xargs -0 -n 1 -P 4 sha1sum > '$wd/high/sha1.log') & \
      (for i in \$(seq 1 '$HIGHLOAD_COPY_REPS'); do cp -a '$wd/dataset' '$wd/high/copy_'\"\$i\"; done) & \
      (tar -C '$wd' -czf '$wd/high/high.tar.gz' dataset && mkdir -p '$wd/high/ext' && tar -C '$wd/high/ext' -xzf '$wd/high/high.tar.gz') & \
      (python3 - <<'PY'
import os, random
root = os.path.join('$wd', 'dataset')
paths = []
for d, _, fs in os.walk(root):
    for f in fs:
        paths.append(os.path.join(d, f))
r = random.Random(999)
for _ in range($HIGHLOAD_READ_OPS):
    p = paths[r.randrange(len(paths))]
    with open(p, 'rb') as fd:
        fd.read(4096)
PY
      ) & \
      wait"
  done
}

mount_kafs_target() {
  local name="$1"
  local opts="$2"
  KAFS_IMG="$TMP_BASE/${name}.img"
  KAFS_MNT="$TMP_BASE/${name}.mnt"
  KAFS_LOG="$OUT_DIR/${name}.kafs.log"
  mkdir -p "$KAFS_MNT"
  truncate -s 2G "$KAFS_IMG"
  "$MKFS_BIN" "$KAFS_IMG" >/dev/null 2>&1

  KAFS_PID=""
  "$KAFS_BIN" --image "$KAFS_IMG" "$KAFS_MNT" -f $opts >"$KAFS_LOG" 2>&1 &
  KAFS_PID=$!

  local mounted=0
  for _ in {1..200}; do
    if grep -Fq "$KAFS_MNT" /proc/mounts 2>/dev/null; then
      mounted=1
      break
    fi
    sleep 0.1
  done
  if [[ "$mounted" -ne 1 ]]; then
    echo "mount failed for $name (opts: $opts). see $KAFS_LOG" >&2
    return 1
  fi
  chmod 777 "$KAFS_MNT" || true
  return 0
}

unmount_kafs_target() {
  set +e
  if [[ -n "${KAFS_MNT:-}" ]]; then
    fusermount3 -u "$KAFS_MNT" 2>/dev/null || umount "$KAFS_MNT" 2>/dev/null || true
  fi
  if [[ -n "${KAFS_PID:-}" ]]; then
    kill "$KAFS_PID" 2>/dev/null || true
    wait "$KAFS_PID" 2>/dev/null || true
  fi
  KAFS_MNT=""
  KAFS_PID=""
}

echo "[bench] profile=$PROFILE repeats=$REPEATS"
echo "[bench] cache_bust_mb=$CACHE_BUST_MB"
echo "[bench] output=$OUT_DIR"

NATIVE_ROOT="$TMP_BASE/native"
mkdir -p "$NATIVE_ROOT"
run_workloads_for_target "native" "$NATIVE_ROOT"

declare -a KAFS_TARGETS=(
  "kafs_default|-s"
  "kafs_no_wbc|-s -o no_writeback_cache"
  "kafs_mt8|-o multi_thread=8"
)

for ent in "${KAFS_TARGETS[@]}"; do
  name=${ent%%|*}
  opts=${ent#*|}
  if mount_kafs_target "$name" "$opts"; then
    run_workloads_for_target "$name" "$KAFS_MNT"
    unmount_kafs_target
  else
    record_result "$name" "1" "mount" "NaN" "fail"
    unmount_kafs_target
  fi
done

python3 - "$RESULT_CSV" "$COPY_FASTPATH_CSV" "$SUMMARY_MD" "$PROFILE" "$REPEATS" "$CACHE_BUST_MB" <<'PY'
import csv
import statistics
import sys
from collections import defaultdict

csv_path, copy_csv, out_md, profile, repeats, cache_bust_mb = sys.argv[1:]
rows = []
with open(csv_path, newline="") as f:
    for r in csv.DictReader(f):
        rows.append(r)

group = defaultdict(list)
fails = defaultdict(int)
for r in rows:
    key = (r["target"], r["workload"])
    if r["status"] != "ok":
        fails[key] += 1
        continue
    try:
        group[key].append(float(r["seconds"]))
    except ValueError:
        fails[key] += 1

targets = sorted({r["target"] for r in rows})
workloads = [
    "bulk_write",
    "chunked_write",
    "bulk_read",
    "chunked_read",
    "mixed_rw",
    "copy_heavy",
    "archive_compress_extract",
    "high_load",
]

with open(out_md, "w", encoding="utf-8") as out:
    out.write(f"# Native vs KAFS Pattern Benchmark\\n\\n")
    out.write(f"- profile: {profile}\\n")
    out.write(f"- repeats: {repeats}\\n")
    out.write(f"- cache_bust_mb: {cache_bust_mb}\\n")
    out.write(f"- raw: `{csv_path}`\\n")
    out.write(f"- copy_fastpath_raw: `{copy_csv}`\\n\\n")
    out.write("## Average Elapsed Time (seconds)\\n\\n")
    out.write("| workload | " + " | ".join(targets) + " |\\n")
    out.write("|---|" + "|".join(["---"] * len(targets)) + "|\\n")
    for w in workloads:
        vals = []
        for t in targets:
            vs = group.get((t, w), [])
            if not vs:
                vals.append("FAIL")
            else:
                vals.append(f"{statistics.mean(vs):.3f}")
        out.write(f"| {w} | " + " | ".join(vals) + " |\\n")

    out.write("\\n## Failures\\n\\n")
    any_fail = False
    for (t, w), cnt in sorted(fails.items()):
        if cnt > 0:
            any_fail = True
            out.write(f"- {t} / {w}: {cnt}\\n")
    if not any_fail:
        out.write("- none\\n")

    out.write("\\n## Copy Fastpath Delta (copy_heavy)\\n\\n")
    out.write("| target | repeat | attempt_blocks | done_blocks | fallback_blocks | skip_unaligned | skip_dst_inline | hit_rate |\\n")
    out.write("|---|---|---|---|---|---|---|---|\\n")
    copy_rows = []
    with open(copy_csv, newline="") as f:
        for r in csv.DictReader(f):
            if r["target"] != "native":
                copy_rows.append(r)
    if copy_rows:
        for r in copy_rows:
            attempt = int(r["attempt_blocks"])
            done = int(r["done_blocks"])
            hit_rate = (done / attempt) if attempt > 0 else 0.0
            out.write(
                f"| {r['target']} | {r['repeat']} | {attempt} | {done} | {r['fallback_blocks']} | {r['skip_unaligned']} | {r['skip_dst_inline']} | {hit_rate:.3f} |\\n"
            )
    else:
        out.write("| none | - | - | - | - | - | - | - |\\n")

        out.write("\\n## Copy Fastpath Correlation (copy_heavy)\\n\\n")
        out.write("| target | samples | mean_copy_heavy_sec | mean_hit_rate | mean_sec_per_done_block |\\n")
        out.write("|---|---|---|---|---|\\n")

        copy_heavy_by_key = {}
        for r in rows:
          if r["workload"] != "copy_heavy" or r["status"] != "ok":
            continue
          try:
            copy_heavy_by_key[(r["target"], r["repeat"])] = float(r["seconds"])
          except ValueError:
            continue

        agg = defaultdict(list)
        for r in copy_rows:
          target = r["target"]
          rep = r["repeat"]
          key = (target, rep)
          if key not in copy_heavy_by_key:
            continue
          attempt = int(r["attempt_blocks"])
          done = int(r["done_blocks"])
          sec = copy_heavy_by_key[key]
          hit_rate = (done / attempt) if attempt > 0 else 0.0
          sec_per_done = (sec / done) if done > 0 else 0.0
          agg[target].append((sec, hit_rate, sec_per_done))

        if agg:
          for target in sorted(agg.keys()):
            vals = agg[target]
            mean_sec = statistics.mean(v[0] for v in vals)
            mean_hit = statistics.mean(v[1] for v in vals)
            mean_eff = statistics.mean(v[2] for v in vals)
            out.write(
              f"| {target} | {len(vals)} | {mean_sec:.3f} | {mean_hit:.3f} | {mean_eff:.6f} |\\n"
            )

          points = []
          for vals in agg.values():
            for sec, hit, _ in vals:
              points.append((hit, sec))
          if len(points) >= 2:
            x_mean = statistics.mean(p[0] for p in points)
            y_mean = statistics.mean(p[1] for p in points)
            cov = sum((p[0] - x_mean) * (p[1] - y_mean) for p in points)
            var_x = sum((p[0] - x_mean) ** 2 for p in points)
            var_y = sum((p[1] - y_mean) ** 2 for p in points)
            if var_x > 0 and var_y > 0:
              corr = cov / ((var_x ** 0.5) * (var_y ** 0.5))
              out.write(
                f"\\n- pearson_corr(hit_rate, copy_heavy_sec): {corr:.6f} (n={len(points)})\\n"
              )
            else:
              out.write(
                f"\\n- pearson_corr(hit_rate, copy_heavy_sec): N/A (zero variance, n={len(points)})\\n"
              )
          else:
            out.write("\\n- pearson_corr(hit_rate, copy_heavy_sec): N/A (insufficient samples)\\n")
        else:
          out.write("| none | - | - | - | - |\\n")
          out.write("\\n- pearson_corr(hit_rate, copy_heavy_sec): N/A (no matched samples)\\n")

print(out_md)
PY

echo "[bench] done"
echo "[bench] csv: $RESULT_CSV"
echo "[bench] copy fastpath csv: $COPY_FASTPATH_CSV"
echo "[bench] summary: $SUMMARY_MD"
