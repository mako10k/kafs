#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

usage() {
  cat <<'EOF'
Usage:
  scripts/benchmark-parallel-tar-tolerance.sh [options]

Options:
  --profile <small|default|stress>   Dataset/size profile (default: default)
  --codec <plain|gzip|pigz>          Archive codec (default: gzip)
  --pigz-threads <N>                 pigz thread count when --codec pigz (default: 4)
  --par <list>                       Parallel jobs (comma separated, default: 2,3)
  --modes <list>                     Priority modes (comma separated, default: normal,idle)
  --idle-nice <0..19>                nice value for idle mode (default: 11)
  --result-file <path>               JSONL output path
  --keep-workdir                     Keep temporary workdirs on failure
  -h, --help                         Show this help

Examples:
  scripts/benchmark-parallel-tar-tolerance.sh
  scripts/benchmark-parallel-tar-tolerance.sh --profile small --codec plain --par 2,3
  scripts/benchmark-parallel-tar-tolerance.sh --codec pigz --pigz-threads 8
EOF
}

PROFILE="default"
CODEC="gzip"
PIGZ_THREADS=4
PAR_LIST="2,3"
MODE_LIST="normal,idle"
IDLE_NICE=11
RESULT_FILE=""
KEEP_WORKDIR=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="$2"
      shift 2
      ;;
    --codec)
      CODEC="$2"
      shift 2
      ;;
    --pigz-threads)
      PIGZ_THREADS="$2"
      shift 2
      ;;
    --par)
      PAR_LIST="$2"
      shift 2
      ;;
    --modes)
      MODE_LIST="$2"
      shift 2
      ;;
    --idle-nice)
      IDLE_NICE="$2"
      shift 2
      ;;
    --result-file)
      RESULT_FILE="$2"
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
      echo "unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "$RESULT_FILE" ]]; then
  RESULT_FILE="${TMPDIR:-/tmp}/kafs-parallel-tar-$(date +%Y%m%d-%H%M%S).jsonl"
fi

case "$PROFILE" in
  small)
    IMAGE_MB=512
    SMALL_COUNT=300
    SMALL_SIZE_KB=8
    MED_COUNT=20
    MED_SIZE_KB=256
    LARGE_COUNT=4
    LARGE_SIZE_MB=2
    ;;
  default)
    IMAGE_MB=1536
    SMALL_COUNT=1200
    SMALL_SIZE_KB=8
    MED_COUNT=100
    MED_SIZE_KB=512
    LARGE_COUNT=16
    LARGE_SIZE_MB=4
    ;;
  stress)
    IMAGE_MB=2048
    SMALL_COUNT=2200
    SMALL_SIZE_KB=8
    MED_COUNT=160
    MED_SIZE_KB=512
    LARGE_COUNT=24
    LARGE_SIZE_MB=4
    ;;
  *)
    echo "invalid profile: $PROFILE" >&2
    exit 2
    ;;
esac

case "$CODEC" in
  plain|gzip|pigz) ;;
  *)
    echo "invalid codec: $CODEC" >&2
    exit 2
    ;;
esac

if [[ "$CODEC" == "pigz" ]] && ! command -v pigz >/dev/null 2>&1; then
  echo "pigz not found (required for --codec pigz)" >&2
  exit 2
fi

if ! command -v tar >/dev/null 2>&1; then
  echo "tar not found" >&2
  exit 2
fi

IFS=',' read -r -a PAR_VALUES <<<"$PAR_LIST"
IFS=',' read -r -a MODE_VALUES <<<"$MODE_LIST"

for p in "${PAR_VALUES[@]}"; do
  if ! [[ "$p" =~ ^[0-9]+$ ]] || [[ "$p" -lt 1 ]]; then
    echo "invalid --par value: $p" >&2
    exit 2
  fi
done

mkdir -p "$(dirname "$RESULT_FILE")"
: >"$RESULT_FILE"

run_one_case() {
  local mode="$1"
  local par="$2"
  local nicev=0
  if [[ "$mode" == "idle" ]]; then
    nicev="$IDLE_NICE"
  fi

  local workdir
  workdir=$(mktemp -d "${TMPDIR:-/tmp}/kafs-ptol.${mode}.p${par}.XXXXXX")
  local img="$workdir/test.img"
  local mnt="$workdir/mnt"
  local front_log="$workdir/front.log"
  local kafs_pid=""
  local keep=0

  cleanup_case() {
    set +e
    if [[ -n "$kafs_pid" ]]; then
      fusermount3 -u "$mnt" 2>/dev/null || umount "$mnt" 2>/dev/null || true
      kill "$kafs_pid" 2>/dev/null || true
      wait "$kafs_pid" 2>/dev/null || true
    fi
    if [[ "$KEEP_WORKDIR" -eq 0 ]] && [[ "$keep" -eq 0 ]]; then
      rm -rf "$workdir" 2>/dev/null || true
    fi
  }

  mkdir -p "$mnt"
  truncate -s "$((IMAGE_MB * 1024 * 1024))" "$img"
  ./src/mkfs.kafs "$img" >/dev/null 2>&1

  KAFS_PENDING_WORKER_PRIO="$mode" KAFS_PENDING_WORKER_NICE="$nicev" \
    ./src/kafs --image "$img" "$mnt" -f -s >"$front_log" 2>&1 &
  kafs_pid=$!

  local mounted=0
  for _ in {1..180}; do
    if grep -Fq "$mnt" /proc/mounts 2>/dev/null; then
      mounted=1
      break
    fi
    sleep 0.1
  done

  if [[ "$mounted" -ne 1 ]]; then
    keep=1
    jq -cn \
      --arg mode "$mode" \
      --argjson par "$par" \
      --arg profile "$PROFILE" \
      --arg codec "$CODEC" \
      --arg error "mount_failed" \
      --arg workdir "$workdir" \
      '{mode:$mode,par:$par,profile:$profile,codec:$codec,error:$error,workdir:$workdir}' \
      >>"$RESULT_FILE"
    cleanup_case
    return 0
  fi

  chmod 777 "$mnt"

  if ! MNT="$mnt" \
    SMALL_COUNT="$SMALL_COUNT" SMALL_SIZE_KB="$SMALL_SIZE_KB" \
    MED_COUNT="$MED_COUNT" MED_SIZE_KB="$MED_SIZE_KB" \
    LARGE_COUNT="$LARGE_COUNT" LARGE_SIZE_MB="$LARGE_SIZE_MB" \
    python3 - <<'PY'
import os, random
mnt = os.environ['MNT']
small_count = int(os.environ['SMALL_COUNT'])
small_size = int(os.environ['SMALL_SIZE_KB']) * 1024
med_count = int(os.environ['MED_COUNT'])
med_size = int(os.environ['MED_SIZE_KB']) * 1024
large_count = int(os.environ['LARGE_COUNT'])
large_size = int(os.environ['LARGE_SIZE_MB']) * 1024 * 1024
r = random.Random(777)
base = os.path.join(mnt, 'work')
src = os.path.join(base, 'dataset')
os.makedirs(src, exist_ok=True)

def write_file(path, size):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as f:
        rem = size
        chunk = 1024 * 1024
        while rem > 0:
            n = chunk if rem > chunk else rem
            f.write(r.randbytes(n))
            rem -= n

for i in range(small_count):
    write_file(os.path.join(src, 'small', f's{i:05d}.bin'), small_size)
for i in range(med_count):
    write_file(os.path.join(src, 'med', f'm{i:04d}.bin'), med_size)
for i in range(large_count):
    write_file(os.path.join(src, 'large', f'l{i:04d}.bin'), large_size)
PY
  then
    local front_alive=1
    if ! kill -0 "$kafs_pid" 2>/dev/null; then
      front_alive=0
      keep=1
    fi
    jq -cn \
      --arg mode "$mode" \
      --argjson par "$par" \
      --arg profile "$PROFILE" \
      --arg codec "$CODEC" \
      --arg error "dataset_prepare_failed" \
      --argjson front_alive "$front_alive" \
      --arg workdir "$workdir" \
      '{mode:$mode,par:$par,profile:$profile,codec:$codec,error:$error,front_alive:$front_alive,workdir:$workdir}' \
      >>"$RESULT_FILE"
    cleanup_case
    return 0
  fi

  local expected_files
  expected_files=$(find "$mnt/work/dataset" -type f | wc -l | tr -d ' ')

  local start end elapsed
  start=$(python3 - <<'PY'
import time
print(time.perf_counter())
PY
)

  local pids=""
  for j in $(seq 1 "$par"); do
    (
      set -e
      arc_base="$mnt/work/archive_p${par}_j${j}.tar"
      out="$mnt/work/extract_p${par}_j${j}"
      rm -rf "$out" "$arc_base" "$arc_base.gz" "$arc_base.zst"

      case "$CODEC" in
        plain)
          tar -cf "$arc_base" -C "$mnt/work" dataset
          mkdir -p "$out"
          tar -xf "$arc_base" -C "$out"
          ;;
        gzip)
          tar -czf "$arc_base.gz" -C "$mnt/work" dataset
          mkdir -p "$out"
          tar -xzf "$arc_base.gz" -C "$out"
          ;;
        pigz)
          tar -I "pigz -p $PIGZ_THREADS" -cf "$arc_base.gz" -C "$mnt/work" dataset
          mkdir -p "$out"
          tar -I "pigz -d -p $PIGZ_THREADS" -xf "$arc_base.gz" -C "$out"
          ;;
      esac
    ) >"$workdir/job_${j}.log" 2>&1 &
    pids="$pids $!"
  done

  local job_fail=0
  for p in $pids; do
    wait "$p" || job_fail=$((job_fail + 1))
  done

  end=$(python3 - <<'PY'
import time
print(time.perf_counter())
PY
)
  elapsed=$(python3 - "$start" "$end" <<'PY'
import sys
print(round(float(sys.argv[2]) - float(sys.argv[1]), 4))
PY
)

  local extract_ok=0
  for j in $(seq 1 "$par"); do
    cnt=$(find "$mnt/work/extract_p${par}_j${j}/dataset" -type f 2>/dev/null | wc -l | tr -d ' ' || true)
    if [[ -z "$cnt" ]]; then
      cnt=0
    fi
    if [[ "$cnt" == "$expected_files" ]]; then
      extract_ok=$((extract_ok + 1))
    fi
  done

  local front_alive=1
  if ! kill -0 "$kafs_pid" 2>/dev/null; then
    front_alive=0
    keep=1
  fi

  jq -cn \
    --arg mode "$mode" \
    --argjson par "$par" \
    --arg profile "$PROFILE" \
    --arg codec "$CODEC" \
    --argjson idle_nice "$nicev" \
    --argjson elapsed_s "$elapsed" \
    --argjson expected_files "$expected_files" \
    --argjson extract_ok "$extract_ok" \
    --argjson job_fail "$job_fail" \
    --argjson front_alive "$front_alive" \
    --arg workdir "$workdir" \
    '{mode:$mode,par:$par,profile:$profile,codec:$codec,idle_nice:$idle_nice,elapsed_s:$elapsed_s,expected_files:$expected_files,extract_ok:$extract_ok,job_fail:$job_fail,front_alive:$front_alive,workdir:$workdir}' \
    >>"$RESULT_FILE"

  cleanup_case
}

for mode in "${MODE_VALUES[@]}"; do
  case "$mode" in
    normal|idle) ;;
    *)
      echo "invalid mode: $mode" >&2
      exit 2
      ;;
  esac
  for par in "${PAR_VALUES[@]}"; do
    run_one_case "$mode" "$par"
  done
done

python3 - "$RESULT_FILE" <<'PY'
import json, sys
p = sys.argv[1]
rows = [json.loads(l) for l in open(p) if l.strip()]
print(f"result_file: {p}")
for r in rows:
    status = "PASS" if r.get("job_fail", 1) == 0 and r.get("extract_ok", 0) == r.get("par", -1) and r.get("front_alive", 0) == 1 and "error" not in r else "FAIL"
    msg = {
        "mode": r.get("mode"),
        "par": r.get("par"),
        "codec": r.get("codec"),
        "status": status,
        "elapsed_s": r.get("elapsed_s"),
        "job_fail": r.get("job_fail"),
        "extract_ok": f"{r.get('extract_ok')}/{r.get('par')}",
        "front_alive": r.get("front_alive"),
    }
    print(json.dumps(msg, ensure_ascii=False))
PY
