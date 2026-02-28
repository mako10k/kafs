#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

if ! command -v npm >/dev/null 2>&1; then
  echo "SKIP: npm not found"
  exit 77
fi

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/kafs-test.npm-offline.XXXXXX")
IMG="$WORKDIR/npm-offline.img"
MNT="$WORKDIR/mnt"
LOG="$WORKDIR/kafs-npm-offline.log"
PKG_SRC="$WORKDIR/pkg-src"
PKG_TGZ="$WORKDIR/pkg-tgz"
APP_DIR="$MNT/app"
KAFS="$ROOT_DIR/src/kafs"
KAFS_PID=""

cleanup() {
  set +e
  fusermount3 -u "$MNT" 2>/dev/null || umount "$MNT" 2>/dev/null || true
  if [[ -n "${KAFS_PID:-}" ]]; then
    kill "$KAFS_PID" 2>/dev/null || true
    wait "$KAFS_PID" 2>/dev/null || true
  fi
  rm -rf "$WORKDIR" 2>/dev/null || true
}
trap cleanup EXIT

echo "=== KAFS npm-offline mixed IO workload ==="
echo "Time: $(date)"

mkdir -p "$MNT" "$PKG_SRC" "$PKG_TGZ"

echo "1. Create fresh image"
truncate -s $((256 * 1024 * 1024)) "$IMG"
"$ROOT_DIR/src/mkfs.kafs" "$IMG" >/dev/null 2>&1

echo "2. Build local package tarballs (network-free)"
PKG_COUNT=18
for i in $(seq 1 "$PKG_COUNT"); do
  pkg_dir="$PKG_SRC/local-pkg-$i"
  mkdir -p "$pkg_dir/lib" "$pkg_dir/data/chunks"

  cat >"$pkg_dir/package.json" <<EOF
{
  "name": "local-pkg-$i",
  "version": "1.0.$i",
  "main": "lib/index.js"
}
EOF

  cat >"$pkg_dir/lib/index.js" <<EOF
module.exports = function () {
  return "local-pkg-$i";
};
EOF

  for j in $(seq 1 20); do
    printf "pkg=%s file=%s\n" "$i" "$j" >"$pkg_dir/data/chunks/chunk-$j.txt"
  done

  (cd "$pkg_dir" && npm pack --silent --pack-destination "$PKG_TGZ" >/dev/null)
done

echo "3. Mount KAFS"
export KAFS_DEBUG=1
export KAFS_IMAGE="$IMG"
"$KAFS" "$MNT" -f -s >"$LOG" 2>&1 &
KAFS_PID=$!

MOUNTED=0
for i in {1..100}; do
  if grep -Fq "$MNT" /proc/mounts 2>/dev/null; then
    MOUNTED=1
    break
  fi
  sleep 0.1
done

if [[ $MOUNTED -eq 0 ]]; then
  echo "Mount failed"
  [[ -f "$LOG" ]] && tail -50 "$LOG"
  exit 1
fi

sleep 1
chmod 777 "$MNT"

echo "4. Create app and run offline npm install"
mkdir -p "$APP_DIR"

deps_json=""
       for i in $(seq 1 "$PKG_COUNT"); do
         name="local-pkg-$i"
         tgz=$(ls "$PKG_TGZ"/"$name"-*.tgz 2>/dev/null | head -n1)
         if [[ -z "$tgz" ]]; then
           echo "ERROR: tarball missing for $name"
           exit 1
         fi
         if [[ -n "$deps_json" ]]; then
           deps_json+=$',\n'
         fi
         deps_json+="    \"${name}\": \"file:${tgz}\""
       done

cat >"$APP_DIR/package.json" <<EOF
{
  "name": "kafs-local-install-workload",
  "version": "1.0.0",
  "private": true,
  "dependencies": {
${deps_json}
  }
}
EOF

cd "$APP_DIR"
npm install --no-audit --no-fund --prefer-offline --ignore-scripts >/dev/null

echo "5. Mixed file writes after install"
mkdir -p "$APP_DIR/src" "$APP_DIR/logs"
for i in $(seq 1 200); do
  printf "console.log('line %s');\n" "$i" >>"$APP_DIR/src/main.js"
  printf "event=%s ts=%s\n" "$i" "$(date +%s)" >>"$APP_DIR/logs/install.log"
done

for i in $(seq 1 50); do
  mkdir -p "$APP_DIR/tmp/dir-$i"
  printf "payload-%s\n" "$i" >"$APP_DIR/tmp/dir-$i/file-$i.txt"
done

npm install --no-audit --no-fund --prefer-offline --ignore-scripts >/dev/null

cd "$ROOT_DIR"

echo "6. Unmount and fsck"
fusermount3 -u "$MNT" 2>/dev/null || umount "$MNT" 2>/dev/null || true
sleep 1
kill "$KAFS_PID" 2>/dev/null || true
wait "$KAFS_PID" 2>/dev/null || true
KAFS_PID=""

"$ROOT_DIR/src/fsck.kafs" "$IMG" >/dev/null

echo "7. Log check"
if [[ ! -f "$LOG" ]]; then
  echo "ERROR: log not found"
  exit 1
fi

EIO_COUNT=$(awk 'BEGIN{IGNORECASE=1} /EIO|I\/O error/{c++} END{print c+0}' "$LOG")
CORRUPT_COUNT=$(awk 'BEGIN{IGNORECASE=1} /sha1|hash|corrupt/{c++} END{print c+0}' "$LOG")
EOF_COUNT=$(awk 'BEGIN{IGNORECASE=1} /unexpected EOF|kafs_blk_read.*EOF/{c++} END{print c+0}' "$LOG")

echo "EIO: $EIO_COUNT"
echo "CORRUPT: $CORRUPT_COUNT"
echo "EOF: $EOF_COUNT"

if [[ "$EIO_COUNT" -eq 0 && "$CORRUPT_COUNT" -eq 0 && "$EOF_COUNT" -eq 0 ]]; then
  echo "SUCCESS: npm-offline mixed IO workload passed"
  exit 0
fi

echo "FAILURE: detected error signals"
tail -50 "$LOG"
exit 1
