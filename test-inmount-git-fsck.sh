#!/bin/bash
set -euo pipefail

IMG="inmount-git.img"
MNT="mnt-inmount-git"
LOG="inmount-git.log"
KAFS="./src/kafs"

echo "============================================"
echo "KAFS In-Mount Git fsck Reproduction"
echo "(run git fsck while still mounted)"
echo "============================================"
echo "Time: $(date)"
echo ""

cleanup() {
  fusermount3 -u "$MNT" 2>/dev/null || umount "$MNT" 2>/dev/null || true
  if [ -n "${KAFS_PID:-}" ]; then
    kill "$KAFS_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

rm -f "$IMG" "$LOG" 2>/dev/null || true
rm -rf "$MNT" 2>/dev/null || true
mkdir -p "$MNT"

echo "[1/6] Creating fresh KAFS image (100MB)..."
truncate -s $((100 * 1024 * 1024)) "$IMG"
mkfs.kafs "$IMG" >/dev/null 2>&1
echo "✓ Image created"

echo ""
echo "[2/6] Mounting KAFS..."
export KAFS_IMAGE="$IMG"
export KAFS_DEBUG=0
"$KAFS" "$MNT" -f -s >"$LOG" 2>&1 &
KAFS_PID=$!
echo "PID: $KAFS_PID"

for i in {1..200}; do
  if grep -q "$MNT" /proc/mounts 2>/dev/null; then
    echo "✓ Mounted"
    break
  fi
  sleep 0.05
done
if ! grep -q "$MNT" /proc/mounts 2>/dev/null; then
  echo "✗ Mount failed"
  tail -n 80 "$LOG" 2>/dev/null || true
  exit 1
fi

echo ""
echo "[3/6] Running git init/add/commit..."
cd "$MNT"
git init -q
printf 'hello\n' > a.txt
mkdir -p sub
printf 'world\n' > sub/b.txt
git add -A
git commit -m init >/dev/null 2>&1

echo ""
echo "[4/6] Running git fsck --full (while mounted)..."
FSCK_OUT=$(git fsck --full 2>&1 || true)
if echo "$FSCK_OUT" | grep -qiE "missing blob|invalid sha1|error:"; then
  echo "✗ git fsck reported errors:"
  echo "$FSCK_OUT"
  exit 2
fi
if [ -n "$FSCK_OUT" ]; then
  echo "(git fsck output)"
  echo "$FSCK_OUT"
fi
echo "✓ git fsck clean"

echo ""
echo "[5/6] Quick object count..."
find .git/objects -type f 2>/dev/null | wc -l

cd - >/dev/null

echo ""
echo "[6/6] Done (will unmount in cleanup)."
