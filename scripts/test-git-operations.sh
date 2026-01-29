#!/bin/bash
set -e

IMG="git-test.img"
MNT="mnt-git-test"
LOG="git-test.log"
KAFS="./src/kafs"

echo "=== KAFS Git Operations Test (with flush/fsync/release/fsyncdir hooks) ==="
echo "Time: $(date)"
echo ""

# Cleanup
rm -rf "$IMG" "$MNT" "$LOG" 2>/dev/null || true
mkdir -p "$MNT"

# Create fresh KAFS filesystem image
echo "1. Creating fresh KAFS filesystem image..."
SIZE=$((100 * 1024 * 1024))
truncate -s "$SIZE" "$IMG"
./src/mkfs.kafs "$IMG" > /dev/null 2>&1
echo "✓ Image created: $IMG (100MB)"

echo ""
echo "2. Mounting KAFS filesystem..."

export KAFS_DEBUG=1
export KAFS_IMAGE="$IMG"

# Mount KAFS with debug logging
"$KAFS" "$MNT" -f -s > "$LOG" 2>&1 &
KAFS_PID=$!
echo "KAFS PID: $KAFS_PID"

# Wait for mount
MOUNTED=0
for i in {1..100}; do
    if grep -q "$MNT" /proc/mounts 2>/dev/null; then
        MOUNTED=1
        echo "✓ Mounted"
        break
    fi
    sleep 0.1
done

if [ $MOUNTED -eq 0 ]; then
    echo "✗ Mount failed"
    [ -f "$LOG" ] && echo "Log:" && cat "$LOG"
    exit 1
fi

sleep 1

# Make root accessible
chmod 777 "$MNT"

echo ""
echo "3. Git init/add/commit operations..."

cd "$MNT"
git init 2>&1 | head -5
echo "✓ git init done"

echo ""
echo "Adding files..."
echo "Hello World" > file1.txt
echo "Test content 2" > file2.txt
mkdir -p subdir
echo "Nested file" > subdir/file3.txt

git add file1.txt file2.txt subdir/file3.txt
echo "✓ git add done"

echo ""
echo "Committing..."
git -c user.email="test@test.com" -c user.name="Test User" commit -m "Initial commit" 2>&1 | head -10
echo "✓ git commit done"

echo ""
echo "4. Verifying files..."
ls -lR .git/objects 2>/dev/null | head -30
echo ""
find .git/objects -type f 2>/dev/null | wc -l
echo "object files found"

echo ""
echo "5. Running fsck.kafs to verify filesystem..."
cd - > /dev/null
sleep 1

# Unmount
fusermount3 -u "$MNT" 2>/dev/null || umount "$MNT" 2>/dev/null || true
sleep 1
kill $KAFS_PID 2>/dev/null || true
wait $KAFS_PID 2>/dev/null || true

echo "✓ Unmounted"

echo ""
echo "6. Running fsck.kafs..."
./src/fsck.kafs "$IMG" 2>&1 | tail -20

echo ""
echo "=== Log Analysis ==="
echo ""

if [ ! -f "$LOG" ]; then
    echo "ERROR: Log file not found!"
    exit 1
fi

LOGLINES=$(wc -l < "$LOG")
echo "KAFS Debug Log file: $LOG"
echo "Total lines: $LOGLINES"

echo ""
echo "=== Checking for EIO/SHA1/EOF errors ==="
echo ""

# Check for errors
ERROR_COUNT=0

echo "--- EIO errors ---"
EIO_COUNT=$(grep -i "EIO\|I/O error" "$LOG" 2>/dev/null | wc -l)
echo "EIO/I/O error count: $EIO_COUNT"
[ $EIO_COUNT -gt 0 ] && ERROR_COUNT=$((ERROR_COUNT + 1))

echo ""
echo "--- SHA1 corruption errors ---"
SHA1_COUNT=$(grep -i "sha1\|hash\|corrupt" "$LOG" 2>/dev/null | wc -l)
echo "SHA1/hash/corruption errors: $SHA1_COUNT"
[ $SHA1_COUNT -gt 0 ] && ERROR_COUNT=$((ERROR_COUNT + 1))

echo ""
echo "--- EOF errors (kafs_blk_read) ---"
EOF_COUNT=$(grep "kafs_blk_read.*EOF\|unexpected EOF" "$LOG" 2>/dev/null | wc -l)
echo "EOF error count: $EOF_COUNT"
[ $EOF_COUNT -gt 0 ] && ERROR_COUNT=$((ERROR_COUNT + 1))

echo ""
echo "--- fsync/flush related errors ---"
FSYNC_COUNT=$(grep "fsync\|flush\|release" "$LOG" 2>/dev/null | grep -i "error\|failed" | wc -l)
echo "fsync/flush/release errors: $FSYNC_COUNT"
[ $FSYNC_COUNT -gt 0 ] && ERROR_COUNT=$((ERROR_COUNT + 1))

echo ""
echo "=== Test Result ==="
if [ $ERROR_COUNT -eq 0 ]; then
    echo "✓ SUCCESS: No EIO/SHA1/EOF errors detected after flush/fsync/release/fsyncdir hooks"
    exit 0
else
    echo "✗ FAILURE: Detected $ERROR_COUNT error categories"
    echo ""
    echo "=== Last 50 lines of KAFS Debug Log ==="
    tail -50 "$LOG"
    exit 1
fi
