#!/usr/bin/env bash
set -euo pipefail
# Lint using gcc/clang with -Wall -Wextra -Werror on a quick compile
: "${CC:=gcc}"
FUSE_CFLAGS=""
if command -v pkg-config >/dev/null 2>&1 && pkg-config --exists fuse3; then
  FUSE_CFLAGS="$(pkg-config --cflags fuse3)"
else
  FUSE_CFLAGS="-I/usr/include/fuse3"
fi
CFLAGS_COMMON=( -Wall -Wextra -Werror -std=c11 -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -I./src ${FUSE_CFLAGS} )
SRC=( src/*.c )
if [[ ${#SRC[@]} -eq 0 ]]; then echo "No C sources"; exit 0; fi
# Compile each source to ensure warnings are caught; no link to avoid multiple mains
TMPDIR=$(mktemp -d)
cleanup(){ rm -rf "$TMPDIR"; }
trap cleanup EXIT
for f in "${SRC[@]}"; do
  $CC -c "${CFLAGS_COMMON[@]}" "$f" -o "$TMPDIR/$(basename "$f").o" || exit 1
done
echo "Lint OK."
