#!/usr/bin/env bash
set -euo pipefail
# Lint using gcc/clang with -Wall -Wextra -Werror on a quick compile
: "${CC:=gcc}"
command -v pkg-config >/dev/null 2>&1 || {
  echo "pkg-config is required to locate libfuse3 >= 3.8.0" >&2
  exit 127
}
pkg-config --exists 'fuse3 >= 3.8.0' || {
  echo "libfuse3 >= 3.8.0 is required" >&2
  exit 1
}
FUSE_CFLAGS="$(pkg-config --cflags fuse3)"
CFLAGS_COMMON=( -Wall -Wextra -Werror -Wno-unused-function -Wno-unused-parameter -std=c11 -include ./src/kafs_config.h -I./src ${FUSE_CFLAGS} )
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
