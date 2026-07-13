#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

owned_files=(
  src/kafs_v7_layout.c
  src/kafs_v7_layout.h
  tests/tests_v7_raw_layout_smoketest.c
)

for file in "${owned_files[@]}"; do
  test -f "$file"
done

forbidden='kafs_sv6_|kafs_v6_|kafs_descriptor_layout|#include "kafs_v6_layout.h"'
if rg -n "$forbidden" "${owned_files[@]}"; then
  echo "v7 raw-layout ownership check: forbidden v6/scaffold dependency found" >&2
  exit 1
fi

rg -q '^mkfs_kafs_SOURCES = .*kafs_v7_layout\.c' src/Makefile.am
rg -q '^fsck_kafs_SOURCES = .*kafs_v7_layout\.c' src/Makefile.am
rg -q '^kafsdump_SOURCES = .*kafs_v7_layout\.c' src/Makefile.am
rg -q '^v7_raw_layout_smoketest_SOURCES = ' tests/Makefile.am

echo "v7 raw-layout ownership check: PASS"
