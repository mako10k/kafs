#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

owned_files=(
  src/kafs_v7_fuse_policy.h
  src/kafs_v7_runtime.c
)

for file in "${owned_files[@]}"; do
  test -f "$file"
done

forbidden='c_v6_controlled_write_enabled|kafs_v6_controlled_write|#include "kafs_v6_fuse_policy.h"'
if rg -n "$forbidden" "${owned_files[@]}"; then
  echo "v7 runtime policy ownership check: forbidden v6 controlled-write dependency found" >&2
  exit 1
fi

rg -q 'c_v7_controlled_write_enabled' src/kafs_context.h
rg -q 'kafs_v7_fuse_policy.h' src/Makefile.am

echo "v7 runtime policy ownership check: PASS"
