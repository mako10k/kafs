#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

owned_files=(
  src/kafs_v7_fuse_policy.h
  src/kafs_v7_runtime.c
  src/kafs_v7_data_cow.c
  src/kafs_v7_data_cow.h
  src/kafs_v7_runtime_transaction.c
  src/kafs_v7_runtime_transaction.h
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
rg -q 'c_v7_runtime_transactions' src/kafs_context.h
rg -q 'kafs_v7_fuse_policy.h' src/Makefile.am
rg -q 'kafs_v7_runtime_transaction.c' src/Makefile.am
rg -q 'kafs_v7_data_cow.c' src/Makefile.am
rg -q 'KAFS_V7_RUNTIME_ENTRYPOINT' src/Makefile.am
rg -q 'kafs_v7_fuse_policy_reject_legacy_mutation' src/kafs_shared_fuse_runtime.c
rg -q 'kafs_v7_runtime_transaction_barrier_context' src/kafs_shared_fuse_runtime.c

echo "v7 runtime policy ownership check: PASS"
