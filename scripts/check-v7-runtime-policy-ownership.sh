#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

v7_owned_files=(
  src/kafs_v7_fuse_policy.h
  src/kafs_v7_runtime.c
  src/kafs_v7_data_cow.c
  src/kafs_v7_data_cow.h
  src/kafs_v7_runtime_transaction.c
  src/kafs_v7_runtime_transaction.h
)

active_policy_boundary_files=(
  src/kafs.h
  src/kafs_block.h
  src/kafs_context.h
  src/kafs_hrl.c
  src/kafs_inode.h
  src/kafs_mount_options_common.c
  src/kafs_shared_fuse_runner.h
  src/kafs_shared_fuse_runtime.c
  src/kafs_v7.c
  src/kafs_v7_entrypoint_adapter.h
  src/kafs_v7_mount_options.c
  src/kafs_v7_runtime.c
  src/kafs_v7_runtime_view.c
)

for file in "${v7_owned_files[@]}" "${active_policy_boundary_files[@]}"; do
  test -f "$file"
done

for retired_header in src/kafs_v6_fuse_init_policy.h src/kafs_v6_fuse_policy.h; do
  if test -e "$retired_header"; then
    echo "v7 runtime policy ownership check: retired policy header remains: $retired_header" >&2
    exit 1
  fi
done

forbidden='c_v6_|kafs_ctx_v6_|kafs_v6_controlled_write|kafs_v6_fuse_(init_)?policy|#include "kafs_v6_'
if rg -n "$forbidden" "${active_policy_boundary_files[@]}"; then
  echo "v7 runtime policy ownership check: forbidden v6 policy/state dependency found" >&2
  exit 1
fi

rg -q 'c_descriptor_layout_desc' src/kafs_context.h
rg -q 'c_descriptor_inode_shards' src/kafs_context.h
rg -q 'c_v7_controlled_write_enabled' src/kafs_context.h
rg -q 'c_v7_runtime_transactions' src/kafs_context.h
rg -q 'kafs_v7_fuse_policy.h' src/Makefile.am
rg -q 'kafs_v7_runtime_transaction.c' src/Makefile.am
rg -q 'kafs_v7_data_cow.c' src/Makefile.am
rg -q 'KAFS_V7_RUNTIME_ENTRYPOINT' src/Makefile.am
rg -q 'kafs_v7_fuse_policy_reject_legacy_mutation' src/kafs_shared_fuse_runtime.c
rg -q 'kafs_v7_runtime_view_validate_policy' src/kafs_shared_fuse_runtime.c
rg -q 'kafs_v7_runtime_transaction_barrier_context' src/kafs_shared_fuse_runtime.c

echo "v7 runtime policy ownership check: PASS"
