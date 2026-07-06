# KAFS format v6 runtime handoff 2026-07-03

## Scope

This is the short end-of-day handoff for the v6 runtime pureification work on
2026-07-03. The detailed cumulative handoff remains in
[sd-card-wear-v6-runtime-handoff-20260626.md](sd-card-wear-v6-runtime-handoff-20260626.md).

## Local State Before Final Handoff Commit

- Branch: `master`
- Remote: `origin` = `https://github.com/mako10k/kafs.git`
- Last implementation commit: `deb7538 Narrow v6 FUSE bridge API`
- Local branch state before this handoff doc commit: `master...origin/master
  [ahead 4]`

Commits queued for push before this handoff doc commit:

- `5d736fe Pure v6 runtime descriptor views`
- `0e8747d Narrow v6 runtime admission boundary`
- `c6a8970 Share v6 entrypoint request reporting`
- `deb7538 Narrow v6 FUSE bridge API`

## Completed Today

- T28 made successful v6 runtime views descriptor-backed and kept legacy
  contiguous table views out of admitted v6 contexts.
- T29-T31 narrowed v6 runtime admission/service boundaries, moved reusable
  descriptor-backed admission into `kafs_v6_admission.h`, and kept production
  `kafs` from linking `kafs_v6_runtime.c`.
- T32 moved `kafs-v6` runtime request rejection reporting into
  `kafs_v6_runtime.c`, so standalone `kafs-v6` and the `KAFS_V6_ENTRYPOINT`
  adapter path share `kafs_v6_runtime_request_t` validation/reporting.
- T33 added `src/kafs_v6_entrypoint_adapter.h` for entrypoint adapter
  declarations and introduced `kafs_main_run_fuse()` so production `kafs` and
  the `kafs-v6` adapter path share FUSE invocation/cleanup mechanics.

## 2026-07-06 Continuation

- T34 extracted the v6 controlled-write FUSE policy guard into
  `src/kafs_v6_fuse_policy.h`.
- T35 moved condition-gated controlled-write rejection and regular-file-only
  write checks behind the same v6 FUSE policy helper boundary.
- T36 moved the remaining direct controlled-write active checks in shared
  write/fsync/release paths behind named v6 FUSE policy helpers.
- T37 moved rejected-operation ids and rejection wording into the v6 FUSE
  policy helper, so shared FUSE operations pass enum values instead of
  free-form strings.
- T38 moved v6 entrypoint request validation and open/admit/init sequencing into
  `src/kafs_v6_entrypoint_adapter.c`, leaving `src/kafs.c` with generic option
  parsing, FUSE argv assembly, image locking, and the shared FUSE runner. The
  entrypoint adapter header uses an adapter-local mode enum instead of
  re-exporting `kafs_v6_runtime.h`.
- T39 renamed the temporary mount bridge files/symbols to
  `kafs_v6_entrypoint_adapter.*` / `kafs_v6_entrypoint_adapter_*` and added
  source ownership notes to make clear that this is a v6-only adapter, not a
  v5/v6 compatibility layer.
- T40 moved v6 mount-main preparation, FUSE option filtering, context
  initialization, image locking, runtime option handoff, and FUSE argv assembly
  into `src/kafs_v6_entrypoint_adapter.c`. `src/kafs.c` now keeps only the
  shared FUSE runner wrapper plus the shared operation
  table and cleanup path.
- T41 separated v6 mount option policy into `src/kafs_v6_mount_options.[ch]`.
  `src/kafs_v6.c` records runtime-admission intent through that helper, while
  `src/kafs_v6_entrypoint_adapter.c` uses it for FUSE passthrough filtering and
  `multi_thread` / `max_threads` handoff instead of carrying its own token
  vocabulary.
- T42 separated the shared FUSE runner hook into `src/kafs_shared_fuse_runner.h`.
  `src/kafs_v6_entrypoint_adapter.h` now exposes only adapter-local state and
  mount-main APIs, while `src/kafs.c` implements `kafs_shared_fuse_run()` for
  the remaining common-object handoff into `fuse_main()` and the shared
  operation table.
- T43 separated format-v6 FUSE init worker-policy handling into
  `src/kafs_v6_fuse_init_policy.h`. `kafs_op_init()` remains in `src/kafs.c`,
  but the v6-specific delayed/background worker suppression check and
  diagnostic now live behind a named helper.
- T44 separated production `kafs` legacy v6 fail-closed token classification
  and guidance into `src/kafs_legacy_v6_failclosed.h`. Production `kafs` still
  rejects legacy v6 mount requests with `kafs-v6` guidance and still does not
  link `kafs_v6_runtime.c`.
- T45 renamed the local shared FUSE operation-table boundary in `src/kafs.c`.
  The table is now `kafs_shared_fuse_operation_table`, reached through
  `kafs_shared_fuse_operations()`, and guarded by
  `KAFS_COMPILE_SHARED_FUSE_OPERATIONS` instead of table-level v6 adapter
  wording.
- T46 renamed the remaining `src/kafs.c` shared-runner export guard to
  `KAFS_SHARED_FUSE_RUNNER_EXPORT`. The `kafs-v6` target still links `kafs.c`
  for shared FUSE operations, but the source-level guard now describes the
  runner export instead of v6 admission ownership.
- T47 renamed the local shared FUSE runner helper in `src/kafs.c` from
  `kafs_main_run_fuse()` to `kafs_shared_fuse_run_with_cleanup()`. Production
  `kafs` main and the exported `kafs_shared_fuse_run()` handoff still share the
  same `fuse_main()` / cleanup path.
- T48 made `kafs-v6` reject production-only KAFS tuning tokens such as
  `sd_card_profile=*` instead of silently stripping options that the v6
  entrypoint does not reflect into its runtime context.
- Shared FUSE operation implementations and the operation table remain in
  `src/kafs.c`.
- The controlled-write surface was not expanded.
- Validation completed on 2026-07-06 with `make -j2`,
  `./scripts/test-cli-surface.sh`,
  `make -C tests check TESTS=v6_descriptor_smoketest`,
  `./scripts/static-checks.sh`, and
  `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`; `make check` reported all
  29 tests passed for the T34, T35, T36, T37, T38, T39, T40, T41, T42, T43,
  T44, T45, T46, T47, and T48 validation runs.

## Validation

Latest completed validation for T48:

```sh
./scripts/format.sh fix
make -j2
git diff --check
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
./scripts/static-checks.sh
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
```

Results:

- `make -j2`: PASS
- `v6_descriptor_smoketest`: PASS
- `./scripts/test-cli-surface.sh`: PASS
- `./scripts/static-checks.sh`: PASS
- strict source clone gate: 35 clones, 345 duplicated lines, 0.91%, below the
  configured threshold
- `make check`: all 29 tests passed

## Current Boundary

`kafs-v6` remains the owning entrypoint for v6 inspection and experimental
controlled-write admission. Production `kafs` still fails closed for legacy v6
inspection/write tokens with `kafs-v6` guidance.

The controlled-write surface was not expanded today. It remains limited to the
existing regular-file create/write/fsync/release path; truncate, fallocate,
unlink, rename, link, symlink, copy/reflink, control-plane write, hotplug
delegated write, writeback cache, runtime TRIM, delayed/background mutation,
and v6 repair write remain outside the allowed surface.

## Next Task

Continue v6 runtime pureification without broadening the write surface.

Recommended next slice:

- continue reducing the remaining shared FUSE common-object adapter path after
  the T40/T41/T42/T43/T44/T45/T46/T47/T48 mount-main, option-policy,
  runner-hook, FUSE-init policy, legacy fail-closed helper, table-boundary,
  runner-export guard, local-runner naming, and unsupported-option rejection
  split,
  or
- prepare a retirement plan for legacy v6 diagnostic scaffolding in production
  `kafs` after operator workflows no longer depend on it.

Do not start production cutover until later v5-parity, workload-copy,
power-loss/torn-write, rollback, and recovery evidence exists.

## Resume Checklist

1. Pull `origin/master`.
2. Confirm `git status --short --branch` is clean and current.
3. Read this file, then the cumulative handoff:
   [sd-card-wear-v6-runtime-handoff-20260626.md](sd-card-wear-v6-runtime-handoff-20260626.md).
4. Re-run at least:

   ```sh
   make -j2
   make -C tests check TESTS=v6_descriptor_smoketest
   ```

5. Pick the next pureification slice from
   [sd-card-wear-v6-runtime-entrypoint-plan.md](sd-card-wear-v6-runtime-entrypoint-plan.md)
   and [sd-card-wear-v6-shared-artifact-boundary-plan.md](sd-card-wear-v6-shared-artifact-boundary-plan.md).
