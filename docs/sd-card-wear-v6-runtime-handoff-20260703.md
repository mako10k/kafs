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
  bridge share `kafs_v6_runtime_request_t` validation/reporting.
- T33 added `src/kafs_v6_mount_bridge.h` for bridge entrypoint declarations and
  introduced `kafs_main_run_fuse()` so production `kafs` and the `kafs-v6`
  bridge share FUSE invocation/cleanup mechanics.

## 2026-07-06 Continuation

- T34 extracted the v6 controlled-write FUSE policy guard into
  `src/kafs_v6_fuse_policy.h`.
- T35 moved condition-gated controlled-write rejection and regular-file-only
  write checks behind the same v6 FUSE policy helper boundary.
- Shared FUSE operation implementations and the operation table remain in
  `src/kafs.c`.
- The controlled-write surface was not expanded.
- Validation completed on 2026-07-06 with `make -j2`,
  `make -C tests check TESTS=v6_descriptor_smoketest`,
  `./scripts/static-checks.sh`, and
  `./scripts/test-cli-surface.sh`,
  `make -C tests check TESTS=v6_descriptor_smoketest`,
  `./scripts/static-checks.sh`, and
  `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`; `make check` reported all
  29 tests passed for both T34 and T35 validation runs.

## Validation

Latest completed validation for T35:

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
- strict source clone gate: 36 clones, 353 duplicated lines, 0.96%, below the
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

- continue reducing the remaining `KAFS_V6_ENTRYPOINT` common-object bridge
  around shared FUSE operation implementations after the T35 entry-gate helper
  extraction, or
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
