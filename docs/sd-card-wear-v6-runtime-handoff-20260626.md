# KAFS format v6 runtime handoff 2026-06-26

## Scope

This handoff covers the Post-Phase 5 format v6 runtime mount enablement work.
It was originally written after `SDW-V6RT-T13 v6 controlled write durability and
fallback hardening` and is now updated through the 2026-07-07 shared FUSE
runtime compile guard naming and production diagnostic scaffolding inventory
closeout, and through the production readonly smoke env gate retirement.

Committed implementation checkpoints:

- `83e9505 Harden v6 controlled write durability`
- `dea5df3 Add v6 controlled write smoke helper`
- `20d653d Extend v6 controlled write rejection smoke`
- `ead12aa Sync v6 controlled write help text`
- `ec233f8 Move v6 inspection admission behind kafs-v6`
- `2b9e53d Move v6 controlled write admission behind kafs-v6`
- `7568068 Split kafs-v6 runtime context opener`
- `5d736fe Pure v6 runtime descriptor views`

## Current status

Format v6 now has two explicit runtime paths, both owned by the dedicated
`kafs-v6` runtime entrypoint:

- Inspection mount: `kafs-v6 --inspection-mount <mountpoint> -o ro`
- Experimental controlled write mount:

```sh
kafs-v6 --controlled-write-mount <mountpoint> -o \
  rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full
```

Normal v6 mount attempts through production `kafs` still do not become implicit
runtime admission. Legacy `kafs -o v6_inspection_mount` and
`kafs -o v6_write_mount` fail closed with `kafs-v6` guidance. Controlled write
admission is not a production default cutover path.

Initial controlled write scope is intentionally narrow:

- Allowed: regular-file `create`, `write`, `fsync` / `fdatasync`, `release`
- Rejected: truncate, fallocate, unlink, rename, link, symlink, copy/reflink,
  control-plane write, hotplug delegated write, writeback cache, runtime TRIM,
  delayed/background mutation, and v6 repair write

`copy_file_range(2)` has a kernel-dependent fallback caveat: when the kernel
satisfies it through generic read/write before the FUSE copy hook is reached,
that path is treated as ordinary regular-file read/write. It is not evidence
that copy/reflink is supported.

## Completed closeout

The original controlled-write closeout hardened the path with regression
coverage for:

- zero-filled block materialization
- partial block overwrite
- ENOSPC on an independent small v6 image with unique block contents
- `fsync_policy=full` success path for `fsync` / `fdatasync`
- test-only forced backing sync failure through `KAFS_TEST_FORCE_FSYNC_ERROR`
- unmount followed by `fsck.kafs --balanced-check`
- post-write failure artifact wording in release and cutover docs

Operator docs updated:

- [kafsresize-cutover-playbook.md](kafsresize-cutover-playbook.md)
- [release-note-v6-explicit-write-opt-in-boundary.md](release-note-v6-explicit-write-opt-in-boundary.md)
- [sd-card-wear-v6-fuse-write-surface-audit.md](sd-card-wear-v6-fuse-write-surface-audit.md)
- [sd-card-wear-tickets.md](sd-card-wear-tickets.md)

Additional closeout after the original handoff:

- T14 added `scripts/v6-controlled-write-smoke.sh` as the repeatable acceptance
  helper.
- T15 extended `v6_descriptor_smoketest` with the allowlist-out rejection
  matrix.
- T16 synchronized `kafs --help` and `man/kafs.1` with the experimental
  controlled write boundary.
- T19 changed the v6 runtime direction: future v6 write work moves behind a
  dedicated v6 runtime entrypoint instead of broadening the production `kafs`
  binary. Shared implementation should be linked through libraries or common
  objects. See
  [sd-card-wear-v6-runtime-binary-split-decision.md](sd-card-wear-v6-runtime-binary-split-decision.md).
- T20 added `kafs-v6` as the dedicated v6 runtime entrypoint skeleton and
  recorded the CLI contract / shared-code split plan in
  [sd-card-wear-v6-runtime-entrypoint-plan.md](sd-card-wear-v6-runtime-entrypoint-plan.md).
- T23 recorded the v6 cutover preparation policy: v6 has no compatibility
  promise before cutover, while v4/v5 compatibility remains protected.
- T24 recorded the shared artifact boundary: final runtime binaries are
  `kafs` and `kafs-v6`; shared implementation may use common objects,
  non-installed archives, or future shared libraries.
- T25 moved read-only v6 inspection admission behind
  `kafs-v6 --inspection-mount`.
- T26 moved controlled-write admission behind
  `kafs-v6 --controlled-write-mount` and updated the operator smoke helper to
  use that entrypoint.
- T27 split the `kafs-v6` successful runtime path away from the generic v4/v5
  `kafs_main_open_runtime_context()` branch and added a dedicated v6
  open/read/admit/init helper.
- T28 made the successful v6 runtime view descriptor-backed: v6 admission no
  longer installs legacy contiguous inode/bitmap table pointers, and v6 contexts
  do not start the journal meta-delta bitmap overlay.
- T29 seals the v6 worker policy: pending worker, tombstone GC worker,
  background dedup worker, and hotplug delegation remain disabled, and
  controlled-write setup revalidates the policy after journal init.
- T30 moves v6 image open, superblock read, magic check, and format v6 check
  into `kafs_v6_runtime_open_context_image()`, narrowing the
  `KAFS_V6_ENTRYPOINT` bridge without expanding the write surface.
- T31 moves the shared descriptor-backed admission core into
  `kafs_v6_admission.h`, and moves `kafs-v6` mode state, diag setup, and
  controlled-write journal service setup into `kafs_v6_runtime.c`, while
  keeping reusable invariants in `kafs_context.h` so production `kafs` does not
  link `kafs_v6_runtime.c`.
- T32 moves the `kafs-v6` runtime request rejection reporter into
  `kafs_v6_runtime.c`; standalone `kafs-v6` and the `KAFS_V6_ENTRYPOINT`
  bridge now share the same `kafs_v6_runtime_request_t` validation/reporting
  contract.
- T33 narrows the v6 FUSE bridge API: `kafs_v6_entrypoint_adapter.h` owns the bridge
  entrypoint declarations, `kafs_v6_runtime.h` stays focused on runtime helper
  contracts, and `kafs_main_run_fuse()` hides the direct `fuse_main()` /
  `kafs_operations` invocation behind the existing cleanup path.
- T34 extracts the v6 controlled-write FUSE policy guard into
  `kafs_v6_fuse_policy.h`, so shared FUSE operation implementations call an
  explicit v6 policy helper while preserving the same write-surface boundary.
- T35-T37 complete the v6 controlled-write FUSE policy helper extraction for
  entry gates, data-layout policy decisions, and rejected-operation vocabulary.
- T38-T41 move the `kafs-v6` adapter request/open path, mount-main ownership,
  and v6-only mount option policy into dedicated v6 entrypoint modules.
- T42-T47 narrow and rename the shared FUSE runner API, operation table,
  export guard, and local runner so the remaining shared code is explicitly
  owned by the shared FUSE runner boundary.
- T48 makes unsupported production-only KAFS mount options fail closed in
  `kafs-v6` instead of being silently stripped from the FUSE option list.
- T49 renames the shared post-`fuse_main()` cleanup helper to
  `kafs_shared_fuse_cleanup_after_run()`, so the cleanup path no longer reads
  as production-main-only ownership.
- T50 renames the shared pre-`fuse_main()` runtime option logger to
  `kafs_shared_fuse_log_runtime_options()`, so the option logging path no
  longer reads as production-main-only ownership.
- T51 renames the local shared table/runner compile guard to
  `KAFS_COMPILE_SHARED_FUSE_RUNTIME`, so the guard reads as the shared FUSE
  runtime boundary it actually covers instead of operation-table-only
  ownership.
- T52 inventories the production `kafs` legacy v6 diagnostic scaffolding and
  records `KAFS_V6_READONLY_SMOKE` plus the already fail-closed legacy-token
  successful branches as the next retirement candidates. See
  [sd-card-wear-v6-production-diagnostic-scaffolding-inventory.md](sd-card-wear-v6-production-diagnostic-scaffolding-inventory.md).
- T53 retires the production `KAFS_V6_READONLY_SMOKE` environment gate. Read-only
  v6 FUSE coverage is now owned only by `kafs-v6 --inspection-mount`; production
  `kafs` keeps legacy fail-closed guidance and offline diagnostics.
- T54 retires the production `kafs` legacy-token successful branches behind the
  fail-closed gate. Legacy `v6_inspection_mount` / `v6_write_mount` tokens now
  exist only to produce `kafs-v6` guidance; successful v6 runtime admission is
  `kafs-v6`-owned.

## 2026-07-02 closeout

Latest implementation checkpoint before this handoff refresh:

- `7568068 Split kafs-v6 runtime context opener`

Current validation:

```sh
make -j2
./scripts/format.sh
git diff --check
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
make check -j2
```

Latest `make check -j2` result:

- all 29 tests passed

## 2026-07-03 T28 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
autoreconf -fi
./configure
make -j2
make -C tests check TESTS=v6_descriptor_smoketest
git diff --check
./scripts/test-cli-surface.sh
make check -j2
```

Latest `make check -j2` result:

- all 25 tests passed
- 4 tests were not run

Current entrypoint boundary:

- `kafs`: production runtime for v4/v5 before v6 cutover; legacy v6
  inspection/write tokens fail closed with `kafs-v6` guidance.
- `kafs-v6`: dedicated format v6 runtime entrypoint; owns v6 inspection and
  controlled-write admission.
- `kafs_v6_runtime.c`: linked only into `kafs-v6`, not production `kafs`.
- Shared implementation is currently through the `KAFS_V6_ENTRYPOINT` common
  object boundary in `kafs.c`; future slices may replace this with a
  non-installed archive or narrower runtime helper.

## 2026-07-03 T29 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
make -j2
make -C tests check TESTS=v6_descriptor_smoketest
git diff --check
./scripts/test-cli-surface.sh
make check -j2
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make -C tests check TESTS='min_git_hooks fs_semantics'
```

Latest `make check -j2` result:

- all 27 tests passed
- 2 tests were not run

The two skipped tests passed when retried with the longer mount timeout:

- all 2 tests passed for `min_git_hooks fs_semantics`

## 2026-07-03 T30 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
make -j2
make -C tests check TESTS=v6_descriptor_smoketest
git diff --check
./scripts/test-cli-surface.sh
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
```

Latest all-test result with the longer mount timeout:

- all 29 tests passed

Static gate status:

- `./scripts/static-checks.sh` completed format, lint, clones, and complexity
  checks.
- Strict source clone report: 36 clones, duplicated lines 365 (1.00%), within
  threshold. The tests clone report remains informational only.

## 2026-07-03 T31 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
make -j2
make -C tests check TESTS=v6_descriptor_smoketest
git diff --check
./scripts/test-cli-surface.sh
./scripts/clones.sh
./scripts/static-checks.sh
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
```

Latest all-test result with the longer mount timeout:

- all 28 tests passed
- 1 test was not run: `stress_fs` skipped after a FUSE mount failure in this
  environment

Static gate status:

- `./scripts/static-checks.sh` completed format, lint, clones, and complexity
  checks.
- Strict source clone report: 35 clones, duplicated lines 345 (0.94%), within
  threshold. The tests clone report remains informational only.

Current T31 boundary:

- `kafs_v6_admission.h` owns the shared descriptor-backed preflight/runtime
  admission core used by `kafs-v6` and legacy production `kafs` diagnostic
  scaffolding.
- `kafs_v6_runtime_admit_mount_context()` owns `kafs-v6` full-image mmap mode,
  mode state, and admission messages.
- `kafs_v6_runtime_init_mount_services()` owns `kafs-v6` diag setup and
  controlled-write journal service setup, then revalidates descriptor-backed
  runtime views and sealed worker policy.
- `kafs_context.h` owns shared v6 context invariants used by both
  `kafs-v6` and legacy production `kafs` diagnostic scaffolding.
- Production `kafs` still does not link `kafs_v6_runtime.c`, and legacy v6
  mount tokens remain fail-closed with `kafs-v6` guidance.

## 2026-07-03 T32 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
make -j2
make -C tests check TESTS=v6_descriptor_smoketest
git diff --check
./scripts/test-cli-surface.sh
./scripts/clones.sh
./scripts/static-checks.sh
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
```

Current T32 boundary:

- `kafs_v6_runtime_print_validation_error()` and
  `kafs_v6_runtime_report_entrypoint_request()` own the `kafs-v6` runtime
  request rejection wording.
- `src/kafs_v6.c` and the `KAFS_V6_ENTRYPOINT` bridge both call the shared v6
  runtime request reporter.
- The bridge no longer has local inspection / controlled-write mount option
  policy checks; production `kafs` keeps its legacy v6 fail-closed validation
  local.

Validation result:

- `./scripts/clones.sh` strict source gate reported 36 clones, 353 duplicated
  lines, 0.96%, which remains below the configured threshold. The tests clone
  report remains informational.
- `./scripts/static-checks.sh` completed format, lint, clone, and complexity
  checks successfully. Complexity warnings remain report-only.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` completed with all 28 tests
  passed and 1 test not run. The skipped test was `stress_fs`, whose log says
  mount failed and was skipped, likely due to missing FUSE permissions.

## 2026-07-03 T33 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
autoreconf -fi
./configure
make -j2
git diff --check
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
./scripts/static-checks.sh
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
```

Current T33 boundary:

- `kafs_v6_entrypoint_adapter.h` declares the `KAFS_V6_ENTRYPOINT` entrypoint
  adapter API, so `kafs_v6_runtime.h` no longer exposes adapter entrypoints.
- `kafs_main_run_fuse()` is the shared local FUSE runner for production `kafs`
  main and the `kafs-v6` adapter path.
- The shared FUSE operation table remains unchanged in `kafs.c`; this slice
  does not broaden controlled-write admission or the FUSE write surface.

Validation result:

- `./scripts/static-checks.sh` completed format, lint, clone, and complexity
  checks successfully. The strict source clone report was 36 clones, 353
  duplicated lines, 0.96%, which remains below the configured threshold.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` completed with all 29 tests
  passed.

## 2026-07-06 T34 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
autoreconf -fi
./configure
make -j2
git diff --check
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
./scripts/static-checks.sh
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
```

Current T34 boundary:

- `kafs_v6_fuse_policy.h` owns the v6 controlled-write active check and
  allowlist-out rejection helper.
- `src/kafs.c` still owns the shared FUSE operation implementations and table,
  but no longer defines the v6 controlled-write policy helper inline.
- The controlled-write write surface remains limited to regular-file
  create/write/fsync/release.

Validation result:

- `./scripts/static-checks.sh` completed format, lint, clone, and complexity
  checks successfully. The strict source clone report was 36 clones, 353
  duplicated lines, 0.96%, which remains below the configured threshold.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` completed with all 29 tests
  passed.

## 2026-07-06 T35 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
make -j2
git diff --check
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
./scripts/static-checks.sh
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
```

Current T35 boundary:

- `kafs_v6_fuse_policy.h` owns condition-gated v6 controlled-write rejection
  and regular-file-only write checks for shared FUSE operation entry points.
- `src/kafs.c` still owns the shared FUSE operation implementations and table,
  but control-plane open/write, `open(O_TRUNC)`, hotplug delegated write, and
  non-regular write gates now call the v6 policy helper boundary.
- The controlled-write write surface remains limited to regular-file
  create/write/fsync/release.

Validation result:

- `./scripts/static-checks.sh` completed format, lint, clone, and complexity
  checks successfully. The strict source clone report was 36 clones, 353
  duplicated lines, 0.96%, which remains below the configured threshold.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` completed with all 29 tests
  passed.

## 2026-07-06 T36 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
make -j2
git diff --check
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
./scripts/static-checks.sh
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
```

Current T36 boundary:

- `kafs_v6_fuse_policy.h` owns named data-layout policy checks for
  zero-block preservation, tail-layout bypass, release reclaim bypass, and
  local write-path selection.
- `src/kafs.c` still owns the shared FUSE write/fsync/release implementations
  and operation table, but no longer calls `kafs_v6_controlled_write_active()`
  directly.
- The controlled-write write surface remains limited to regular-file
  create/write/fsync/release.

Validation result:

- `./scripts/static-checks.sh` completed format, lint, clone, and complexity
  checks successfully. The strict source clone report was 36 clones, 353
  duplicated lines, 0.96%, which remains below the configured threshold.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` completed with all 29 tests
  passed.

## 2026-07-06 T37 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
make -j2
git diff --check
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
./scripts/static-checks.sh
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
```

Current T37 boundary:

- `kafs_v6_fuse_policy.h` owns `kafs_v6_controlled_write_op_t` and the mapping
  from controlled-write rejected-operation ids to log wording.
- `src/kafs.c` still owns the shared FUSE operation implementations and table,
  but passes enum values to the v6 policy helper instead of free-form rejection
  strings.
- The controlled-write write surface remains limited to regular-file
  create/write/fsync/release.

Validation result:

- `./scripts/static-checks.sh` completed format, lint, clone, and complexity
  checks successfully. The strict source clone report was 36 clones, 353
  duplicated lines, 0.96%, which remains below the configured threshold.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` completed with all 29 tests
  passed.

## 2026-07-06 T49 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
make -j2
git diff --check
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
./scripts/static-checks.sh
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
```

Current T49 boundary:

- `kafs_shared_fuse_cleanup_after_run()` owns the shared post-`fuse_main()`
  cleanup sequence used by production `kafs` and the `kafs-v6` shared runner
  export path.
- `src/kafs.c` still owns the shared FUSE operation implementations and table,
  but the local cleanup helper no longer reads as production-main-only
  ownership.
- The controlled-write write surface remains limited to regular-file
  create/write/fsync/release.

Validation result:

- `lsp-cli` references for the renamed cleanup helper returned only the
  definition and the `kafs_shared_fuse_run_with_cleanup()` call site.
- `./scripts/static-checks.sh` completed format, lint, clone, and complexity
  checks successfully. The strict source clone report was 35 clones, 345
  duplicated lines, 0.91%, which remains below the configured threshold.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` completed with all 29 tests
  passed.

## 2026-07-06 T50 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
make -j2
git diff --check
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
./scripts/static-checks.sh
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
```

Current T50 boundary:

- `kafs_shared_fuse_log_runtime_options()` owns the shared pre-`fuse_main()`
  runtime option logging used by production `kafs` and the `kafs-v6` shared
  runner export path.
- `src/kafs.c` still owns the shared FUSE operation implementations and table,
  but the local runtime option logger no longer reads as production-main-only
  ownership.
- The controlled-write write surface remains limited to regular-file
  create/write/fsync/release.

Validation result:

- `lsp-cli` references for the renamed option logger returned the active
  production compile branch definition and production main call site; `rg`
  confirmed the inactive `KAFS_SHARED_FUSE_RUNNER_EXPORT` call site was also
  renamed and no old helper name remained.
- `./scripts/static-checks.sh` completed format, lint, clone, and complexity
  checks successfully. The strict source clone report was 35 clones, 345
  duplicated lines, 0.91%, which remains below the configured threshold.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` completed with all 29 tests
  passed.

## 2026-07-07 T51 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
make -j2
git diff --check
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
./scripts/static-checks.sh
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
```

Current T51 boundary:

- `KAFS_COMPILE_SHARED_FUSE_RUNTIME` owns the local shared table/runner compile
  guard used when building production `kafs` or the `kafs-v6` shared runner
  export path.
- `src/kafs.c` still owns the shared FUSE operation implementations and table,
  but the compile guard no longer reads as operation-table-only ownership.
- The controlled-write write surface remains limited to regular-file
  create/write/fsync/release.

Validation result:

- `lsp-cli` references/hover confirmed the macro definition and the two
  `#ifdef` sites guarding the shared operation table and local shared runner
  helper. `lsp-cli rename` reported macro rename as unsupported, so the rename
  was applied after checking all refs with `rg`.
- `./scripts/static-checks.sh` completed format, lint, clone, and complexity
  checks successfully. The strict source clone report was 35 clones, 345
  duplicated lines, 0.91%, which remains below the configured threshold.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` completed with all 29 tests
  passed.

## 2026-07-07 T52 validation

Commands completed successfully:

```sh
git diff --check
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
```

Current T52 boundary:

- Production `kafs` keeps legacy v6 token fail-closed guidance and plain-v6
  offline-only descriptor preflight diagnostics.
- `KAFS_V6_ADMISSION_HANDOFF` and `KAFS_V6_READONLY_SMOKE` are diagnostic-only
  gates; neither is an operator entrypoint.
- The controlled-write write surface remains limited to regular-file
  create/write/fsync/release.

Validation result:

- `lsp-cli` references confirmed production v6 diagnostic helpers are local to
  their definitions and single call sites in `src/kafs.c`.
- `rg` found current test coverage for `KAFS_V6_ADMISSION_HANDOFF` and no test
  that directly sets `KAFS_V6_READONLY_SMOKE`; read-only FUSE coverage now uses
  `kafs-v6 --inspection-mount`.
- `make -C tests check TESTS=v6_descriptor_smoketest` completed with 1 test
  passed.

## 2026-07-07 T53 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
make -j2
git diff --check
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
./scripts/static-checks.sh
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
```

Current T53 boundary:

- `KAFS_V6_READONLY_SMOKE` no longer exists in `src/` or tests.
- Read-only format-v6 FUSE inspection remains owned by `kafs-v6 --inspection-mount`.
- Production `kafs` keeps `KAFS_V6_ADMISSION_HANDOFF` as a diagnostic-only
  offline gate plus legacy v6 token fail-closed guidance.
- The controlled-write write surface remains limited to regular-file
  create/write/fsync/release.

Validation result:

- `rg KAFS_V6_READONLY_SMOKE src tests` returned no matches.
- `./scripts/static-checks.sh` completed format, lint, clone, and complexity
  checks successfully. The strict source clone report was 35 clones, 345
  duplicated lines, 0.91%, which remains below the configured threshold.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` completed with all 29 tests
  passed.

## 2026-07-07 T54 validation

Commands completed successfully:

```sh
./scripts/format.sh fix
make -j2
git diff --check
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
./scripts/static-checks.sh
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
```

Current T54 boundary:

- Production `kafs` no longer has successful `v6_inspection_mount` or
  `v6_write_mount` branches in `kafs_main_open_runtime_context()`.
- Legacy v6 tokens remain parsed only so production `kafs` can fail closed with
  `kafs-v6` guidance.
- `kafs-v6 --inspection-mount` and `kafs-v6 --controlled-write-mount` remain the
  only successful format-v6 runtime admission entrypoints.
- Production `kafs` still keeps `KAFS_V6_ADMISSION_HANDOFF` and plain-v6
  offline-only preflight as diagnostic scaffolding.

Validation result:

- `lsp-cli` with `/usr/bin/clangd-18` returned no workspace symbol for
  `kafs_main_v6_inspection_mount` or `kafs_main_v6_controlled_write_mount`.
- `rg` returned no removed helper / validator references in `src/` or tests.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` completed with all 29 tests
  passed.

## Original validation run

Commands completed successfully:

```sh
make -j2
make -C tests check TESTS=v6_descriptor_smoketest
make check -j2
./scripts/format.sh
./scripts/lint.sh
./scripts/clones.sh
./scripts/static-checks.sh
```

`make check -j2` result:

- 27 tests passed
- 2 tests were not run
- `clone_template_copy` skipped after a 5000 ms FUSE mount timeout
- `stress_fs` skipped because the stress mount failed, likely due to FUSE permissions

`./scripts/clones.sh` exited 0 and produced the existing clone report. It did not
block this closeout.

## Remaining risks

- Controlled write mount is still experimental and should not be used as an
  implicit production cutover path.
- No real SD-card wear, power-loss, or torn-write hardware campaign has been
  completed for the controlled write path.
- v6 repair write remains unsupported; post-write fsck failure still means
  preserve artifacts and roll back rather than repair in place.
- FUSE mount availability can make some mount tests skip in constrained
  environments.

## Current next boundary

The next boundary remains v6 runtime pureification after the shared FUSE
runtime compile guard naming and production diagnostic scaffolding retirement.
Do not broaden the v6 write surface as the next step.

Start from the pressure points recorded in
[sd-card-wear-v6-runtime-entrypoint-plan.md](sd-card-wear-v6-runtime-entrypoint-plan.md):

- retirement of remaining v6 diagnostic scaffolding in `kafs`, next deciding
  whether to keep `KAFS_V6_ADMISSION_HANDOFF` and plain-v6 offline-only
  descriptor preflight;
- further reduction of the remaining shared FUSE runner / operation
  implementation boundaries, especially where production `kafs` and `kafs-v6`
  still meet in `src/kafs.c`, without duplicating filesystem logic.

Production cutover discussion stays behind that pureification and behind later
v5-parity, workload-copy, power-loss or torn-write, rollback, and recovery
evidence.

## Resume checklist

1. Confirm the pushed branch and clean worktree.
2. Start from `docs/sd-card-wear-tickets.md` at the latest `SDW-V6RT` entry.
3. Confirm `kafs-v6 --inspection-mount` and `--controlled-write-mount` remain
   the only successful v6 runtime admission paths.
4. Start the next implementation from the remaining shared FUSE runner /
   operation boundary pressure points, not from a broader write surface.
5. Keep shared code in libraries or common objects rather than duplicating v5/v6
   filesystem logic.
6. Do not enable production v6 write cutover from the controlled smoke result
   alone.
