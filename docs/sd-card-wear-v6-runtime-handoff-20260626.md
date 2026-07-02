# KAFS format v6 runtime handoff 2026-06-26

## Scope

This handoff covers the Post-Phase 5 format v6 runtime mount enablement work.
It was originally written after `SDW-V6RT-T13 v6 controlled write durability and
fallback hardening` and is now updated through the 2026-07-02 `kafs-v6`
entrypoint isolation and runtime-context opener pureification closeout.

Implementation checkpoints:

- `83e9505 Harden v6 controlled write durability`
- `dea5df3 Add v6 controlled write smoke helper`
- `20d653d Extend v6 controlled write rejection smoke`
- `ead12aa Sync v6 controlled write help text`
- `ec233f8 Move v6 inspection admission behind kafs-v6`
- `2b9e53d Move v6 controlled write admission behind kafs-v6`
- `7568068 Split kafs-v6 runtime context opener`

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

The latest slice hardened the controlled write path with regression coverage for:

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

Current entrypoint boundary:

- `kafs`: production runtime for v4/v5 before v6 cutover; legacy v6
  inspection/write tokens fail closed with `kafs-v6` guidance.
- `kafs-v6`: dedicated format v6 runtime entrypoint; owns v6 inspection and
  controlled-write admission.
- `kafs_v6_runtime.c`: linked only into `kafs-v6`, not production `kafs`.
- Shared implementation is currently through the `KAFS_V6_ENTRYPOINT` common
  object boundary in `kafs.c`; future slices may replace this with a
  non-installed archive or narrower runtime helper.

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

The next boundary is descriptor-backed runtime view pureification. Do not
broaden the v6 write surface as the next step.

Start from the pressure points recorded in
[sd-card-wear-v6-runtime-entrypoint-plan.md](sd-card-wear-v6-runtime-entrypoint-plan.md):

- descriptor-backed runtime views instead of v5-style mmap geometry;
- controlled-write runtime context setup that does not inherit generic v5
  worker assumptions;
- explicit v6 delayed/background mutation policy;
- retirement plan for legacy v6 diagnostic scaffolding in `kafs` after
  operator workflows no longer depend on it.

Production cutover discussion stays behind that pureification and behind later
v5-parity, workload-copy, power-loss or torn-write, rollback, and recovery
evidence.

## Resume checklist

1. Confirm the pushed branch and clean worktree.
2. Start from `docs/sd-card-wear-tickets.md` at the latest `SDW-V6RT` entry.
3. Confirm `kafs-v6 --inspection-mount` and `--controlled-write-mount` remain
   the only successful v6 runtime admission paths.
4. Start the next implementation from descriptor-backed runtime view
   pureification, not from a broader write surface.
5. Keep shared code in libraries or common objects rather than duplicating v5/v6
   filesystem logic.
6. Do not enable production v6 write cutover from the controlled smoke result
   alone.
