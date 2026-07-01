# KAFS format v6 runtime handoff 2026-06-26

## Scope

This handoff covers the Post-Phase 5 format v6 runtime mount enablement work.
It was originally written after `SDW-V6RT-T13 v6 controlled write durability and
fallback hardening` and has been updated through the controlled write operator
documentation closeout.

Implementation checkpoints:

- `83e9505 Harden v6 controlled write durability`
- `dea5df3 Add v6 controlled write smoke helper`
- `20d653d Extend v6 controlled write rejection smoke`
- `ead12aa Sync v6 controlled write help text`

## Current status

Format v6 now has two explicit runtime paths:

- Inspection mount: `-o ro,v6_inspection_mount`
- Experimental controlled write mount:
  `-o rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off`

Normal v6 mount attempts still do not become implicit write admission.
Controlled write admission is not a production default cutover path.

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

The controlled write path is available only as an experimental opt-in and
operator smoke surface in the current `kafs` binary. Do not broaden that
user-facing v6 write surface in `kafs` as the next step.

The next boundary is a v6 runtime binary split: define the dedicated v6
entrypoint, move future v6 runtime admission behind it, and factor shared code
through libraries or common objects. Production cutover discussion stays behind
that split and behind later v5-parity, workload-copy, power-loss or torn-write,
rollback, and recovery evidence.

## Resume checklist

1. Confirm the pushed branch and clean worktree.
2. Start from `docs/sd-card-wear-tickets.md` at the latest `SDW-V6RT` entry.
3. Start the next implementation from the v6 runtime binary split decision, not
   from a broader `kafs` controlled-write surface.
4. Keep shared code in libraries or common objects rather than duplicating v5/v6
   filesystem logic.
5. Do not enable production v6 write cutover from the controlled smoke result
   alone.
