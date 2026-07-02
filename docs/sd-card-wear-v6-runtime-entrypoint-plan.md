# KAFS format v6 runtime entrypoint plan

Date: 2026-07-02
Status: inspection and controlled-write admission implemented

## Boundary

The dedicated format v6 runtime entrypoint is `kafs-v6`.

`kafs` remains the production runtime entrypoint for v4/v5 images. Legacy v6
inspection and controlled-write tokens in `kafs` now fail closed with
`kafs-v6` guidance. New v6 runtime admission and write-surface expansion must
move behind `kafs-v6`.

Until v6 production cutover, v6 has no backward compatibility promise. The
format and feature set may change drastically when the pure v6 target requires
it. The v4/v5 production runtime remains compatibility-preserving.

## CLI contract

Initial `kafs-v6` modes are explicit and mutually exclusive:

- `--inspection-mount`: read-only v6 inspection contract. Requires `-o ro`.
- `--controlled-write-mount`: experimental controlled write contract. Requires
  `-o rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full`.

The entrypoint rejects legacy `v6_inspection_mount` / `v6_write_mount` tokens.
Those tokens belong to the historical `kafs` bounded diagnostic surface. In
`kafs-v6`, selecting the binary and mode is the v6 admission signal.

The T20 skeleton validated the CLI boundary and image format, then failed
closed before mounting. T25 moves the read-only inspection admission path behind
`kafs-v6`: descriptor preflight still runs first, then `--inspection-mount`
admits a read-only FUSE mount. T26 moves controlled-write admission behind
`kafs-v6`: descriptor preflight still runs first, then
`--controlled-write-mount` admits only the existing conservative v6 write
surface.

## T25/T26 v6 admission

T25 links `kafs-v6` with the shared FUSE runtime object set through
`KAFS_V6_ENTRYPOINT`. The bridge is available only to `kafs-v6`; production
`kafs` does not link `kafs_v6_runtime.c` and no longer admits
`v6_inspection_mount` or `v6_write_mount` as a successful runtime path.

The read-only inspection path now uses:

- `kafs-v6 --inspection-mount ... -o ro` as the admission signal;
- descriptor and journal preflight from `kafs_v6_runtime.c`;
- shared FUSE/runtime mechanics from `kafs.c` compiled as a common object;
- forced read-only runtime state before `fuse_main`.

The controlled-write path now uses:

- `kafs-v6 --controlled-write-mount ... -o
  rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full`
  as the admission signal;
- descriptor and journal preflight from `kafs_v6_runtime.c`;
- shared FUSE/runtime mechanics from `kafs.c` compiled as a common object;
- the pre-existing bounded write surface: regular-file create/write/fsync and
  release, with broader metadata mutations still rejected.

## Code to pure next

The next implementation slice should move from isolation to v6-native runtime
context pureification. Candidate pressure points:

- descriptor-backed runtime views instead of v5-style mmap geometry;
- controlled-write runtime context setup that does not inherit generic v5
  worker assumptions;
- explicit v6 delayed/background mutation policy;
- removal or retirement plan for legacy v6 diagnostic scaffolding in `kafs`
  after operator workflows no longer depend on it.

## Shared implementation boundary

Do not duplicate filesystem logic between `kafs` and `kafs-v6`. Final runtime
binaries are the product boundary; shared implementation may move into common
objects or libraries (`.o`, `.a`, and future `.so`) when it is needed by both
binaries:

- descriptor parsing and validation
- journal segment validation
- runtime context setup/cleanup helpers
- common FUSE operation tables where policy checks are explicit
- inode, block allocation, HRL, and filesystem operation helpers

`src/Makefile.am` builds `kafs-v6` as a separate binary. T21/T22 created the v6
runtime helper surface for the dedicated entrypoint. T25 links `kafs-v6` with a
common object set that includes `kafs.c`, guarded by `KAFS_V6_ENTRYPOINT`, while
keeping `kafs_v6_runtime.c` out of production `kafs`. T26 uses the same common
object boundary for controlled-write admission. Later slices can replace the
common-object bridge with a non-installed static archive or a narrower runtime
context helper.

The concrete shared artifact boundary is recorded in
[sd-card-wear-v6-shared-artifact-boundary-plan.md](sd-card-wear-v6-shared-artifact-boundary-plan.md).
The immediate next implementation boundary is v6-native runtime context
pureification, not write-surface expansion.

## Smoke

The minimum smoke for this entrypoint is:

```sh
make -j2
./src/kafs-v6 --help
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
```

Broader regression remains required when the split starts moving shared runtime
or FUSE operation code.
