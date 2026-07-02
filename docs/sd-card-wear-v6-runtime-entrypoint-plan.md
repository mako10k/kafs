# KAFS format v6 runtime entrypoint plan

Date: 2026-07-02
Status: skeleton implemented

## Boundary

The dedicated format v6 runtime entrypoint is `kafs-v6`.

`kafs` remains the production runtime entrypoint for v4/v5 images. Existing v6
inspection and controlled-write behavior in `kafs` remains bounded diagnostic
and smoke surface while the runtime split is completed; new v6 write-surface
expansion must move behind `kafs-v6`.

## CLI contract

Initial `kafs-v6` modes are explicit and mutually exclusive:

- `--inspection-mount`: read-only v6 inspection contract. Requires `-o ro`.
- `--controlled-write-mount`: experimental controlled write contract. Requires
  `-o rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full`.

The entrypoint rejects legacy `v6_inspection_mount` / `v6_write_mount` tokens.
Those tokens belong to the historical `kafs` bounded diagnostic surface. In
`kafs-v6`, selecting the binary and mode is the v6 admission signal.

The T20 skeleton validates the CLI boundary and the image format, then fails
closed before mounting. This is intentional: it creates the separate entrypoint
and build surface without moving FUSE runtime admission or expanding v6 write
semantics in the same slice.

## Code to move next

The next implementation slice should move the v6-specific admission path out of
the production `kafs` entrypoint and behind `kafs-v6`. Candidate code areas:

- v6 option/mode admission currently represented by `v6_inspection_mount` and
  `v6_write_mount` handling in `src/kafs.c`.
- v6 descriptor and journal segment preflight before runtime context admission.
- v6 runtime context setup for read-only inspection and controlled write modes.
- v6 delayed/background mutation policy application.
- user-facing v6 fail-closed diagnostics that should no longer be mixed into
  the v4/v5 production entrypoint.

## Shared implementation boundary

Do not duplicate filesystem logic between `kafs` and `kafs-v6`. Shared code
should move into common objects or libraries when it is needed by both binaries:

- descriptor parsing and validation
- journal segment validation
- runtime context setup/cleanup helpers
- common FUSE operation tables where policy checks are explicit
- inode, block allocation, HRL, and filesystem operation helpers

`src/Makefile.am` now builds `kafs-v6` as a separate binary. T21 started the
shared-code split with `kafs_v6_runtime_request_t` and option / image-format
validation helpers. T22 moves descriptor / journal segment preflight into the
same shared boundary. Later slices can move runtime context setup behind it.

## T20 smoke

The minimum smoke for this skeleton is:

```sh
make -j2
./src/kafs-v6 --help
./scripts/test-cli-surface.sh
```

Broader regression remains required when the split starts moving shared runtime
or FUSE operation code.
