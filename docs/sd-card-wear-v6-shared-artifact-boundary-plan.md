# KAFS format v6 shared artifact boundary plan

Date: 2026-07-02
Status: accepted

## Purpose

This plan defines how v6 runtime work may share implementation with v4/v5
without adding user-facing helper binaries or reintroducing v6 policy into the
production `kafs` runtime. It is the preparation step before moving more v6
runtime setup behind `kafs-v6`.

## Product Boundary

Only final runtime binaries are product artifacts:

- `kafs`: production runtime for v4/v5 before v6 cutover.
- `kafs-v6`: dedicated format v6 runtime entrypoint.

Shared implementation artifacts are allowed:

- per-target common source/object lists in `Makefile.am`;
- non-installed static archives such as `noinst_LIBRARIES` when the build system
  is prepared for archive generation;
- future shared libraries when an ABI/install policy exists.

Do not create additional executable helper binaries for code sharing. A helper
that users can run is a product surface and needs its own CLI policy.

## Current Link Surface

| Target | Current shared sources | v6-specific sources | Boundary |
| --- | --- | --- | --- |
| `kafs` | `kafs_hrl.c`, `kafs_locks.c`, `kafs_journal.c`, `kafs_rpc.c` | none in `Makefile.am` | v4/v5 production runtime; legacy v6 inspection token now fails closed with `kafs-v6` guidance |
| `kafs-v6` | `kafs.c`, `kafs_hrl.c`, `kafs_locks.c`, `kafs_journal.c`, `kafs_rpc.c` through `KAFS_V6_ENTRYPOINT` | `kafs_v6.c`, `kafs_v6_runtime.c` | v6 CLI/admission owner; read-only inspection admission is active, controlled write remains fail-closed |
| `kafsctl` / `kafs-back` | selected runtime support sources through `kafs.c` and RPC/HRL/journal helpers | none in `Makefile.am` | must not gain `kafs_v6_runtime.c` implicitly |
| offline tools | mostly standalone source files plus header-only v6 layout helpers | header-only v6 descriptor logic | offline/staging tools, not runtime admission owners |

This boundary deliberately keeps `kafs_v6_runtime.c` out of the production
`kafs` link. T25 temporarily shares `kafs.c` into `kafs-v6` as a common object
bridge for FUSE operations and read-only inspection setup, guarded by
`KAFS_V6_ENTRYPOINT` so the bridge is not part of the production `kafs`
entrypoint.

## Artifact Classes

### v6-Only Runtime Policy

Owns CLI shape, admission wording, and v6 cutover policy. This code should link
only into `kafs-v6` unless a later product decision changes the binary boundary.

Current and near-term candidates:

- `src/kafs_v6.c`
- `src/kafs_v6_runtime.c`
- future `src/kafs_v6_mount_admission.c`
- future `src/kafs_v6_runtime_context.c`

### Shared Pure Metadata Helpers

May be common `.o`, `.a`, or future `.so` implementation because these helpers
do not own user-facing runtime policy.

Current candidates:

- descriptor discovery and replica validation from `kafs_v6_layout.h`;
- descriptor-backed bitmap/inode/allocator/HRL coverage checks;
- descriptor-backed journal header/data/segment lookup and validation;
- read-only fsck/dump descriptor report helpers after their CLI output remains
  owned by the calling tool.

The first extraction should prefer a non-installed static archive or common
source list, not another executable. If a static archive is used, add the needed
Autotools archive support in the same slice and keep the archive `noinst`.

### Shared Runtime Mechanics

May become common implementation only when the policy boundary is explicit and
tests show v4/v5 behavior is unchanged.

Candidates:

- image open/read-superblock helpers;
- mmap and unmap helpers that do not assume v5 prefix metadata;
- descriptor-backed runtime context setup and cleanup;
- common FUSE operation helpers after write policy remains selected by the
  final binary.

These helpers must not silently start v5 workers, hotplug delegation, pending
log drain, tombstone GC, or background dedup for a v6 context.

### Production `kafs` Local Legacy Surface

Keep this code local until retired or moved:

- legacy `v6_inspection_mount` parsing in `kafs`, now fail-closed with
  `kafs-v6` guidance;
- legacy `v6_write_mount` parsing and controlled-write admission in `kafs`;
- `KAFS_V6_READONLY_SMOKE` and `KAFS_V6_ADMISSION_HANDOFF`;
- legacy v6 diagnostic messages emitted by `kafs`;
- production auto-migration and v2/v3/v4/v5 compatibility gates.

These paths are compatibility and smoke scaffolding, not the target v6 runtime
contract.

## T25 Result And Next Boundary

`SDW-V6RT-T25 kafs-v6 inspection admission migration` moved the read-only v6
inspection acceptance path behind `kafs-v6`.

T25:

1. move read-only v6 inspection admission out of `kafs.c` and behind
   `kafs-v6`;
2. keep controlled-write migration out of scope;
3. preserve `kafs` v4/v5 behavior and keep legacy `kafs` v6 flags fail-closed
   or diagnostic-only;
4. add tests that measure v6 acceptance through `kafs-v6`, not through the
   production `kafs` binary.

The next isolation slice should move or retire the legacy controlled-write
admission path in `kafs`. That slice should still be an isolation step, not a
write-surface expansion. If it needs more shared build machinery, the acceptable
first form remains a non-installed static archive or common object/source list.
Do not add a new runtime executable.

## Validation Standard

For boundary-only changes:

- `git diff --check`
- `./scripts/test-cli-surface.sh`
- `make -j2`

For any slice that moves runtime context setup or FUSE operation code:

- `make -C tests check TESTS=v6_descriptor_smoketest`
- `make check -j2`
- targeted checks showing `kafs` still rejects or preserves legacy v6 behavior
  while `kafs-v6` owns the new v6 admission path.
