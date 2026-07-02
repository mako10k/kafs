# KAFS format v6 cutover preparation policy

Date: 2026-07-02
Status: accepted

## Policy

Until the v6 production cutover is explicitly accepted, format v6 is not a
stable compatibility contract. The v6 on-disk format, mount contract, and
feature set may change drastically when that is the cleaner way to reach the
v6 target design. This does not loosen v4/v5 compatibility: the production
`kafs` runtime must continue to protect v4/v5 behavior.

The implementation target for v6 is the pure v6 feature set, not the closest
shape that can fit the v5 runtime assumptions. When a v5 compatibility rule
forces awkward v6 layout or behavior, record the pressure point and prefer a
v6-native design before expanding runtime support.

Runtime binaries are final deliverables only:

- `kafs`: production runtime for v4/v5 and legacy images before v6 cutover.
- `kafs-v6`: dedicated runtime entrypoint for format v6.

Shared implementation is allowed through common object or library artifacts
such as `.o`, `.a`, or future `.so` outputs. Common source lists and helper
objects are acceptable when the policy boundary stays at the final runtime
binary. Do not create user-facing helper binaries just to share code.

## Current v5 Compatibility Pressure Points

| Area | v5 pressure | v6-native direction |
| --- | --- | --- |
| Runtime entrypoint | Historical `kafs` v6 flags mix v6 admission policy into the v4/v5 production binary. | Move v6 admission and future write policy behind `kafs-v6`; keep `kafs` v6 paths legacy-only until retired. |
| Metadata placement | v5 assumes prefix-derived bitmap, inode, allocator, HRL, and journal offsets. | Treat the selected v6 descriptor as the only source of truth after anchor discovery. |
| Superblock geometry | Existing helpers compute mmap layout from v4/v5-style offsets. | v6 mount setup should build descriptor-backed views first, then expose only the views needed by the runtime. |
| Journal layout | v4/v5 journal data starts after the legacy header region. | v6 journal header/data segment selection comes from descriptor-backed segment pairs. |
| Tail metadata | v5 tail packing uses legacy inode/tail metadata storage. | Either define descriptor-backed tail metadata as a v6 feature or keep tail packing disabled for v6. |
| Pending log and background mutation | v5 runtime can start pending, tombstone GC, and background dedup workers from generic write context setup. | v6 runtime must explicitly admit descriptor-backed equivalents or reject those features at the `kafs-v6` policy boundary. |
| Hotplug write delegation | v5 hotplug assumes the existing mounted write surface. | v6 should not inherit hotplug write delegation until descriptor-backed write and recovery semantics are covered. |
| fsck repair | v5 repair/write modes are tied to legacy metadata placement. | v6 repair remains detect-only until descriptor replica repair, journal reset, and shard repair have v6-native ordering and tests. |
| Operator cutover | v5 cutover assumes the mounted target is production-capable. | v6 cutover requires a separate acceptance gate for the `kafs-v6` runtime contract. |

## Immediate Preparation Tasks

1. Keep `kafs_v6_runtime.c` linked only into the v6 runtime entrypoint unless a
   later shared-object/library boundary makes the dependency explicit.
2. Add a concrete common-object or static-library plan before moving more FUSE
   runtime setup behind `kafs-v6`. Completed by
   [sd-card-wear-v6-shared-artifact-boundary-plan.md](sd-card-wear-v6-shared-artifact-boundary-plan.md).
3. Move current `kafs` v6 inspection and controlled-write admission code to
   `kafs-v6`, then deprecate or hide the legacy `kafs` v6 flags.
4. Rework docs and tests so v6 acceptance is measured through `kafs-v6`, while
   `kafs` tests prove v4/v5 behavior is unchanged.
5. Before adding more v6 operations, decide whether each feature is
   v6-native, temporarily disabled, or retained only for migration/staging.
