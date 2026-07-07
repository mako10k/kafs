# KAFS format v7 pivot

Date: 2026-07-07
Status: accepted

## Decision

Format v6 is closed as an experimental implementation.  It proved the
descriptor-backed runtime entrypoint split, controlled-write opt-in boundary,
and production `kafs` fail-closed behavior, but it is not the compatibility
surface that future work should preserve.

Format v7 is the breaking-change continuation of that work:

- new images may be created with `mkfs.kafs --format-version 7`;
- format v7 runtime requests go through `kafs-v7`;
- production `kafs` remains the v4/v5 runtime and fails closed for v6/v7;
- v6 remains available for existing experimental tests and images, but new
  layout or policy expansion should target v7 unless explicitly scoped as a v6
  regression fix.

## Current Implementation Boundary

The first v7 slice has a separate v7 entrypoint/runtime/adapter source set:
`kafs_v7.c`, `kafs_v7_runtime.*`, `kafs_v7_mount_options.*`, and
`kafs_v7_entrypoint_adapter.*`.  The remaining historical dependency is the
low-level descriptor scaffold parser/builder in `kafs_v6_layout.h`; it is a
temporary scaffold-family helper, not the public v7 runtime contract.

Diagnostic keys are format-specific:

- v6 keeps `v6_layout_descriptor`, `v6_bitmap_shards`, and
  `v6_journal_segments` for existing diagnostic consumers.
- v7 uses `layout_descriptor`, `bitmap_shards`, and `journal_segments`.

User-facing entrypoints and on-disk format numbers are no longer ambiguous:

| Surface | Role |
| --- | --- |
| `kafs` | Production v4/v5 runtime. Rejects v6/v7 descriptor-backed images. |
| `kafs-v6` | Frozen experimental v6 runtime entrypoint. |
| `kafs-v7` | Breaking-change descriptor-backed runtime entrypoint. |
| `mkfs.kafs --format-version 7` | Creates a format v7 image using the current descriptor scaffold. |
| `fsck.kafs` / `kafsdump` | Validate/report descriptor-backed v6/v7 images offline. |

## Non-Goals

- Do not add old-v6 compatibility gates solely to preserve the experimental v6
  shape.
- Do not widen production `kafs` to admit v6/v7 runtime mounts.
- Do not rename the whole internal descriptor scaffold in the same slice as the
  v7 entrypoint unless that rename is independently validated by LSP-backed
  refactoring.

## Follow-Up Boundaries

1. Move the remaining low-level descriptor scaffold names from `v6` to a
   neutral descriptor family name where it reduces ambiguity.
2. Add `kafsresize --migrate-create --format-version 7` once the v7 mkfs and
   offline validation surface is stable.
3. Prove `kafs-v7 --inspection-mount` and then controlled-write runtime paths
   with mount tests before expanding the write surface.
