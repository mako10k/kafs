# KAFS format v7 pivot

Date: 2026-07-07
Status: accepted

## Decision

Format v6 is closed as an experimental implementation.  It proved the
descriptor-backed runtime entrypoint split, controlled-write opt-in boundary,
and production `kafs` fail-closed behavior, but it is not the compatibility
surface that future work should preserve.

Format v7 is the breaking-change continuation of that work:

- `mkfs.kafs --format-version 7` exists, but its current pre-specification
  output is not an accepted v7 image; accepted creation begins only after the
  descriptor version 2 offline round trip lands;
- format v7 runtime requests go through `kafs-v7`;
- production `kafs` remains the v4/v5 runtime and fails closed for v6/v7;
- v7 is not a v6 compatibility layer and should not preserve experimental v6
  behavior unless a compatibility exception is explicitly accepted;
- v6 remains available for existing experimental tests and images, but new
  layout or policy expansion should target v7 unless explicitly scoped as a v6
  regression fix.

The v7 decision model in
[sd-card-wear-format-v7-inception-deck.md](sd-card-wear-format-v7-inception-deck.md)
and the v7 raw layout in
[sd-card-wear-format-v7-raw-layout.md](sd-card-wear-format-v7-raw-layout.md)
were accepted on 2026-07-13.  They make fault tolerance and SD-card wear
distribution the joint top priorities, with deterministic fsck recovery or
rejection as a hard gate.  New v7 mkfs/runtime behavior must conform to the
accepted `K7LD` descriptor version 2 contract.

## Current Implementation Boundary

The v7 runtime has a separate v7 entrypoint/runtime/adapter source set:
`kafs_v7.c`, `kafs_v7_runtime.*`, `kafs_v7_mount_options.*`, and
`kafs_v7_entrypoint_adapter.*`.

Resource ownership is intentionally strict:

- v5-and-earlier, v6, and v7 resources are separate ownership boundaries.
- v7 code must not directly call v5/v6-owned public entrypoints to get a
  successful v7 path.
- If v7 needs behavior that currently lives behind a v5/v6-owned file or
  function, copy it into v7-owned resources or extract a neutral helper first.
- Neutral helpers must be named and documented as neutral scaffolding, not as a
  bridge or compatibility layer between formats.
- Entry surfaces remain format-specific: `kafs` owns v4/v5, `kafs-v6` owns
  frozen experimental v6, and `kafs-v7` owns v7.

Format v7 descriptor discovery, superblock anchor initialization, mkfs
descriptor build, and v7 wire magic are owned by `kafs_v7_layout.h`.  The v7
wire identifiers are separate from experimental v6: the superblock descriptor
anchor uses `K7SA`, and the layout descriptor uses `K7LD`.

The neutral `kafs_descriptor_layout.h` facade is a low-level descriptor
scaffold shared by descriptor-backed formats.  It provides selected-descriptor
loading, coverage validation, journal segment validation, and explicit-wire
helper functions used by the v6/v7 layout entrypoints.  It is not the public v7
layout entrypoint.  The backing in-memory structs still originate from the
experimental v6 descriptor scaffold, so the facade currently delegates to
`kafs_v6_layout.h`; that delegation is an implementation detail, not the
public v7 runtime contract.

The current builder emits accepted `K7SA` locator / `K7LD` descriptor version 2
with v7-owned group, shard, checkpoint, and replica records.  It supports the
canonical single- and multi-group offline geometry.  Pre-specification version
1 scaffold images may be reported with recreate guidance, but they are not
accepted raw-layout images and must not pass v7 runtime admission or repair.

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
| `mkfs.kafs --format-version 7` | Emits the accepted version 2 grouped raw layout for offline validation. |
| `fsck.kafs` / `kafsdump` | Validate/report descriptor-backed v6/v7 images offline. |

## Non-Goals

- Do not add old-v6 compatibility gates solely to preserve the experimental v6
  shape.
- Do not directly reuse v5/v6-owned entrypoints, layout APIs, or admission
  paths from v7 as a shortcut. Move the code into v7-owned resources or extract
  a neutral helper first.
- Do not widen production `kafs` to admit v6/v7 runtime mounts.
- Do not rename the whole internal descriptor scaffold in the same slice as the
  v7 entrypoint unless that rename is independently validated by LSP-backed
  refactoring.

## Follow-Up Boundaries

1. Extend descriptor/checkpoint replica placement and same-generation
   divergence testing with a real-media-oriented fault matrix.
2. Prove `kafs-v7 --inspection-mount` with mount tests while keeping the write
   surface closed.
3. Add `kafsresize --migrate-create --format-version 7` once the accepted v7
   mkfs, offline validation, and inspection surfaces are stable.
4. Prove controlled-write admission, structured-journal recovery, locking, and
   mutation fault tests before expanding the write surface.
