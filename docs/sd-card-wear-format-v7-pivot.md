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
- v6 is being removed under the phased
  [v6 source retirement plan](sd-card-wear-v6-retirement-plan.md); it is not a
  compatibility, feature, or regression-fix target.

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
- Entry surfaces remain format-specific: `kafs` owns v4/v5, the temporary
  `kafs-v6` placeholder rejects retired v6 runtime use, and `kafs-v7` owns v7.

Format v7 descriptor discovery, superblock anchor initialization, mkfs
descriptor build, and v7 wire magic are owned by `kafs_v7_layout.h`.  The v7
wire identifiers are separate from experimental v6: the superblock descriptor
anchor uses `K7SA`, and the layout descriptor uses `K7LD`.

The neutral `kafs_descriptor_layout.h` implementation is a low-level descriptor
scaffold shared by descriptor-backed formats. It owns the in-memory descriptor
structures, selected-descriptor loading, coverage validation, journal segment
validation, and explicit-wire helpers. The former `kafs_v6_layout.h` name
adapter has been removed; offline v6 readers call the neutral implementation
directly. It is not the public v7 layout entrypoint. The unqualified legacy
builder/discovery values remain v6 wire values until offline v6 creation and
diagnostics are retired; v7 keeps its wire contract in `kafs_v7_layout.*` and
does not use those legacy defaults.

The current builder emits accepted `K7SA` locator / `K7LD` descriptor version 2
with v7-owned group, shard, checkpoint, and replica records.  It supports the
canonical single- and multi-group offline geometry.  Pre-specification version
1 scaffold images may be reported with recreate guidance, but they are not
accepted raw-layout images and must not pass v7 runtime admission or repair.

The offline recovery proof covers primary/tail copies emitted by mkfs and an
accepted three-copy sparse fixture with a midpoint recovery neighborhood.  It
exercises independent descriptor/checkpoint loss, one- and two-neighborhood
loss, asymmetric surviving copies, interrupted generation publication, and
same-generation divergence.  These are filesystem-offset fault domains, not a
claim about an SD controller's physical erase-block or FTL placement.

Diagnostic keys are format-specific:

- v6 keeps `v6_layout_descriptor`, `v6_bitmap_shards`, and
  `v6_journal_segments` for existing diagnostic consumers.
- v7 uses `layout_descriptor`, `bitmap_shards`, and `journal_segments`.

User-facing entrypoints and on-disk format numbers are no longer ambiguous:

| Surface | Role |
| --- | --- |
| `kafs` | Production v4/v5 runtime. Rejects v6/v7 descriptor-backed images. |
| `kafs-v6` | Temporary fail-closed placeholder for the retired v6 runtime; removed in the final retirement phase. |
| `kafs-v7` | Breaking-change descriptor-backed entrypoint; admits accepted v7 images for read-only inspection or explicitly bounded controlled write. |
| `mkfs.kafs --format-version 7` | Emits the accepted version 2 grouped raw layout for offline validation and inspection. |
| `fsck.kafs` / `kafsdump` | Validate/report descriptor-backed v6/v7 images offline. |

## Runtime Implementation History

This section records the incremental path from inspection admission to the
current controlled-write surface. Statements that a later slice remained
outside an earlier checkpoint are historical boundaries; the current matrix is
maintained in
[sd-card-wear-v7-capability-rebaseline-20260721.md](sd-card-wear-v7-capability-rebaseline-20260721.md).

The read-only inspection blockers identified by the 2026-07-16 source/docs
audit are now closed for the bounded initial runtime surface:

1. The validator-selected `K7LD`/`K7CP` state is retained in a v7-owned runtime
   view without using a v6 public wire/layout entrypoint.
2. The accepted raw-layout specification explicitly owns the `KDIR` version 1
   namespace contract for v7; mkfs emits a canonical empty root payload.
3. Inode shard lookup, retained `N+1` references, and group-local logical data
   mapping are used by the shared read path. Logical block zero remains
   distinguishable from a zero/hole reference.
4. `statfs` reports `s_r_blkcnt` and the selected `K7CP` recovered counters.
5. Admission is intentionally bounded to `checkpoint_seq == 0` with empty
   journal segments. Offline validation can parse and simulate non-empty
   journals, but runtime admission still rejects any selected non-empty
   segment. The image is opened and mapped read-only and runtime mutation
   guards return `EROFS`.
6. The mount regression covers multi-group nested lookup/readdir, inline and
   block-backed reads, symlinks, degraded recovery-pair inspection, unpaired
   publication rejection before FUSE, mutation rejection, and an unchanged
   image digest after unmount.

The offline `K7JB/K7JM/K7JC/K7JA` parser and idempotent replay simulation are
implemented in v7-owned code. They validate rotating selected prefixes,
transaction control and mutation records, global sequence gaps/divergence,
before/after target chains, and recovered counters without writing the image.

The v7-owned mutation router is also implemented. It resolves every canonical
bitmap, inode, allocator-summary, HRL-index, and HRL-entry identity through the
selected descriptor to one group and one physical target. Transaction planning
is output-atomic and rejects cross-group or aliased targets before a journal
begin record can be emitted.

The v7-owned checkpoint publisher now writes and flushes full `K7CP` blocks one
at a time, verifies at least two byte-identical selected-generation copies, and
resumes an interrupted one-copy generation without advancing it. It retains
the previous selected copy on three-replica images until two new copies exist;
journal reclamation and runtime write admission remain outside this slice.

The v7-owned locking primitive now serializes transactions through an exclusive
write gate, then takes the filesystem sequence and exactly one group lock in
ranks 1, 2, and 3. Checkpoint publication takes the write gate alone. The lock
wait is bounded and observable, cancellation is disabled while held, and a
stale owner fails the current operation instead of silently continuing. This
correctness-first RC boundary deliberately does not provide concurrent
cross-group transactions. The v7 and existing metadata wrappers now publish
rank and lock identity to one format-neutral per-thread stack. Acquiring a v7
rank while any metadata rank 10-50 is held returns `EDEADLK`; the forward v7
rank 1-3 -> metadata rank 10-50 order remains permitted and must unwind in
strict reverse order.

The v7-owned global sequence state now derives the last visible sequence from a
validated checkpoint/journal view, reserves exactly the next value while the
composite transaction lock remains held, and consumes it only after fresh
validation finds that exact sequence in the reserved group's selected journal
prefix. A pre-header cancellation proves the sequence is absent before reuse;
ambiguous publication poisons the state and blocks later reservations.

The v7-owned journal writer now encodes commit and abort transactions against
the recovered overlay view, validates group-local routing and exact allocator
deltas, and binds the opaque transaction to the active sequence token and
thread. It appends to the eligible group segment with the lowest selected
header generation, flushes transaction data, rotates one `K7JH` slot, and
flushes that header. Unpublished data therefore remains outside the selected
prefix, and multi-segment groups spread header generations before reusing a
segment.

The multi-group mutation fault matrix now fixes four-group interleaving and
global fail-closed behavior. It accepts group-local transactions ordered
0,3,1,2 by their filesystem-global sequence, and rejects cross-group sequence
collision/gap, loss of a middle group's selected payload/header, and a
checksum-consistent mutation that claims a foreign group. This does not enable
cross-group atomic transactions.

The v7 metadata closeout now materializes replay after-images, flushes and
read-backs those targets, publishes two byte-identical covering checkpoints,
and only then rotates covered group-local segments to flushed empty headers.
It restores an interrupted one-copy checkpoint before changing metadata and
resumes cleanly after target apply, checkpoint publication, or a partial
segment reset.

The v7-owned mount-lifetime transaction coordinator now holds this rank 1-3
state across sequence reservation and group-local journal publication, then
runs metadata apply, two-copy checkpoint publication, and covered journal
reclamation before returning success. It does not route through a v5/v6
transaction entrypoint.

The v7-owned data COW/allocator planner selects group-local free blocks from the
authoritative bitmap/L1/L2 overlay, stages and verifies up to twelve direct
blocks, and publishes direct references, inode attributes, and allocator
after-images in one transaction. Independent post-checkpoint retirement clears
retained blocks only after direct, indirect-walk, and HRL reference checks.

Controlled write is now explicitly admitted for the bounded same-group direct
surface: partial/multi-block overwrite, contiguous growth, direct shrinking
truncate above the inline boundary or to zero, `O_TRUNC`, empty regular-file create, inline-file write,
inline-to-one-direct-block regular-file promotion, and inline/direct directory
append/growth. FUSE negotiation caps and reports the per-request atomic boundary.
Recovery tests cover journal publication, metadata apply, checkpoint-copy, and
journal-reclaim interruptions, including promotion-specific recovery. Indirect
mutation, non-zero direct-to-inline conversion, holes, cross-group allocation,
and unrelated metadata mutation remain fail closed. The v7 policy state and
helpers are v7-owned; production `kafs` and frozen `kafs-v6` behavior remain
separate.

V7-owned direct/single/double/triple address calculation, walking, and
retirement guards now exist. That foundation does not admit indirect write,
create, growth, or truncate.

FTL/ECC correlated-failure injection is not in this implementation blocker
list.  It is governed by the RC media-qualification boundary in the accepted
raw-layout specification and does not relax any software recovery gate.

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

1. Run the M7 controlled-write RC qualification gate against the current
   bounded surface. Add the non-destructive procedure and evidence contract
   first; require explicit operator authorization for the exact real-device
   sample matrix and destructive actions.
2. T49 is the bounded exception made while exact real-media preparation is
   deferred: it closes regular-file inline-to-one-direct-block promotion and
   refreshes both qualification matrices without making an RC claim.
3. T50 closes the corresponding common-admission gap: every allocated inode
   has a zero disabled tail, and inline records have zero block count and zero
   padding. It does not add repair or widen mutation admission.
4. Keep M8-C indirect mutation behind M7. Address calculation and traversal do
   not justify widening journal/recovery admission before the direct surface is
   qualified.
5. Treat full M9 migration/cutover as distinct from the implemented v7
   destination-image creation path, and keep M10 cross-group mutation behind an
   explicit design-direction decision.
