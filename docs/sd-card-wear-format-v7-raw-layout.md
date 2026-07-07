# KAFS format v7 raw layout draft

Date: 2026-07-07
Status: draft

## Purpose

This document is the first raw image layout proposal for format v7.  It applies
the decision model in
[sd-card-wear-format-v7-inception-deck.md](sd-card-wear-format-v7-inception-deck.md):
recoverability and fsck determinism first, SD-card wear distribution second,
then journal safety, core mutation routing, HRL durability, and implementation
containment.

The draft intentionally chooses the descriptor-rooted grouped metadata family.
The first implementation slice may use a single metadata group, but the on-disk
shape must still be the v7 group layout.  A single group is an implementation
limit, not a v5 prefix compatibility mode.

## Candidate Decision

| Candidate | Result | Reason |
| --- | --- | --- |
| Descriptor-rooted grouped metadata | Baseline | Passes the gates and best matches recovery plus wear-distribution goals. |
| Descriptor veneer over v5 prefix layout | Rejected | Keeps the prefix hot spot and makes v7 a compatibility wrapper. |
| Segment-first log layout | Deferred | Attractive for wear and crash behavior, but too large for the next implementation slice. |
| Staged v7 group layout | First slice | Acceptable only as a strict subset of descriptor-rooted grouped metadata. |

## Layout Overview

Format v7 keeps a 256-byte primary `kafs_ssuperblock_t` at offset 0 only as a
discovery anchor and legacy tool rejection surface.  Successful v7 tools must
not use legacy superblock prefix offsets as the authoritative metadata layout.

The authoritative layout is the selected `K7LD` layout descriptor.  Descriptor
replicas describe metadata groups, data spans, metadata shards, checkpoint
replicas, and descriptor replicas.

```text
offset 0
  [primary 256-byte KAFS superblock, s_format_version=7]
  [padding to block boundary]
  [primary K7LD descriptor replica]

  [group 0 metadata]
    [checkpoint replica shard]
    [block bitmap shard]
    [inode table shard]
    [allocator summary shard]
    [HRL index shard]
    [HRL entry shard]
    [journal header shard]
    [journal data shard]
    [optional v7-owned pending/tail shards, or absent with fail-closed policy]
  [group 0 data]

  [group 1 metadata]
  [group 1 data]
  ...

  [optional midpoint K7LD descriptor replica]
  ...
  [tail checkpoint replica]
  [tail K7LD descriptor replica]
```

For the first implementation slice, `group_count == 1` is allowed.  Even then,
all metadata is found through descriptor and shard records, not through v5/v6
prefix offsets.

## Primary Superblock And Anchor

The primary superblock remains at byte offset 0:

- `s_magic == KAFS_MAGIC`;
- `s_format_version == 7`;
- `s_log_blksize`, `s_inocnt`, `s_blkcnt`, `s_r_blkcnt`, and hash algorithm
  fields remain basic filesystem identity fields;
- legacy offset fields such as HRL, journal, allocator, pending-log, and
  tail-metadata offsets are not authoritative for successful v7 paths;
- `s_reserved` stores a v7-owned anchor using `K7SA`.

The v7 anchor should deliberately adopt the existing 32-byte anchor shape with
v7 magic and version:

| Field | Meaning |
| --- | --- |
| `va_magic` | `K7SA`. |
| `va_version` | anchor format version, initially 1. |
| `va_flags` | zero for v7 draft 1; unknown non-zero flags are incompatible. |
| `va_primary_desc_off` | byte offset of the primary `K7LD` descriptor replica. |
| `va_primary_desc_bytes` | descriptor byte length. |
| `va_candidate_count` | deterministic descriptor candidate count. |
| `va_anchor_crc32` | CRC over the anchor with this field zeroed. |
| `va_reserved0` | zero. |

This is a v7 raw-layout decision, not old-v6 compatibility.  Code may share a
neutral parser, but the public record is owned by the v7 spec and uses v7 magic.

## Descriptor Replicas

The v7 descriptor uses `K7LD` magic and version 1.  It owns:

- descriptor header, generation, image size, block size, feature flags, and CRC;
- metadata group table;
- shard table;
- descriptor replica table.

Replica policy:

- minimum replica count is 2: primary and tail backup;
- use 3 replicas when the image is large enough for a midpoint backup without
  consuming the first or last metadata group;
- readers validate every deterministic candidate independently;
- the selected descriptor is the highest-generation valid descriptor;
- if generations tie, matching CRC descriptors are equivalent and the lower
  replica id wins;
- divergent descriptors at the selected generation are reported by `fsck.kafs`
  and must fail runtime admission until a repair policy exists;
- if no valid descriptor remains, `fsck.kafs`, `kafsdump`, and `kafs-v7` fail
  closed.

Descriptor replicas are descriptor-described through `layout_descriptor` shard
records so `kafsdump` can report them without hidden geometry.

Candidate offsets for draft 1:

| Candidate | Offset rule |
| --- | --- |
| primary | `va_primary_desc_off`, normally the first block boundary after the 256-byte primary superblock. |
| tail backup | largest block-aligned offset where `offset + descriptor_bytes <= image_size`. |
| midpoint backup | block-aligned offset nearest `image_size / 2` that can hold the descriptor without overlapping another descriptor candidate. |

The descriptor byte length is block-aligned.  Candidate offsets outside the
image, overlapping another descriptor candidate, or unable to hold the full
descriptor are treated as missing.

Descriptor updates are copy-update by generation.  A writer should write
non-primary replicas first and the primary replica last.  Readers do not trust
write order; they select by independent CRC, bounds, and generation checks.

## Metadata Groups

Each metadata group is an explicit v7-owned record.  The v7 group descriptor
should make data mapping explicit, not infer it from v5 prefix geometry.

Required group fields:

| Field | Meaning |
| --- | --- |
| `group_id` | zero-based group id. |
| `flags` | zero for v7 draft 1; unknown non-zero flags are incompatible. |
| `metadata_physical_off` / `metadata_physical_bytes` | block-aligned metadata span owned by this group. |
| `data_logical_start` / `data_logical_count` | logical filesystem block range owned by this group. |
| `data_physical_off` / `data_physical_bytes` | block-aligned physical data span for that logical range. |
| `first_shard_index` / `shard_count` | shard table range owned by this group. |
| `generation_floor` | minimum generation accepted for mutable shards in this group. |

Data block mapping is group-local and linear:

```text
physical_off = group.data_physical_off
             + (logical_block - group.data_logical_start) * block_size
```

The block bitmap shard for a group must cover the same logical data range as the
group.  `fsck.kafs` must reject gaps, overlaps, physical span overflow, or
metadata/data physical overlap.

## Shard Records

Every mutable or reportable metadata region is represented by a shard record.
The v7 shard descriptor should include:

| Field | Meaning |
| --- | --- |
| `type` | metadata region id, or `layout_descriptor`; `unknown` is not valid on disk. |
| `flags` | zero for draft 1 unless the type-specific spec defines it. |
| `group_id` | owning metadata group. |
| `storage_class` | fixed record, bit packed, allocator summary, or byte span. |
| `physical_off` / `physical_bytes` | image byte range. |
| `logical_start` / `logical_count` | type-specific logical coverage. |
| `record_bytes` | fixed record size, or 0 for byte-span records. |
| `header_bytes` | shard-local header size, initially 0 unless specified. |
| `generation_floor` | minimum generation for accepting mutable shard content. |
| `mapping_seed` | type-specific mapping seed, initially 0 for linear mapping. |

Common validation:

- physical ranges are block-aligned except the primary 256-byte superblock
  checkpoint span;
- physical ranges are inside the owning metadata group unless the type is
  `layout_descriptor` or the primary superblock checkpoint;
- writable metadata physical ranges do not overlap;
- logical ranges for each required type have no gaps and no overlaps;
- fixed-record shards have the expected `record_bytes`;
- unknown type values or unknown non-zero flags are incompatible.

## Metadata Region Decisions

| Region | v7 draft decision |
| --- | --- |
| `superblock_checkpoint` | Primary superblock at offset 0 plus descriptor-owned checkpoint replicas. Mutable counters should move to checkpoint replicas where possible. |
| `layout_descriptor` | `K7LD` descriptor replicas selected by generation and CRC. |
| `block_bitmap` | Bit-packed shards exactly cover `[0, s_r_blkcnt)`. Draft 1 requires shard logical starts and physical offsets to align to `sizeof(kafs_blkmask_t)` to keep bitmap updates word-based. |
| `inode_table` | Fixed-record shards exactly cover `[0, s_inocnt)`. Draft 1 deliberately adopts the current inode record shape as v7-owned, with root inode covered by exactly one shard. |
| `allocator_summary` | One allocator summary shard per bitmap/data range. Draft 1 deliberately adopts the existing L1/L2 summary shape and requires rebuild from authoritative bitmap shards. |
| `hrl_index` | Fixed `uint32_t` bucket-head records. Bucket coverage is exact, and each bucket resolves through exactly one shard. |
| `hrl_entries` | Fixed `kafs_hrl_entry_t` records. Entry-id coverage is exact, chains must stay within covered entry shards, and index/entry group consistency is required. |
| `journal_header` | Fixed `kj_header_t` segment records with generation and CRC. Segment id coverage must match `journal_data`. |
| `journal_data` | Byte-span segment records. Each segment has an equal byte range, matching header segment id, and replay scans valid generations. |
| `pending_log` | Disabled for draft 1 write admission. Any runtime path that would require it fails closed until a v7-owned pending-log layout is accepted. |
| `tail_metadata` | Disabled or retired for draft 1 write admission. Tail packing, normalization, reclaim, and tombstone GC paths fail closed until a v7-owned layout is accepted. |

## Journal Layout

Journal segments are descriptor-owned.  Each segment id must resolve to exactly
one `journal_header` record and one `journal_data` byte-span range.  Header and
data for a segment should belong to the same metadata group in draft 1.

Selection and replay:

- validate every segment header CRC and area bounds;
- validate data record CRCs inside each segment span;
- select the highest-generation valid segment;
- break equal-generation ties by lower segment id;
- recover from a torn newer segment when an older valid segment remains;
- fail closed if no valid segment remains;
- write/replay/reset only the selected descriptor-backed segment, never the v5
  prefix journal range.

Draft 1 deliberately adopts `kj_header_t` and the existing journal record
format as v7-owned records.  The placement and segment selection policy are v7
specific.

## HRL Layout

HRL is first-class mutable metadata in v7.  It is not a reporting add-on.

Draft 1 deliberately adopts the existing HRL record shapes:

- `hrl_index` record: `uint32_t` bucket head;
- `hrl_entries` record: `kafs_hrl_entry_t`.

Validation:

- all buckets are covered exactly once;
- all entry ids are covered exactly once;
- every non-zero bucket head references an in-range entry id;
- every chain terminates without loops;
- every chain entry belongs to the entry shard expected for the owning bucket
  group;
- unreadable records, out-of-range ids, loops, or cross-group chain violations
  fail fsck and runtime admission.

## Data Block Address Space

The v7 descriptor owns logical-to-physical data mapping.  `s_first_data_block`
is not the authoritative source for successful v7 paths.

Draft 1 uses group-local linear mapping:

- each group owns a contiguous logical data-block range;
- each group owns a contiguous physical data span;
- metadata reservations are outside the data physical span;
- block bitmap and allocator summary shards cover the same logical range as
  the group's data span;
- `mkfs.kafs` rejects v7 images where metadata reservations leave no usable data
  block.

Future non-linear placement would require a new incompatible layout flag or a
later format version.

## Feature Flags And Compatibility

Unknown v7 descriptor incompat flags are fatal.  Unknown read-only-compatible
flags may permit `kafsdump` reporting but must not permit runtime admission
unless the runtime explicitly supports them.

Draft 1 flags:

| Flag class | Initial policy |
| --- | --- |
| incompat | grouped descriptor layout, descriptor-owned data mapping, descriptor-owned journal segments. |
| ro-compat | none initially. |
| compat/debug | report-only flags may be added only when older tools fail closed or ignore them safely. |

All v7-owned numeric fields are little-endian.  Fixed-size records must specify
their packed size and overflow rules before code writes them.

## Small-Image Policy

`mkfs.kafs --format-version 7` must reject an image that cannot fit:

- primary superblock and anchor;
- at least two descriptor replicas;
- one metadata group;
- checkpoint, bitmap, inode, allocator, HRL, journal header, and journal data
  shards;
- at least one usable data block after metadata reservations.

Small images may use `group_count == 1`, but they must not fall back to v5
prefix metadata.

## First Implementation Slice

The recommended first slice is candidate D as a strict subset of candidate A:

- create a v7 image with `group_count == 1`;
- produce at least primary and tail `K7LD` descriptor replicas;
- keep all metadata access descriptor-owned;
- support `fsck.kafs` and `kafsdump` validation/reporting before runtime
  admission;
- keep pending log and tail metadata disabled/fail-closed;
- use v7-owned names and structs even when record shapes are deliberately
  adopted from existing KAFS records.

The first slice should not enable broad write runtime admission.  It should
first prove mkfs, dump, fsck, and `kafs-v7 --inspection-mount` against this raw
layout.

## Open Questions

- Should the v7 descriptor record copy the v6 table field order with v7-owned
  struct names, or add `storage_class` and explicit data logical range fields
  before any implementation?
- Should mutable free counts remain in the primary superblock for draft 1, or
  move immediately to checkpoint replicas?
- Should journal segments be strictly group-local, or can a global journal
  group own all segments while other metadata remains group-local?
- Should HRL entry group consistency be mandatory for all images, or a draft-1
  fsck policy that can later be relaxed by an incompatible flag?
- What minimum descriptor replica count should `mkfs.kafs` require for very
  small test images?
