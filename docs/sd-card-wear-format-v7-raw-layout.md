# KAFS format v7 raw layout specification

Date: 2026-07-07
Accepted: 2026-07-13
Status: accepted

## Purpose

This document defines the accepted raw image layout for format v7.  It applies
the decision model in
[sd-card-wear-format-v7-inception-deck.md](sd-card-wear-format-v7-inception-deck.md):
fault tolerance and deterministic fsck recovery must not be weakened, while
repeated metadata writes are distributed away from a single hot prefix.  After
those primary concerns come journal safety, core mutation routing, HRL
durability, and implementation containment.

The accepted layout uses the descriptor-rooted grouped metadata family.
The first implementation slice may use a single metadata group, but the on-disk
shape must still be the v7 group layout.  A single group is an implementation
limit, not a v5 prefix compatibility mode.

## Accepted Candidate Decision

| Candidate | Result | Reason |
| --- | --- | --- |
| Descriptor-rooted grouped metadata | Accepted | Passes the gates and best matches recovery plus wear-distribution goals. |
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
  [primary K7CP checkpoint replica]

  [group 0 metadata]
    [block bitmap shard]
    [inode table shard]
    [allocator summary shard]
    [HRL index shard]
    [HRL entry shard]
    [journal header shard]
    [journal data shard]
    [pending/tail shards absent; related paths fail closed]
  [group 0 data]

  [group 1 metadata]
  [group 1 data]
  ...

  [optional midpoint K7LD descriptor replica]
  [optional midpoint K7CP checkpoint replica]
  ...
  [tail checkpoint replica]
  [tail K7LD descriptor replica]
  [reserved final block; tail K7SA root-locator in its final 32 bytes]
```

Small images may retain `group_count == 1`.  Larger images use the canonical
multi-group planner below.  In both cases all metadata is found through
descriptor and shard records, not through v5/v6 prefix offsets.

## Primary Superblock And Anchor

The primary superblock remains at byte offset 0:

- `s_magic == KAFS_MAGIC`;
- `s_format_version == 7`;
- `s_log_blksize`, `s_inocnt`, `s_blkcnt`, `s_r_blkcnt`, and hash algorithm
  fields remain basic filesystem identity fields.  For v7, `s_blkcnt` is the
  physical image block count and `s_r_blkcnt` is the descriptor-mapped logical
  data-block namespace size; `s_blkcnt * block_size` must equal the detected
  image length using checked arithmetic;
- accepted v7 block sizes are powers of two from 1024 through 65536 bytes;
  `s_log_blksize` stores `log2(block_size) - 10`, therefore its accepted decoded
  value is 0 through 6;
- `s_hash_algo_fast == 2` means FNV-1a 64 and
  `s_hash_algo_strong == 0` means no stored strong digest in descriptor version
  2; other values are incompatible.  This new fast-hash id avoids reusing the
  existing id 1, which denotes XXH64 in older formats;
- legacy offset fields such as HRL, journal, allocator, pending-log, and
  tail-metadata offsets, including `s_first_data_block`, are not authoritative
  for successful v7 paths; version 2 mkfs writes them as zero, while readers
  ignore rather than require their value;
- `s_reserved[0..31]`, absolute superblock bytes 208--239, stores the primary
  v7-owned root locator using `K7SA`; `s_reserved[32..47]`, bytes 240--255, is
  zero.

Bytes from the end of the 256-byte superblock through the end of the primary
root block are zero.  No other resource may share that block.

The accepted v7 locator uses a 32-byte `K7SA` version 2 record.  Version 2 adds
the block size needed to recover deterministic tail/midpoint descriptor offsets
without trusting the primary superblock or primary descriptor:

| Field | Meaning |
| --- | --- |
| `va_magic` | `K7SA`. |
| `va_version` | root-locator version, 2. |
| `va_flags` | zero for version 2; unknown non-zero flags are incompatible. |
| `va_primary_desc_off` | byte offset of the primary `K7LD` descriptor replica. |
| `va_primary_desc_bytes` | descriptor byte length. |
| `va_candidate_count` | deterministic descriptor candidate count. |
| `va_anchor_crc32` | CRC over the anchor with this field zeroed. |
| `va_block_size` | filesystem block size used to derive candidate offsets. |

```text
+0   u32  magic = K7SA              +4   u16  version = 2
+6   u16  flags = 0                 +8   u64  primary_desc_off
+16  u32  primary_desc_bytes        +20  u32  candidate_count
+24  u32  anchor_crc32              +28  u32  block_size
```

`candidate_count` is 2 or 3 and equals the selected descriptor's
`replica_count`.  `primary_desc_bytes` equals its `descriptor_bytes`.
`block_size` is one of the accepted 1024--65536-byte powers of two, matches the
selected descriptor and valid primary identity when present, and aligns all
descriptor candidates.
`image_size_bytes` must be an exact multiple of this block size.  The locator
CRC covers all 32 bytes with its CRC field zeroed, using the common CRC
algorithm below.

The same 32-byte `K7SA` record is copied byte-for-byte to the final 32 bytes of
the image.  Offline discovery validates the primary and tail locators
independently.  One valid locator is sufficient to find descriptor candidates;
two valid but non-byte-identical locators are divergent and fail closed.  A
valid tail locator permits `fsck.kafs` to diagnose a damaged primary superblock
and, with explicit offline repair, restore only its `K7SA` locator bytes.  It
does not contain enough identity information to reconstruct the rest of the
primary superblock.  Runtime admission still requires valid primary identity
fields.

If neither locator is valid, tools fail closed and do not scan the image for a
plausible descriptor magic.  Heuristic salvage may be a separate explicit
offline mode later, but it cannot produce normal admission evidence.

The whole final filesystem block is a recovery-root reservation.  Bytes before
the final 32-byte locator in that block are zero, and no descriptor, checkpoint,
group, shard, or data span may overlap the block.  Reserving the block rather
than only 32 bytes makes a torn or partially rewritten neighboring resource
unable to consume the tail discovery root.

The locators are immutable after mkfs because descriptor size and candidate
locations do not change in place.  This avoids making offset 0 a single point
of discovery failure without creating a new recurring write hot spot.  It is a
v7 raw-layout decision, not old-v6 compatibility.  Code may share a neutral
parser, but the public record is owned by the v7 spec and uses v7 magic.

## Descriptor Version Boundary

The accepted raw layout uses `K7LD` descriptor version 2.  Descriptor version 1
was emitted by the pre-specification v7 scaffold and used the experimental v6
table shape behind a neutral facade.  It is not an accepted v7 raw layout.

- `kafsdump` and `fsck.kafs` may identify a readable version 1 image as a
  pre-specification scaffold and direct the operator to recreate it offline;
- version 1 must not pass `kafs-v7` runtime admission or repair;
- `mkfs.kafs --format-version 7` must emit version 2 after the version 2 builder
  lands;
- accepted images use `K7SA` locator version 2; the pre-specification version 1
  anchor remains part of the scaffold format and is not upgraded in place.

This version boundary prevents the new v7-owned group, shard, and checkpoint
records from being decoded as the already-emitted scaffold layout.

## Fixed Wire Record Contract

All numeric fields are little-endian and all records are packed.  Implementations
must enforce both `sizeof` and field offsets with compile-time assertions.  The
accepted fixed sizes are:

| Record | Magic/version | Packed bytes |
| --- | --- | ---: |
| root locator (primary anchor and tail copy) | `K7SA` / 2 | 32 |
| layout descriptor header | `K7LD` / 2 | 128 |
| metadata group descriptor | v7 descriptor version 2 | 96 |
| metadata shard descriptor | v7 descriptor version 2 | 96 |
| descriptor replica record | v7 descriptor version 2 | 32 |
| mutable checkpoint record | `K7CP` / 1 | 64 |
| bitmap word | v7 descriptor version 2 | 8 |
| inode record | v7 descriptor version 2 | 128 |
| HRL index record | v7 descriptor version 2 | 4 |
| HRL entry record | v7 descriptor version 2 | 24 |
| rotating journal-header slot | `K7JH` / 1 | 64 |
| journal record header | `K7JR` | 20 |
| journal transaction-control payload | v7 descriptor version 2 | 32 |
| journal mutation payload header | v7 descriptor version 2 | 56 |

Magic mnemonics denote the following host values after little-endian decode:
`K7SA=0x4b375341`, `K7LD=0x4b374c44`, `K7CP=0x4b374350`, and
`K7JH=0x4b374a48`.  They are stored as little-endian u32 values; the mnemonic
is not a claim that a raw byte dump is in display order.

### Common CRC and counter rules

Every v7 CRC field uses CRC-32/ISO-HDLC over bytes in on-disk order: reflected
polynomial `0xEDB88320`, initial register `0xffffffff`, reflected input/output,
and final XOR `0xffffffff`.  Equivalently, this is the result produced by the
existing `kj_crc32()` algorithm when called with an initial API value of zero.
Before calculation, the CRC field in the covered record is set to zero.

The exact covered spans are:

| Record | CRC span |
| --- | --- |
| `K7SA` | all 32 locator bytes |
| `K7LD` | all `descriptor_bytes`, including tables and zero padding |
| `K7CP` | all 64 checkpoint bytes |
| `K7JH` | one 64-byte journal-header slot |
| `K7JR` | its 20-byte record header plus `payload_bytes`; alignment padding is excluded |

A matching CRC proves integrity only.  Replica equivalence is always a
byte-for-byte comparison of the complete covered span; a CRC collision never
makes different records equivalent.  Descriptor generation, checkpoint
generation, journal-header generation, and journal transaction sequence values
must never wrap.  An increment from `UINT64_MAX` fails closed with an overflow
diagnostic.

The v7-owned layout descriptor header deliberately adopts the proven table
directory shape, but not the v6 public type or entrypoint:

```text
+0   u32  magic = K7LD              +4   u16  version = 2
+6   u16  header_bytes = 128        +8   u32  descriptor_bytes
+12  u32  flags = 0                 +16  u64  generation
+24  u64  image_size_bytes          +32  u32  block_size
+36  u32  group_count               +40  u32  group_desc_off
+44  u16  group_desc_bytes = 96     +46  u16  mapping_policy
+48  u32  shard_count               +52  u32  shard_desc_off
+56  u16  shard_desc_bytes = 96     +58  u16  reserved0 = 0
+60  u32  replica_count             +64  u32  replica_desc_off
+68  u16  replica_desc_bytes = 32   +70  u16  reserved1 = 0
+72  u64  feature_flags             +80  u64  incompat_flags
+88  u64  ro_compat_flags           +96  u64  mapping_seed
+104 u32  descriptor_crc32          +108 u32  reserved2 = 0
+112 u64  reserved3 = 0             +120 u64  reserved4 = 0
```

The v7 metadata group record is not the v6 64-byte block-range record.  Its
96-byte field order is:

```text
+0   u32  group_id                  +4   u32  flags
+8   u32  first_shard_index         +12  u32  shard_count
+16  u64  metadata_physical_off     +24  u64  metadata_physical_bytes
+32  u64  data_logical_start        +40  u64  data_logical_count
+48  u64  data_physical_off         +56  u64  data_physical_bytes
+64  u64  generation_floor          +72  u64  reserved0 = 0
+80  u64  reserved1 = 0             +88  u64  reserved2 = 0
```

The v7 metadata shard record carries storage semantics explicitly:

```text
+0   u16  type                      +2   u16  storage_class
+4   u32  flags                     +8   u32  group_id
+12  u32  reserved0 = 0             +16  u64  physical_off
+24  u64  physical_bytes            +32  u64  logical_start
+40  u64  logical_count             +48  u32  record_bytes
+52  u32  header_bytes              +56  u64  generation_floor
+64  u64  mapping_seed              +72  u64  reserved1 = 0
+80  u64  reserved2 = 0             +88  u64  reserved3 = 0
```

`storage_class` values are v7-owned: `0=invalid`, `1=fixed_record`,
`2=bit_packed`, `3=allocator_summary`, and `4=byte_span`.  Zero and unknown
values are incompatible, which prevents a zero-initialized missing field from
being accepted accidentally.
`group_id == UINT32_MAX` is reserved for the global recovery resources
`layout_descriptor` and `superblock_checkpoint`; all other shard types require
`group_id < group_count`.

The descriptor replica record remains a v7-owned 32-byte record:

```text
+0   u32  replica_id                +4   u16  role
+6   u16  flags = 0                 +8   u64  physical_off
+16  u32  descriptor_bytes          +20  u32  reserved0 = 0
+24  u64  reserved1 = 0
```

`replica_id` must equal the replica-table index.  Role values are
`0=primary`, `1=tail`, and `2=midpoint`; any other value, a duplicate role, or
a role at the wrong index is fatal.  `descriptor_bytes` and `physical_off`
must match the deterministic candidate for that role.

Every table calculation must reject overflow before addition or multiplication:
validate `count <= (container_bytes - table_off) / entry_bytes`, not a wrapped
product.  Range checks use `bytes <= image_size - off` and
`count <= UINT64_MAX - start` after first validating the subtraction operands.
The canonical descriptor table directory uses these checked calculations:

```text
group_desc_off  = 128
shard_desc_off  = align_up(group_desc_off + group_count * 96, 8)
shard_capacity  = shard_count + 2 * (3 - replica_count)
replica_desc_off = align_up(shard_desc_off + shard_capacity * 96, 8)
descriptor_bytes = align_up(replica_desc_off + 3 * 32, block_size)
```

The two reserved shard-capacity entries per absent midpoint are the possible
midpoint `layout_descriptor` and `superblock_checkpoint` records.  The replica
area similarly reserves three entries.  Only `shard_count` and `replica_count`
entries are live; unused capacity and all bytes between/after tables are zero.
Consequently the descriptor length and primary/tail offsets are identical for
the 2- and 3-replica forms of otherwise identical geometry, removing a
candidate-count sizing cycle.

Tables are 8-byte aligned, non-overlapping, and fully contained in the
block-aligned `descriptor_bytes`, which is no larger than 16 MiB.
`group_count`, `shard_count`, and `replica_count` are non-zero.  `generation` is non-zero,
`image_size_bytes` equals the detected backing-image length, and `block_size`
matches the locator and supported primary identity.  Reserved fields must be
zero.

## Descriptor Replicas

The v7 descriptor uses `K7LD` magic and accepted descriptor version 2.  It owns:

- descriptor header, generation, image size, block size, feature flags, and CRC;
- metadata group table;
- shard table;
- descriptor replica table.

Replica policy:

- descriptor version 2 permits exactly 2 or 3 replicas;
- replica id equals its replica-table index; ids and roles are exactly
  `0=primary`, `1=tail`, and, when present, `2=midpoint`;
- primary and tail backup replicas are mandatory and occupy table entries 0
  and 1;
- a midpoint backup is mandatory when its descriptor and paired checkpoint
  block can be placed without overlap and without consuming any required
  metadata-group span or the final usable data block;
- there is no one-replica exception for test images;
- readers validate every deterministic candidate independently;
- the selected descriptor is the highest-generation valid descriptor;
- if generations tie, byte-identical `descriptor_bytes` spans are equivalent
  and the lower replica id wins;
- divergent descriptors at the selected generation are reported by `fsck.kafs`
  and must fail runtime admission until a repair policy exists;
- if no valid descriptor remains, `fsck.kafs`, `kafsdump`, and `kafs-v7` fail
  closed.

Offline tools may select and report a single surviving valid descriptor as
degraded.  Future read-only inspection may do the same after its mount gate is
implemented, but controlled-write admission requires at least two byte-identical
valid descriptors at the selected generation.

Descriptor replicas are descriptor-described through `layout_descriptor` shard
records so `kafsdump` can report them without hidden geometry.  Those shards
must exact-cover logical replica ordinals `[0, replica_count)`: each has
`logical_start == replica_id`, `logical_count == 1`,
`storage_class=byte_span`, `record_bytes == 0`, `header_bytes == 0`, and an
offset/length exactly matching the corresponding replica-table record.  The
replica-table `descriptor_bytes` values and every replica's parsed
  `descriptor_bytes` must match both byte-identical locators when both are
  valid, or the sole surviving valid locator otherwise.

Candidate offsets for descriptor version 2:

| Candidate | Offset rule |
| --- | --- |
| primary | exactly `block_size`; `va_primary_desc_off` must contain this value. |
| tail backup | exactly `image_size_bytes - block_size - descriptor_bytes`, immediately before the reserved final root block. |
| midpoint backup | exactly `align_down(image_size_bytes / 2, block_size)`. |

The primary checkpoint block starts exactly at
`block_size + descriptor_bytes`.  The tail checkpoint block starts exactly at
`tail_descriptor_off - block_size`.  A midpoint descriptor, when present, is
followed immediately by its checkpoint block.  The midpoint pair is present if
and only if that exact descriptor-plus-checkpoint span is in range and does not
overlap either root block, a primary/tail recovery span, or any group metadata
or data span.  Otherwise `candidate_count` and `replica_count` are 2 and no
midpoint table/shard records may appear.  A parser does not search alternative
midpoint offsets.

The descriptor byte length is block-aligned.  Underflow, an offset outside the
image, a partial descriptor/checkpoint span, a count inconsistent with the
deterministic offsets, or any overlap is fatal rather than a reason to search
for another candidate.

Normal v7 runtime does not rewrite layout descriptors.  Geometry changes use an
offline rebuild/migration.  Mkfs and an explicit offline descriptor repair write
non-primary replicas first and the primary replica last, flush each copy, and
must establish at least two byte-identical valid replicas before the image can
be admitted.  A semantic descriptor generation change must also publish matching
checkpoint replicas before any old layout-dependent journal can be reclaimed.
Readers do not trust write order; they select by independent CRC, bounds, and
generation checks.

### Physical ownership and overlap

The following physical resources are all range-checked before admission: the
primary root block `[0, block_size)`, the final root block, every `K7LD`
replica, every `K7CP` block, every group metadata container, every group data
span, and every shard payload.  Distinct resources must not overlap.

The sole containment exception is that a group-local shard is wholly contained
by its owning group's metadata container.  Such owned shard payloads are still
pairwise non-overlapping and exact-cover that container without physical gaps.
A group metadata container never contains another group, a data span, a global
recovery shard, or either root block.  Global
`layout_descriptor` and `superblock_checkpoint` shard ranges equal their
corresponding recovery-resource ranges; this equality describes the same
resource and is not a second allocation.  Any other overlap, alias, partial
containment, or duplicate physical ownership is fatal.

## Mutable Checkpoint Replicas

The primary superblock is an identity and discovery record, not mutable v7
checkpoint authority.  Successful v7 paths must not use `s_blkcnt_free`,
`s_inocnt_free`, `s_checkpoint_seq`, `s_commit_seq`, mount counters, or primary
superblock timestamps as authoritative mutable state.  Version 2 mkfs initializes
those legacy mutable fields to zero, and normal v7 operation does not rewrite
offset 0.

The selected `K7CP` record is the authoritative durable checkpoint baseline.
The block bitmap and inode shards remain allocation truth, but committed journal
transactions newer than `checkpoint_seq` may advance the recovered state beyond
the raw checkpoint.  `checkpoint_seq` is the largest globally finalized
transaction sequence whose committed mutations are durable in metadata;
aborted transactions through that sequence are accounted for as well.
For committed transactions newer than the checkpoint, recovery adds their
signed, checked `free_blocks_delta` and `free_inodes_delta` values to the K7CP
baseline; aborted transaction deltas are ignored.  After replay or simulation,
`fsck.kafs` recomputes free counts from the recovered bitmap and canonical-free
inode records and requires them to equal those derived values.  Signed
overflow, a result outside the filesystem bounds, or a mismatch is fatal unless
an explicit offline repair is requested.  A raw direct comparison with the
K7CP values is valid only when there is no newer committed journal transaction;
the first offline implementation slice uses that sequence-zero case.

The packed 64-byte checkpoint record is:

```text
+0   u32  magic = K7CP              +4   u16  version = 1
+6   u16  record_bytes = 64         +8   u32  flags = 0
+12  u32  reserved0 = 0             +16  u64  generation
+24  u64  descriptor_generation     +32  u64  checkpoint_seq
+40  u64  free_blocks               +48  u64  free_inodes
+56  u32  crc32                     +60  u32  reserved1 = 0
```

`generation` is non-zero.  `free_blocks <= s_r_blkcnt` and
`free_inodes <= s_inocnt - 2` after first requiring `s_inocnt >= 2`; values
outside those bounds are fatal before any journal simulation.

Checkpoint replica policy:

- the checkpoint replica count equals the descriptor replica count, so every
  valid image has 2 or 3 independently readable checkpoints;
- every checkpoint occupies a separate block-aligned, non-overlapping
  `superblock_checkpoint` shard with `storage_class=fixed_record`,
  `record_bytes=64`, `header_bytes=0`, `physical_bytes=block_size`, and one
  logical replica ordinal; these shards exact-cover `[0, replica_count)` with
  `logical_start` equal to the corresponding descriptor replica id and
  `logical_count=1`; bytes after the record in that block are zero;
- checkpoint and layout-descriptor shards use `group_id == UINT32_MAX`; their
  primary, optional midpoint, and tail neighborhoods distribute recovery writes
  without pretending that they belong to a metadata group;
- the primary checkpoint is the first free block after the primary descriptor,
  the tail checkpoint is the block immediately before the tail descriptor, and
  an optional midpoint checkpoint is the block immediately after the midpoint
  descriptor; there is no alternate-before placement;
- checkpoint physical ranges must not overlap descriptor replicas, metadata,
  data, or one another;
- `crc32` covers all 64 bytes with the CRC field zeroed;
- after independent CRC/bounds validation, only checkpoints whose
  `descriptor_generation` matches the selected `K7LD` are eligible; other
  readable generations are reported as stale and never outrank an eligible
  record;
- readers select the highest-generation eligible checkpoint; matching records at
  the selected generation are equivalent only when all 64 bytes are
  byte-identical, and the lowest logical replica ordinal wins;
- non-byte-identical records at the same selected generation are divergent even
  if their CRCs collide and fail runtime admission; if no valid checkpoint
  remains, runtime admission fails closed.

Offline tools may report a single surviving valid checkpoint as degraded, but
controlled-write admission requires at least two byte-identical valid
checkpoints at the selected generation and matching selected descriptor
generation.

After the journal and metadata represented by a checkpoint are durable, the
writer publishes generation `N+1` as follows:

1. choose the first target cyclically from `(N + 1) % replica_count`; when
   another target exists, skip the currently selected replica so one old copy
   remains untouched, then write and flush the target;
2. while retaining the journal required by generation `N`, write and flush
   the next cyclic distinct target with a byte-identical record at the same
   generation; with three replicas, keep the old selected copy untouched until
   two new copies exist;
3. verify that at least two replicas contain the same byte-identical valid
   `N+1` record;
4. only then reclaim or reset journal transactions at or below the new
   `checkpoint_seq`; an optional third replica may be refreshed afterward.

The last selected valid record is not overwritten first when another target is
available.  With two replicas both blocks necessarily change each publication;
with three, the two-copy target pair rotates and the third copy normally remains
as the older fallback until a later publication selects it.  An optional third
refresh is permitted only after step 4 and is not the wear-oriented default.  A
generation of `UINT64_MAX` cannot wrap and fails with overflow.  A
crash before step 3 retains the unreclaimed journal and at least one flushed
valid old-or-new checkpoint; with only two replicas it does not necessarily
retain the older generation.  Controlled-write admission remains denied until
two byte-identical replicas of one valid generation have been restored.  A
completed publication has two independently readable new checkpoint copies.

## Metadata Groups

Each metadata group is an explicit v7-owned record.  The v7 group descriptor
must make data mapping explicit, not infer it from v5 prefix geometry.

Required group fields:

| Field | Meaning |
| --- | --- |
| `group_id` | zero-based group id. |
| `flags` | zero for descriptor version 2; unknown non-zero flags are incompatible. |
| `metadata_physical_off` / `metadata_physical_bytes` | block-aligned metadata span owned by this group. |
| `data_logical_start` / `data_logical_count` | logical filesystem block range owned by this group. |
| `data_physical_off` / `data_physical_bytes` | block-aligned physical data span for that logical range. |
| `first_shard_index` / `shard_count` | shard table range owned by this group. |
| `generation_floor` | zero in descriptor version 2. |

`group_id` must equal the group-table index.  `metadata_physical_bytes`,
`data_logical_count`, `data_physical_bytes`, and `shard_count` are non-zero;
`flags`, `generation_floor`, and all reserved fields are zero in descriptor
version 2.  Group shard-table slices are disjoint and together contain every
group-local shard exactly once.  Global recovery shards are outside all slices.

Data block mapping is group-local and linear:

```text
physical_off = group.data_physical_off
             + (logical_block - group.data_logical_start) * block_size
```

Exactly one block bitmap shard and one paired allocator-summary shard belong to
each group, and both cover the same logical data range as the group.
`fsck.kafs` must reject gaps, overlaps, physical span overflow, or metadata/data
physical overlap.

Across all groups, data logical ranges exactly cover `[0, s_r_blkcnt)` without
gaps or overlap.  `data_physical_bytes` must equal the checked product of
`data_logical_count * block_size`; lookup must also check the logical delta
multiplication before adding `data_physical_off`.  Metadata and data physical
ranges are block-aligned and mutually non-overlapping.  Each group-local shard
appears in exactly one group's `first_shard_index` / `shard_count` range; global
recovery shards appear in no group range.

## Shard Records

Every mutable or reportable metadata region is represented by a shard record.
The v7 shard descriptor includes:

| Field | Meaning |
| --- | --- |
| `type` | metadata region id, or `layout_descriptor`; `unknown` is not valid on disk. |
| `flags` | zero for descriptor version 2 unless the type-specific spec defines it. |
| `group_id` | owning metadata group. |
| `storage_class` | fixed record, bit packed, allocator summary, or byte span. |
| `physical_off` / `physical_bytes` | image byte range. |
| `logical_start` / `logical_count` | type-specific logical coverage. |
| `record_bytes` | fixed record size, or 0 for byte-span records. |
| `header_bytes` | shard-local header size; zero for every version 2 type below. |
| `generation_floor` | zero in descriptor version 2; generated records validate their own generation. |
| `mapping_seed` | zero for descriptor version 2 linear mapping. |

Common validation:

- physical ranges are block-aligned; the primary 256-byte superblock and fixed
  final root block are discovery roots, not checkpoint shards;
- group-local physical ranges are inside the owning metadata group;
- `layout_descriptor` and `superblock_checkpoint` are global recovery shards and
  may be outside metadata groups only when `group_id == UINT32_MAX`;
- all shard physical ranges obey the physical ownership and overlap rules;
- logical ranges for each required type have no gaps and no overlaps;
- fixed-record shards have the expected `record_bytes`;
- unknown type values or unknown non-zero flags are incompatible.

Every shard has non-zero `physical_bytes` and `logical_count`.  In descriptor
version 2, `flags`, `header_bytes`, `generation_floor`, `mapping_seed`, and all
reserved fields are zero.  The type-specific rules below determine
`record_bytes` and the exact checked physical payload size.

Type and storage-class combinations are fixed for descriptor version 2:

| Type id | Type | Required storage class |
| ---: | --- | --- |
| 0 | `superblock_checkpoint` | `fixed_record` |
| 1 | `block_bitmap` | `bit_packed` |
| 2 | `inode_table` | `fixed_record` |
| 3 | `allocator_summary` | `allocator_summary` |
| 4 | `hrl_index` | `fixed_record` |
| 5 | `hrl_entries` | `fixed_record` |
| 6 | `journal_header` | `fixed_record` |
| 7 | `journal_data` | `byte_span` |
| 8 | `pending_log` | unsupported and fail-closed in version 2 |
| 9 | `tail_metadata` | unsupported and fail-closed in version 2 |
| 10 | `unknown` | invalid on disk |
| 11 | `layout_descriptor` | `byte_span` |

These numeric type ids are deliberately adopted for diagnostic continuity, but
the v7 enums and records are v7-owned.  Type ids 8 and 9 reserve names only: a
version 2 descriptor containing either shard is fatal.  Type id 10 and all
unknown ids are also fatal.  A type/class mismatch is fatal.  For a fixed-record
shard, `record_bytes` is non-zero and `physical_bytes` equals the block-aligned
checked value of `logical_count * record_bytes`; block padding is zero unless a
type-specific rule says otherwise.  `layout_descriptor` byte spans have
`record_bytes=0` and the exact replica length.  Journal data spans also have
`record_bytes=0`, divide evenly by `logical_count`, and give each logical segment
a block-aligned non-zero byte span.

## Type-Specific Record Contract

The descriptor is not allowed to inherit a host C type.  The following byte
shapes, sizes, and meanings are the complete version 2 contract.

### Block bitmap

A `block_bitmap` shard has `storage_class=bit_packed`, `record_bytes=8`, and
`header_bytes=0`.  Payload words are little-endian `u64`.  Bit 0 of a word maps
the lowest logical block in that word; `0=free` and `1=allocated or unavailable`.
The payload size for `n=logical_count` is `ceil(n / 64) * 8`, and
`physical_bytes=align_up(payload_size, block_size)`.

Bitmap shards exact-cover `[0, s_r_blkcnt)`.  Every `logical_start` is a
multiple of 64, so every internal shard/group boundary is word-aligned.  Bits
past the final valid logical block and every physical padding byte are one.  A
bitmap word is the atomic journal target; byte-granular host bitmap types are
not part of the v7 wire contract.

### Inode records and block references

An `inode_table` shard has `storage_class=fixed_record`, `record_bytes=128`, and
`header_bytes=0`.  Its v7-owned inode record is:

```text
+0   u16  mode                      +2   u16  uid
+4   u64  size                      +12  u64  atime
+20  u64  ctime                     +28  u64  mtime
+36  u64  dtime                     +44  u16  gid
+46  u16  link_count                +48  u32  blocks
+52  u16  rdev                      +54  u8   inline_or_block_refs[60]
+114 u8   disabled_tail_bytes[14] = all zero
```

All scalar fields are little-endian.  For `size <= 60`, bytes 54--113 contain
exactly `size` inline data bytes followed by zeros, and `blocks == 0`.  For
`size > 60`, those bytes are fifteen little-endian u32 block references: zero
means no reference and value `N+1` names v7 logical data block `N`.  Slots
0--11 are direct references, slots 12--14 are single-, double-, and
triple-indirect roots.  An indirect block is exactly `block_size / 4`
little-endian u32 plus-one references.  Therefore `s_r_blkcnt` must not exceed
`UINT32_MAX`.  The 14 tail bytes are retained only to make the record 128 bytes;
tail metadata is absent, and any non-zero tail byte is fatal for descriptor
version 2.

Inode shards exact-cover `[0, s_inocnt)`, where `s_inocnt >= 2`.  An inode is
allocated exactly when `mode != 0`.  A free inode is the canonical all-zero
128-byte record; `mode == 0` with any other non-zero byte is corruption.  Inode
0 is reserved, remains canonical zero, is never allocatable, and is excluded
from `free_inodes`.  Inode 1 is the allocated root.  `K7CP.free_inodes` is the
number of canonical-zero records in `[2, s_inocnt)`.  Checked fixed-record
sizing and block padding follow the common shard rule.

### Allocator summary

An `allocator_summary` shard has `storage_class=allocator_summary`,
`record_bytes=0`, and `header_bytes=0`, and pairs one-to-one with a bitmap shard
having identical `group_id`, `logical_start`, and `logical_count`.  For
`n=logical_count`:

```text
l0_bytes = ceil(n / 8)          # authoritative bytes derived from the bitmap
l1_bytes = ceil(l0_bytes / 8)
l2_bytes = ceil(l1_bytes / 8)
payload  = L1 || L2
physical_bytes = align_up(l1_bytes + l2_bytes, block_size)
```

L1 bit `i` is one exactly when authoritative L0 bitmap byte `i` contains at
least one free valid bit.  L2 bit `j` is one exactly when L1 byte `j` is
non-zero.  Both arrays use LSB-first indexing: logical bit `k` is
`array[k / 8] & (1u << (k % 8))`.  Unused L1/L2 bits and physical padding are zero.  The bitmap remains
authoritative: a mismatch fails runtime admission, while explicit offline fsck
repair may rebuild L1/L2 from it.

### HRL index and entries

An `hrl_index` shard has `storage_class=fixed_record`, `record_bytes=4`, and
`header_bytes=0`.  Each little-endian u32 is `entry_id_plus1`: zero terminates a
chain, and `N+1` names HRL entry id `N`.

An `hrl_entries` shard has `storage_class=fixed_record`, `record_bytes=24`, and
`header_bytes=0`.  Its v7-owned entry is:

```text
+0   u32  ref_count                +4   u32  next_entry_id_plus1
+8   u32  logical_block_plus1      +12  u32  reserved = 0
+16  u64  fast_hash
```

All fields are little-endian.  HRL bucket and entry-id ranges are each exact
covered from zero without gaps or overlaps.  The resulting bucket count is a
non-zero power of two no greater than `2^32`, and the total entry count is no
greater than `UINT32_MAX` so every `entry_id_plus1` is representable.
`fast_hash` is FNV-1a 64 over the complete referenced logical data block:
initialize to `14695981039346656037`, then for each byte XOR and multiply modulo
`2^64` by `1099511628211`.  Its bucket is
`fast_hash & (bucket_count - 1)`.  A free
entry has `ref_count == 0` and all other fields zero.  Every non-zero head/next
value is in range, every live entry is reachable exactly once from its computed
bucket, every chain terminates without a loop, and a live entry names an
in-range logical data block.  A bucket, every entry in its chain, and every
referenced logical data block must resolve to the same metadata/data group.
Unreadable records, out-of-range ids, loops, multiply reachable entries, hash
or content mismatch, or cross-group chains fail fsck and runtime admission.

Cross-group HRL chains remain a deferred capability so the initial layout keeps
damage containment and fsck recovery deterministic.  Supporting them later
requires a new incompatible flag, an explicit bucket-to-entry/data mapping
rule, and matching fsck/recovery tests.  Descriptor version 2 readers must not
infer or silently accept that behavior.

### Pending and tail metadata

`pending_log` and `tail_metadata` shards are absent in descriptor version 2.
Their type ids are reserved only to produce a specific diagnostic; the presence
of either shard is fatal.  Any pending worker, tail packing/normalization,
reclaim, or tombstone-GC path fails before mutation.

## Journal Layout And Transaction Records

Journal segments are descriptor-owned.  `segment_count` is at least
`max(2, group_count)`, segment ids exact-cover `[0, segment_count)`, and every
group owns at least one segment.  Each id resolves to exactly one
`journal_header` logical record and one `journal_data` byte-span segment.  The
header and data for an id belong to the same metadata group.  There is no
dedicated global journal group.

A `journal_header` shard has `storage_class=fixed_record`,
`record_bytes=block_size`, and `header_bytes=0`.  Each logical record is one
block containing `block_size / 64` rotating `K7JH` slots.  A slot is:

```text
+0   u32  magic = K7JH             +4   u16  version = 1
+6   u16  flags = 0                 +8   u32  segment_id
+12  u32  slot_bytes = 64           +16  u64  generation
+24  u64  data_bytes                +32  u64  write_bytes
+40  u64  first_sequence            +48  u64  last_sequence
+56  u32  crc32                     +60  u32  reserved = 0
```

`data_bytes` equals the paired segment span and `write_bytes <= data_bytes`.
`segment_id` equals the logical segment id resolved through both header and data
shards.  `version`, `slot_bytes`, `flags`, and `reserved` must have the exact
values above.  Every valid slot has non-zero `generation`.  An empty segment has
`write_bytes == first_sequence == last_sequence == 0`.  A non-empty segment has
8-byte-aligned non-zero `write_bytes`, ending exactly after the terminal record
padding of a complete transaction, and non-zero
`first_sequence <= last_sequence`.  Those sequence fields equal the actual
minimum and maximum complete transaction sequences in the described prefix.
An all-zero unused slot is invalid.  To publish a new prefix, the writer flushes
journal data first, writes the next slot, and
flushes it before applying the logged metadata.  Valid generation 1 uses slot
0, and generation `G` uses
`(G - 1) % (block_size / 64)`, so slots rotate deterministically.
Readers first validate only slot header fields and CRCs, then select the highest
generation among those valid headers.  Invalid/torn headers are ignored, so an
unpublished newer slot may fall back to the previous valid header.
Non-byte-identical valid headers at the selected generation make that segment
divergent and fatal.  After selection, readers validate the complete prefix
described by that one header.  An invalid selected prefix is evidence of lost
durable journal data and is fatal; it is not downgraded.  Lower-generation
prefixes are stale and are not validated because reset/reuse may already have
overwritten their data.  If a required segment has no valid selected
header/prefix pair, recovery fails closed.

A `journal_data` shard has `storage_class=byte_span`, `record_bytes=0`, and
`header_bytes=0`.  Its physical range divides into equal, block-aligned segment
spans.  Records begin at the segment start and are append-only through the
selected header's `write_bytes`.  Each `K7JR` record has a packed 20-byte header:

```text
+0   u32  tag                       +4   u32  payload_bytes
+8   u64  sequence                  +16  u32  crc32
+20  u8   payload[payload_bytes]
       u8 zero_padding[align_up(20 + payload_bytes, 8) - (20 + payload_bytes)]
```

The only tags are the following mnemonic values after little-endian decode:

| Tag | little-endian u32 | Payload |
| --- | ---: | --- |
| `K7JB` begin | `0x4b374a42` | 32-byte transaction control |
| `K7JM` mutation | `0x4b374a4d` | 56-byte mutation header plus patch bytes |
| `K7JC` commit | `0x4b374a43` | 32-byte transaction control |
| `K7JA` abort | `0x4b374a41` | 32-byte transaction control |

Unknown tags, zero sequence numbers, non-zero padding, record overflow, or a
record crossing the selected prefix are fatal.  There is no v5 `WRAP` record:
a transaction that cannot fit is placed wholly in another empty/reclaimable
segment or fails before mutation.

The begin/commit/abort transaction-control payload is:

```text
+0   u32  group_id                  +4   u32  mutation_count
+8   u32  mutation_payload_bytes    +12  u32  mutation_stream_crc32
+16  i64  free_blocks_delta         +24  i64  free_inodes_delta
```

The mutation payload is:

```text
+0   u16  target_type               +2   u16  flags = 0
+4   u32  group_id                  +8   u64  logical_index
+16  u32  target_bytes              +20  u32  patch_off
+24  u32  patch_bytes               +28  u32  before_crc32
+32  u32  after_crc32               +36  u32  reserved = 0
+40  i64  free_blocks_delta         +48  i64  free_inodes_delta
+56  u8   after_image_patch[patch_bytes]
```

`target_type` is one of block bitmap, inode, allocator summary, HRL index, or
HRL entries.  Target identity is canonical: bitmap uses the word's
64-block-aligned first logical block, inode uses the inode number, allocator
summary uses its paired bitmap shard's `logical_start`, HRL index uses the
bucket id, and HRL entry uses the entry id.  The descriptor resolves that value
to exactly one target in the stated group.  Target units are respectively one
8-byte bitmap word, one 128-byte inode, one complete unpadded L1/L2 summary
payload, one 4-byte bucket, or one 24-byte HRL entry.  Aliased target identities
are fatal.  `target_bytes` equals that unit's exact size,
`patch_bytes` is non-zero, and `patch_off + patch_bytes <= target_bytes` is
checked without overflow.  `before_crc32` and `after_crc32` cover the complete
target unit before and after applying the patch.  For every target unit,
recovery builds the ordered committed-mutation chain and requires each
mutation's `before_crc32` to equal the preceding committed mutation's
`after_crc32`.  Aborted mutations are excluded.  The current target must match
one state in that chain (the initial before state or any after state); recovery
resumes after that state and verifies every resulting
`after_crc32`.  A third state or a broken chain is fatal.  This permits
idempotent recovery when a crash leaves different targets at different durable
points without treating arbitrary corruption as an already-applied update.

Mutation deltas are little-endian two's-complement i64 values and mean
`free_after - free_before`.  Only a bitmap mutation may have non-zero
`free_blocks_delta`, and only an inode mutation may have non-zero
`free_inodes_delta`; all other delta fields are zero.  A bitmap delta is within
`[-64, 64]`, and an inode delta is within `[-1, 1]`.  The two transaction-control
deltas equal the checked sums of all mutation deltas.  Recovery applies those
totals exactly once for a committed transaction, regardless of how many target
after-images were already durable at crash time, and never applies them for an
aborted transaction.

All records in a transaction are contiguous in one segment and share one
sequence.  A complete transaction is one begin, exactly `mutation_count`
mutation records, and one terminal commit or abort; `mutation_count` is
non-zero.  Control payloads must be byte-identical.  The control `group_id`,
every mutation `group_id`, and the owning group of both journal shards must all
match.  `mutation_payload_bytes` is the checked sum of the mutation
payload lengths, must fit u32, and `mutation_stream_crc32` covers their
concatenation in record order.  A committed transaction's complete journal bytes are flushed
and exposed by a durable header slot before any target is changed.  An aborted
transaction changes no target.

Complete transaction sequences are strictly increasing within a segment; a
sequence cannot appear twice in one prefix.  Duplicate copies are permitted
only across segments and are reconciled by the global byte-identical rule below.
Bytes beyond the selected `write_bytes` are stale/unused and never influence
minimum/maximum sequence or replay.

Physical placement and header generations are group-local; transaction
sequences and replay are filesystem-global.  Starting from selected
`K7CP.checkpoint_seq`, recovery gathers every complete transaction from every
valid segment, deduplicates byte-identical copies of the same sequence, rejects
different copies or commit/abort disagreement, and replays committed
transactions in ascending sequence.  Every sequence from
`checkpoint_seq + 1` through the largest visible sequence must be represented
by exactly one committed or aborted transaction after deduplication.  A gap is
recovery ambiguity and fails closed.  A sequence is not consumed until its
complete transaction is exposed by a valid header, so a crash before that point
does not create a legitimate gap.

Writers serialize global sequence publication across groups.  A later
transaction may prepare unnumbered private payload, but sequence `N+1` is not
allocated and no `K7JH` update may expose it until the commit-or-abort
transaction for `N` is already exposed by a flushed valid header slot.  Thus
a normal writer crash cannot make a later group-visible sequence while leaving
an earlier sequence absent; the recovery gap rule diagnoses media loss or a
non-conforming writer rather than ordinary scheduling.

A segment is reset only after two byte-identical checkpoints have
`checkpoint_seq >= last_sequence`.  Reset increments its header generation and
writes/flushes an empty selected prefix before any byte at the data-segment
start is reused.  Lower-generation headers then remain only as stale slot
history; neither header generation nor transaction sequence may wrap.
Until a separate multi-group transaction protocol is accepted, every
transaction has one group and an operation needing more than one group fails
before its begin record.  Descriptor version 2 never reads, writes, replays, or
resets the v5 prefix journal.

## Data Block Address Space

The v7 descriptor owns logical-to-physical data mapping.  `s_first_data_block`
is not the authoritative source for successful v7 paths.

Descriptor version 2 uses group-local linear mapping:

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

Descriptor version 2 requires these incompatibility bits:

| Bit | Meaning |
| ---: | --- |
| 0 | v7-owned grouped descriptor layout |
| 1 | descriptor-owned logical-to-physical data mapping |
| 2 | descriptor-owned journal segments |
| 3 | replicated authoritative `K7CP` checkpoints |
| 4 | group-local journal placement with filesystem-global replay order |
| 5 | mandatory HRL bucket/entry group consistency |

All six bits must be present.  Missing required bits or unknown incompatibility
bits are fatal.  `feature_flags`, `ro_compat_flags`, and `mapping_seed` are zero,
and `mapping_policy` is `0=group_local_linear`, for descriptor version 2.
Report-only flags may be added later only when older tools fail closed or ignore
them safely.  Unknown mapping policies or a non-zero mapping seed are fatal.

## Small-Image Policy

`mkfs.kafs --format-version 7` must reject an image that cannot fit:

- primary superblock and anchor;
- the independently reserved final root block and tail `K7SA` locator;
- at least two descriptor replicas;
- the matching two checkpoint replicas;
- one metadata group;
- checkpoint, bitmap, inode, allocator, HRL, and at least two journal
  header/data segments;
- at least one usable data block after metadata reservations.

Small images may use `group_count == 1`, but they must not fall back to v5
prefix metadata.  There is no valid one-replica test-image exception.  Tests use
sparse files for valid small images and direct malformed parser fixtures for the
one-replica rejection case.

## First Implementation Slice

The first implementation delivery is candidate D as a strict subset of
candidate A:

- emit accepted `K7LD` descriptor version 2 with `group_count == 1`;
- produce at least primary and tail `K7LD` descriptor replicas;
- produce matching rotated `K7CP` checkpoint replicas;
- produce at least two empty group-local journal segments with valid rotating
  `K7JH` headers and global sequence zero;
- keep all metadata access descriptor-owned;
- support `fsck.kafs` and `kafsdump` validation/reporting before runtime
  admission;
- keep pending log and tail metadata disabled/fail-closed;
- use the v7-owned byte shapes above rather than host or v5/v6 wire structs.

### Canonical single-group physical planner

The first builder uses one deterministic placement, so replica count cannot be
changed by an implementation's choice of group order:

1. Compute `descriptor_bytes` with the three-replica capacity formula above,
   even when the eventual count is two.  Derive the primary/tail descriptor and
   checkpoint spans from that fixed length.
2. Set group 0 metadata start to the block immediately after the primary
   checkpoint.  Place exactly one shard of each group-local type in this order,
   with every start at the previous block-aligned end:
   `block_bitmap`, `inode_table`, `allocator_summary`, `hrl_index`,
   `hrl_entries`, `journal_header`, `journal_data`.  The journal shards cover
   exactly two equal data segments.  The group metadata container is exactly
   the concatenation of these seven physical shard spans.  These are shard-table
   indices 0--6 and group 0 records `first_shard_index=0, shard_count=7`.
   Global recovery shard records follow as descriptor/checkpoint pairs in
   replica-id order: primary, tail, then optional midpoint.
3. Given the requested inode count, HRL bucket/entry counts, and block-aligned
   journal segment bytes, evaluate the exact type-specific size formulas for
   each candidate data-block count `n`.  Choose the largest `n >= 1` for which
   the metadata container followed immediately by `n * block_size` data bytes
   ends no later than the tail checkpoint.  Checked arithmetic is required;
   if no `n` fits, mkfs rejects the image.
4. Set group 0 data start to the metadata-container end and its logical range
   to `[0, n)`.  Any whole-block slack between the data end and tail checkpoint
   is zero and unowned; it is not silently added to a shard or group.
5. Test the fixed midpoint descriptor-plus-checkpoint span against the completed
   two-replica geometry.  If it lies wholly in unowned slack, add its two shard
   records and replica record, then set locator candidate count, descriptor
   replica count, and derived checkpoint replica count to three.  Because table
   capacity was reserved, offsets and `descriptor_bytes` do not change.  If it
   overlaps group metadata/data or another recovery span, retain two replicas;
   no data block is removed merely to create a midpoint copy.

The v7 parser/validator still accepts a correctly described three-replica image
created by a future planner or a test fixture.

The initial single-group delivery proved the offline
`mkfs -> kafsdump -> fsck` round trip, descriptor/checkpoint fallback, and
deterministic rejection of corruption.  It did not claim that single-group
placement completed wear leveling; it provided the v7-owned placement and
recovery foundation extended by the multi-group planner below.
`kafs-v7 --inspection-mount` remains fail-closed for descriptor version 2 until
a following mount-smoke slice proves the accepted layout; controlled-write
admission remains later still.

## Canonical Multi-Group Physical Planner

The multi-group builder extends the same version 2 wire contract without
changing record sizes, mapping policy, or recovery selection:

1. Group counts are powers of two from 1 through 64 and must divide the 1024
   HRL buckets.  `mkfs.kafs --v7-group-count N` requests an exact supported
   count.  With no override, mkfs starts with the largest supported power of
   two no greater than one group per 64 MiB of image and halves the count until
   the complete checked geometry fits.  It never silently substitutes a
   different count for an explicit request.
2. The fixed-point planner reserves the primary and tail recovery spans first,
   then chooses the largest logical data-block count that fits all groups.  It
   requires at least one complete 64-bit bitmap word per group.  Complete
   64-block units are distributed by quotient and remainder; any final partial
   word belongs only to the last group.  Thus every internal logical group
   boundary is 64-block aligned and group data counts differ by at most 64
   blocks.
3. Inodes and HRL entries exact-cover their global namespaces by quotient and
   remainder, with lower group ids receiving the remainder.  HRL buckets
   exact-cover the fixed 1024-bucket namespace equally.  Inode 0 and the root
   inode remain in group 0.  All group-local HRL chains and referenced logical
   data blocks must remain in that same group; cross-group chains fail closed.
4. A multi-group image has one journal segment per group.  A single-group image
   retains two segments.  The requested total journal bytes are divided into
   equal, non-zero, block-aligned segment spans; header and data for each
   global segment id remain in its owning group.
5. Physical placement is canonical and interleaved in ascending group-id order:
   `[group N seven metadata shards][group N data]`.  This spreads bitmap,
   inode, allocator, HRL, and journal locations through the filesystem-owned
   placement arena instead of concentrating every mutable metadata shard at a
   common prefix.  Each group container is still exactly the concatenation of
   the seven shard spans in the single-group order.
6. Midpoint recovery is evaluated only after every group has its maximum-size
   data span.  It is emitted only if the fixed midpoint descriptor/checkpoint
   pair lies wholly in remaining zero slack.  The planner never removes a data
   block or changes a group boundary to manufacture that replica.

`kafsdump` reports `wear_distribution` with group count, first-to-last metadata
placement span, total filesystem placement arena, and the minimum/maximum
group data-block counts.  These values prove filesystem-level address
distribution only.  They do not claim knowledge of an SD card controller's
flash translation layer, erase-block mapping, or physical-media wear leveling.
`scripts/check-v7-wear-distribution.sh` fixes the current sparse-image proof at
512 MiB: eight groups, no more than 64 blocks of data-count skew, monotonically
distributed metadata starts, and metadata placement spanning at least 70% of
the filesystem placement arena.

Runtime inspection and write admission remain separate gates.  In particular,
this planner does not introduce multi-group transaction atomicity or cross-group
HRL behavior.

## Recovery Replica Fault-Matrix Proof

`v7_replica_fault_smoketest` fixes the offline recovery behavior on an
eight-group image.  The normal 512 MiB fixture uses the primary and tail
descriptor/checkpoint pairs emitted by mkfs.  The accepted three-copy fixture
extends that sparse image to 1 GiB without moving its group/data geometry, then
places the midpoint descriptor/checkpoint pair in the resulting zero slack.
The latter proves the accepted parser and recovery selector; it does not claim
that canonical mkfs geometry always has midpoint slack.

The placement gate requires the primary and tail descriptor span to cover at
least 95% of the image address range.  For three copies, the midpoint descriptor
starts exactly at half the image size.  Every checkpoint remains in its
specified adjacent block and all recovery ranges remain outside group metadata
and data.

The localized-loss matrix zeroes complete descriptor spans and checkpoint
blocks independently.  It proves:

- either primary or tail descriptor/checkpoint may be lost while the other
  pair is selected with degraded status;
- descriptor and checkpoint selection are independent, so their surviving
  replica ids may differ;
- a three-copy image remains offline-readable after loss of any one or any two
  recovery neighborhoods, but loss of all descriptors or all checkpoints is
  fatal;
- one or two independently valid higher-generation checkpoints are selected
  over stale copies;
- a higher-generation descriptor without a checkpoint tied to that descriptor
  generation fails closed, while a coordinated descriptor/checkpoint pair is
  offline-readable as degraded;
- non-byte-identical, independently shape-valid descriptors or structurally
  valid checkpoints at the selected generation fail with divergence rather
  than being resolved by majority.

The damage zones above describe filesystem byte ranges only.  They do not
model an SD controller's remapping, correlated internal failure, ECC behavior,
or physical erase-block boundaries.  A single surviving copy is sufficient for
offline diagnosis, but it does not satisfy the two-identical-checkpoint gate
required by future controlled-write admission.

## RC Media Qualification Boundary

Deterministically injecting a correlated FTL/ECC failure is not a normal
implementation or runtime-admission gate.  It is an explicit RC qualification
constraint and residual risk: the filesystem-offset replica proof above must
not be described as proof that copies occupy independent NAND erase blocks or
controller failure domains.

An RC that exposes format v7 remains an explicit `kafs-v7` opt-in and requires
independent review of recorded real-SD-card qualification evidence.  The
qualification plan must cover every enabled surface on the agreed
card/controller sample set: format, mount, unmount, remount, and fsck are the
baseline; an RC exposing controlled write additionally requires write, full
fsync, and controlled power-interruption cycles.  Release notes must state the
unproven physical-failure-domain boundary.  Stable/GA promotion must reassess
that residual risk rather than inheriting the RC qualification unchanged.

This RC boundary does not waive software correctness gates.  Descriptor and
checkpoint selection, logical-to-physical mapping, journal ordering/replay,
checkpoint publication, locking, mutation routing, crash recovery, and
fail-closed behavior remain blockers for the corresponding mount/write mode.
Controlled-write admission continues to require at least two byte-identical
valid descriptor and checkpoint copies at the selected generation.

## Accepted Decision Closeout

The five implementation-blocking questions were closed on 2026-07-13:

1. v7 uses the explicit 96-byte v7-owned group and shard records above;
   `storage_class` and both logical and physical ranges are wire fields before
   implementation begins.
2. replicated `K7CP` checkpoints, not mutable primary-superblock fields, are the
   durable counter authority.
3. journal header/data placement is group-local and replay ordering is
   filesystem-global; no global journal group is introduced.
4. HRL bucket/entry group consistency is mandatory.  Cross-group behavior is a
   deferred incompatible capability with its own mapping and recovery proof.
5. every valid image has primary and tail descriptor/checkpoint replicas, plus
   midpoint replicas whenever the deterministic non-overlap rule permits them.

The accepted layout also closes the discovery single-point failure by requiring
the immutable tail `K7SA` locator.  Any later relaxation must use an explicit
incompatible version or flag and preserve deterministic fsck recovery or
rejection.
