# KAFS format v6 metadata layout descriptor spec

最終更新: 2026-06-17

対象: `SDW-P3-T1 Format v6 descriptor spec`, `SDW-P3-T2 Descriptor replica policy`

## Status

この文書は format v6 の root metadata layout descriptor を固定するための仕様である。
この時点では runtime mountable な v6 を定義しない。`mkfs.kafs --format-version 6` は
offline-only scaffold image を作成し、read-only parser / `fsck.kafs` / `kafsdump` が
descriptor discovery と replica reporting を行う。

現行 v4/v5 image の mount / fsck / dump behavior は変更しない。

## Goals

- distributed metadata layout の root descriptor を一意に解釈できるようにする。
- descriptor bounds、group bounds、shard bounds を overflow なしに検証できるようにする。
- unsupported descriptor version / incompatible feature を明示的に拒否する。
- `fsck.kafs` が v6 image を read-only discovery できる順序を固定する。
- primary descriptor が壊れた場合でも backup descriptor 候補を deterministic に発見できる。

## Non-Goals

- v6 image の runtime mount admission は `SDW-P4` まで有効化しない。
- v5 image の in-place metadata relocation は扱わない。
- shard 内部フォーマットの全実装詳細は `SDW-P4` の各 shard ticket で固定する。
- stale/corrupt descriptor replica の repair write は `SDW-P3-T3` 以降で実装する。

## Byte Order And Units

- すべての multibyte field は little-endian。
- byte range は半開区間 `[offset, offset + size)`。
- block range は filesystem block number の半開区間 `[start_blo, start_blo + block_count)`。
- descriptor 内 table offset は descriptor 先頭からの byte offset。
- image 内 physical offset は image 先頭からの byte offset。
- parser はすべての `offset + size` と `count * entry_size` を overflow check してから使う。

## Superblock Anchor

v6 image は通常の `kafs_ssuperblock_t` を offset 0 に置き、`s_magic=KAFS_MAGIC`、
`s_format_version=6` とする。descriptor discovery 用に `s_reserved[0..31]` を
次の anchor として解釈する。

```c
#define KAFS_FORMAT_VERSION_V6 6u
#define KAFS_V6_SUPERBLOCK_ANCHOR_MAGIC 0x4B365341u /* 'K6SA' */
#define KAFS_V6_SUPERBLOCK_ANCHOR_VERSION 1u

struct kafs_sv6_superblock_anchor {
  kafs_su32_t va_magic;              /* +0  'K6SA' */
  uint16_t va_version;               /* +4  1 */
  uint16_t va_flags;                 /* +6  0 for v1 */
  kafs_su64_t va_primary_desc_off;   /* +8  primary descriptor offset */
  kafs_su32_t va_primary_desc_bytes; /* +16 primary descriptor byte length */
  kafs_su32_t va_candidate_count;    /* +20 deterministic descriptor candidates */
  kafs_su32_t va_anchor_crc32;       /* +24 CRC32 over this struct with this field zeroed */
  kafs_su32_t va_reserved0;          /* +28 must be zero */
} __attribute__((packed));
```

Anchor rules:

- `va_primary_desc_off` must be block-aligned and greater than or equal to one filesystem block.
- `va_primary_desc_bytes` must be non-zero, block-aligned, and no larger than
  `KAFS_V6_LAYOUT_MAX_BYTES` (16 MiB).
- `va_primary_desc_off + va_primary_desc_bytes` must be within the detected image size.
- `va_candidate_count` must be 2 or 3.
- Unknown non-zero `va_flags` are incompatible and must be rejected.

The superblock anchor deliberately stores only the primary offset and candidate count. Backup
locations are computed from image geometry so discovery does not depend on a readable primary
descriptor.

## Layout Descriptor Header

The primary descriptor starts at `va_primary_desc_off`.

```c
#define KAFS_V6_LAYOUT_MAGIC 0x4B364C44u /* 'K6LD' */
#define KAFS_V6_LAYOUT_VERSION 1u
#define KAFS_V6_LAYOUT_HEADER_BYTES 128u
#define KAFS_V6_LAYOUT_MAX_BYTES (16u * 1024u * 1024u)
#define KAFS_V6_REPLICA_DESC_BYTES 32u
#define KAFS_V6_LAYOUT_REPLICA_MIN_COUNT 2u
#define KAFS_V6_LAYOUT_REPLICA_MAX_COUNT 3u

struct kafs_sv6_layout_desc_header {
  kafs_su32_t ld_magic;             /* +0   'K6LD' */
  uint16_t ld_version;              /* +4   descriptor format version, 1 */
  uint16_t ld_header_bytes;         /* +6   128 */
  kafs_su32_t ld_descriptor_bytes;  /* +8   bytes covered by ld_descriptor_crc32 */
  kafs_su32_t ld_flags;             /* +12  0 for v1 */
  kafs_su64_t ld_generation;        /* +16  monotonically increasing descriptor generation */
  kafs_su64_t ld_image_size_bytes;  /* +24  expected logical image size */
  kafs_su32_t ld_block_size;        /* +32  filesystem block size in bytes */
  kafs_su32_t ld_group_count;       /* +36  number of group descriptors */
  kafs_su32_t ld_group_desc_off;    /* +40  group descriptor table offset */
  uint16_t ld_group_desc_bytes;     /* +44  64 for v1 */
  uint16_t ld_mapping_policy;       /* +46  deterministic mapping policy */
  kafs_su32_t ld_shard_count;       /* +48  number of shard descriptors */
  kafs_su32_t ld_shard_desc_off;    /* +52  shard descriptor table offset */
  uint16_t ld_shard_desc_bytes;     /* +56  96 for v1 */
  uint16_t ld_reserved0;            /* +58  must be zero */
  kafs_su32_t ld_replica_count;     /* +60  descriptor replica count */
  kafs_su32_t ld_replica_desc_off;  /* +64  replica descriptor table offset */
  uint16_t ld_replica_desc_bytes;   /* +68  32 for v1 */
  uint16_t ld_reserved1;            /* +70  must be zero */
  kafs_su64_t ld_feature_flags;     /* +72  advisory feature flags */
  kafs_su64_t ld_incompat_flags;    /* +80  unknown bits are fatal */
  kafs_su64_t ld_ro_compat_flags;   /* +88  unknown bits allow read-only discovery only */
  kafs_su64_t ld_mapping_seed;      /* +96  deterministic shard mapping seed */
  kafs_su32_t ld_descriptor_crc32;  /* +104 CRC32 over descriptor_bytes with this field zeroed */
  kafs_su32_t ld_reserved2;         /* +108 must be zero */
  kafs_su64_t ld_reserved3;         /* +112 must be zero */
  kafs_su64_t ld_reserved4;         /* +120 must be zero */
} __attribute__((packed));
```

Header validation:

- `ld_magic == KAFS_V6_LAYOUT_MAGIC`.
- `ld_version == KAFS_V6_LAYOUT_VERSION`; otherwise reject as unsupported.
- `ld_header_bytes == KAFS_V6_LAYOUT_HEADER_BYTES`.
- `ld_descriptor_bytes == va_primary_desc_bytes`.
- `ld_descriptor_bytes <= KAFS_V6_LAYOUT_MAX_BYTES`.
- `ld_descriptor_bytes` must be block-aligned.
- `ld_image_size_bytes` must equal the detected logical image size for regular files. For block
  devices, it must be less than or equal to the detected device size.
- `ld_block_size` must equal `1 << (10 + s_log_blksize)` and must divide all required block-aligned
  ranges.
- `ld_group_count > 0`.
- `ld_shard_count > 0`.
- `ld_replica_count == va_candidate_count`.
- `ld_replica_count` must be between `KAFS_V6_LAYOUT_REPLICA_MIN_COUNT` and
  `KAFS_V6_LAYOUT_REPLICA_MAX_COUNT`.
- `ld_replica_desc_bytes == KAFS_V6_REPLICA_DESC_BYTES`.
- The replica descriptor table must be fully inside `ld_descriptor_bytes`.
- Unknown non-zero `ld_flags` or `ld_incompat_flags` are fatal for mount, fsck, and dump.
- Unknown `ld_ro_compat_flags` allow `fsck.kafs` / `kafsdump` read-only reporting but prohibit
  repair and runtime mount.
- `ld_descriptor_crc32` must match CRC32 over exactly `ld_descriptor_bytes` bytes with the field
  zeroed.

`ld_mapping_policy` values:

| value | name | meaning |
| ---: | --- | --- |
| 0 | `group_local_range` | logical ranges are assigned to explicit shard descriptors; each shard belongs to one metadata group |

Unknown mapping policy values are incompatible.

## Descriptor Replica Policy

Descriptor replicas are full byte-for-byte copies of the layout descriptor. Each replica carries
the same `ld_generation`, tables, shard descriptors, replica descriptors, and
`ld_descriptor_crc32`.

The candidate set is deterministic:

| candidate | role | offset rule |
| ---: | --- | --- |
| 0 | `primary` | `va_primary_desc_off`; `SDW-P3-T3` scaffold must use exactly one block after the primary superblock |
| 1 | `tail_backup` | `align_down(detected_image_size - va_primary_desc_bytes, block_size)` |
| 2 | `mid_backup` | `align_down(detected_image_size / 2, block_size)`, present only when it does not overlap candidates 0 or 1 |

Candidate validation:

- `va_candidate_count == ld_replica_count`.
- Candidate 0 and 1 are mandatory.
- Candidate 2 is the default for images large enough to place it without overlap.
- Candidate physical ranges are `[candidate_off, candidate_off + ld_descriptor_bytes)`.
- Candidate ranges must not overlap each other.
- Candidate 0 must match `va_primary_desc_off`.
- Candidate 1 must match the tail formula.
- Candidate 2, when present, must match the midpoint formula.
- Once a candidate header is decoded, `ld_image_size_bytes`, `ld_descriptor_bytes`, and
  `ld_block_size` must match the values used to compute the deterministic candidate set.
- If candidate 1 would overlap candidate 0, the image is too small for v6.

Replica descriptor table:

```c
#define KAFS_V6_REPLICA_ROLE_PRIMARY 0u
#define KAFS_V6_REPLICA_ROLE_TAIL_BACKUP 1u
#define KAFS_V6_REPLICA_ROLE_MID_BACKUP 2u

struct kafs_sv6_replica_desc {
  kafs_su32_t rd_replica_id;       /* +0  candidate ordinal */
  uint16_t rd_role;                /* +4  KAFS_V6_REPLICA_ROLE_* */
  uint16_t rd_flags;               /* +6  0 for v1 */
  kafs_su64_t rd_physical_off;     /* +8  image byte offset */
  kafs_su32_t rd_descriptor_bytes; /* +16 descriptor bytes at this candidate */
  kafs_su32_t rd_reserved0;        /* +20 must be zero */
  kafs_su64_t rd_reserved1;        /* +24 must be zero */
} __attribute__((packed));
```

Replica descriptor validation:

- `rd_replica_id` must equal the table index.
- `rd_role` must match the deterministic role for that candidate.
- Unknown non-zero `rd_flags` are incompatible.
- `rd_physical_off` must match the deterministic candidate offset.
- `rd_descriptor_bytes == ld_descriptor_bytes`.
- Reserved fields must be zero.

Latest-valid selection:

1. Read every deterministic candidate from the superblock anchor.
2. A candidate is valid only if the descriptor header, descriptor CRC, table bounds, group
   descriptors, shard descriptors, and replica descriptors all validate.
3. Select the valid candidate with the highest `ld_generation`.
4. If multiple valid candidates have the same highest generation and the same
   `ld_descriptor_crc32`, select the lowest `rd_replica_id`.
5. If multiple valid candidates have the same highest generation but different
   `ld_descriptor_crc32`, report same-generation divergence and reject runtime mount / repair.
6. If no valid candidate remains, discovery fails.

Replica status terms:

| status | meaning |
| --- | --- |
| `selected` | valid candidate chosen by latest-valid selection |
| `valid` | valid candidate with the selected generation and matching CRC, but not selected |
| `stale` | valid candidate whose generation is lower than the selected generation |
| `corrupt` | candidate was readable but failed magic, CRC, table, group, or shard validation |
| `unsupported` | candidate has unsupported descriptor version or incompat flags |
| `missing` | candidate range is out of bounds or could not be read |
| `divergent` | valid candidate shares selected generation but has a different descriptor CRC |

Descriptor write ordering for future writers:

1. Build a complete new descriptor with `ld_generation = previous_generation + 1`.
2. Write backup candidates first, from highest candidate id down to 1.
3. Write candidate 0 primary last.
4. Update the superblock anchor only when descriptor size or candidate count changes; such updates
   are offline-only and must be followed by rewriting all descriptor replicas.

This order lets interrupted updates leave either an older primary plus newer backup or a newer
primary plus older backup. Readers always choose by generation and CRC instead of assuming primary
is newest.

## Group Descriptor

Group descriptors partition the image into metadata groups and nearby data ranges. The group table
starts at `ld_group_desc_off` and contains `ld_group_count` entries.

```c
#define KAFS_V6_GROUP_DESC_BYTES 64u

struct kafs_sv6_group_desc {
  kafs_su32_t gd_group_id;              /* +0  zero-based group id */
  kafs_su32_t gd_flags;                 /* +4  0 for v1 */
  kafs_su32_t gd_metadata_start_blo;    /* +8  group metadata block range start */
  kafs_su32_t gd_metadata_block_count;  /* +12 group metadata block count */
  kafs_su32_t gd_data_start_blo;        /* +16 group data block range start */
  kafs_su32_t gd_data_block_count;      /* +20 group data block count */
  kafs_su32_t gd_first_shard_index;     /* +24 first shard descriptor owned by this group */
  kafs_su32_t gd_shard_count;           /* +28 shard descriptor count owned by this group */
  kafs_su64_t gd_generation_floor;      /* +32 minimum generation of mutable group metadata */
  kafs_su64_t gd_reserved0;             /* +40 must be zero */
  kafs_su64_t gd_reserved1;             /* +48 must be zero */
  kafs_su64_t gd_reserved2;             /* +56 must be zero */
} __attribute__((packed));
```

Group validation:

- `ld_group_desc_bytes == KAFS_V6_GROUP_DESC_BYTES`.
- The group descriptor table must be fully inside `ld_descriptor_bytes`.
- `gd_group_id` must be exactly the table index.
- Unknown non-zero `gd_flags` are incompatible.
- `gd_metadata_block_count > 0`.
- `gd_metadata_start_blo + gd_metadata_block_count <= s_r_blkcnt`.
- `gd_data_start_blo + gd_data_block_count <= s_r_blkcnt`.
- Metadata block ranges for different groups must not overlap.
- Data block ranges for different groups must not overlap.
- A data block range must not overlap any metadata block range.
- `gd_first_shard_index + gd_shard_count <= ld_shard_count`.
- Every shard descriptor must be owned by exactly one group.

Group 0 must contain the shard that covers inode 0 and `KAFS_INO_ROOTDIR`. This keeps root
discovery deterministic.

## Shard Descriptor

Shard descriptors describe each logical metadata shard and its physical span. The shard table starts
at `ld_shard_desc_off` and contains `ld_shard_count` entries.

```c
#define KAFS_V6_SHARD_DESC_BYTES 96u

struct kafs_sv6_shard_desc {
  uint16_t sd_type;                 /* +0  KAFS_META_REGION_* compatible type */
  uint16_t sd_flags;                /* +2  type-specific flags, 0 for v1 */
  kafs_su32_t sd_group_id;          /* +4  owning metadata group */
  kafs_su64_t sd_physical_off;      /* +8  image byte offset */
  kafs_su64_t sd_physical_bytes;    /* +16 byte length */
  kafs_su64_t sd_logical_start;     /* +24 type-specific logical range start */
  kafs_su64_t sd_logical_count;     /* +32 type-specific logical range count */
  kafs_su32_t sd_record_bytes;      /* +40 fixed record size, or 0 for variable records */
  kafs_su32_t sd_header_bytes;      /* +44 shard-local header bytes, or 0 */
  kafs_su64_t sd_generation_floor;  /* +48 minimum valid generation for this shard */
  kafs_su64_t sd_mapping_seed;      /* +56 optional type-specific mapping seed */
  kafs_su64_t sd_reserved0;         /* +64 must be zero */
  kafs_su64_t sd_reserved1;         /* +72 must be zero */
  kafs_su64_t sd_reserved2;         /* +80 must be zero */
  kafs_su64_t sd_reserved3;         /* +88 must be zero */
} __attribute__((packed));
```

`sd_type` uses Phase 2 metadata region IDs where possible:

| type | name | logical range semantics |
| ---: | --- | --- |
| 0 | `superblock_checkpoint` | checkpoint replica ordinal range |
| 1 | `block_bitmap` | filesystem block number range |
| 2 | `inode_table` | inode number range |
| 3 | `allocator_summary` | filesystem block number range summarized by this shard |
| 4 | `hrl_index` | HRL bucket range |
| 5 | `hrl_entries` | HRL entry id range |
| 6 | `journal_header` | journal segment id range |
| 7 | `journal_data` | journal segment id range |
| 8 | `pending_log` | pending-log slot id range |
| 9 | `tail_metadata` | tail metadata container id range |
| 11 | `layout_descriptor` | descriptor replica ordinal range |

`10` (`unknown`) is a counter bucket only and is not a valid v6 shard type.

Shard validation:

- `ld_shard_desc_bytes == KAFS_V6_SHARD_DESC_BYTES`.
- The shard descriptor table must be fully inside `ld_descriptor_bytes`.
- `sd_group_id < ld_group_count`.
- Unknown `sd_type` values are incompatible.
- Unknown non-zero `sd_flags` are incompatible until the type-specific spec defines them.
- A T2-valid descriptor must include at least one shard for each type from
  `superblock_checkpoint` through `pending_log`.
- `sd_physical_bytes > 0`.
- `sd_physical_off + sd_physical_bytes <= image_size`.
- `sd_physical_off` and `sd_physical_bytes` must be block-aligned except:
  - `superblock_checkpoint` may include the 256-byte primary superblock at offset 0.
  - `layout_descriptor` may use `ld_descriptor_bytes` alignment rules from the anchor.
- A shard physical range must be contained in the owning group's metadata block range, except
  primary superblock checkpoint and `layout_descriptor` replicas.
- Writable metadata shard physical ranges must not overlap.
- `sd_logical_count > 0` for every required shard type.
- For fixed-record shard types, `sd_record_bytes` must match the format-specific record size.

Logical coverage requirements:

- `superblock_checkpoint` shards must cover ordinal 0 and include the primary 256-byte
  superblock at offset 0.
- `block_bitmap` shards must exactly cover `[0, s_r_blkcnt)` with no gaps and no overlap.
- `inode_table` shards must exactly cover `[0, s_inocnt)` with no gaps and no overlap.
- `allocator_summary` shards must cover the same block ranges as `block_bitmap` shards.
- `hrl_index` shards must exactly cover all HRL buckets.
- `hrl_entries` shards must exactly cover all HRL entry ids.
- `journal_header` and `journal_data` shards must cover the same journal segment id set.
- `pending_log` shards must cover all configured pending-log slot ids.
- `tail_metadata` shards are required when the v6 feature set includes tail packing; otherwise
  they may be absent.
- `layout_descriptor` shards must exactly cover replica ordinals `[0, ld_replica_count)` with one
  physical shard per replica candidate.

## Discovery And Rejection Rules

### Runtime Mount

Until `SDW-P4` explicitly enables v6 runtime support, `kafs` must reject `s_format_version=6` with
an unsupported-format error. It must not attempt to mount a v6 descriptor image through the v4/v5
prefix layout.

### `kafsdump`

`kafsdump --json` should report the v6 descriptor in read-only mode once the parser is implemented.
It must list every deterministic replica candidate with:

- `replica_id`
- `role`
- `offset`
- `bytes`
- `status`
- `generation` when the header could be decoded
- `crc_ok`
- `selected`

If the selected descriptor is found, `kafsdump` should print group, shard, and replica summaries
from that descriptor. Stale or corrupt non-selected candidates are reported in the replica list. If
all candidates fail, or if the selected generation has divergent valid CRCs, `kafsdump` must report
the superblock format plus descriptor replica statuses and exit non-zero.

### `fsck.kafs`

`fsck.kafs` discovery order for v6:

1. Read the 256-byte superblock at offset 0.
2. Verify `s_magic == KAFS_MAGIC` and `s_format_version == 6`.
3. Parse `kafs_sv6_superblock_anchor` from `s_reserved[0..31]`.
4. Validate anchor magic, version, flags, candidate count, primary offset, primary length, and
   anchor CRC.
5. Compute deterministic replica candidate offsets.
6. Read and validate every candidate independently.
7. Select the latest valid descriptor by generation and CRC.
8. Only after descriptor selection succeeds, run type-specific shard checks.

If step 3 through 7 fails, `fsck.kafs` must not fall back to v5 prefix offsets. It should report
`v6 descriptor discovery failed` and exit non-zero without repair.

If every readable candidate has unsupported `ld_version` or unknown `ld_incompat_flags`,
`fsck.kafs` must report `unsupported v6 layout descriptor` and exit non-zero without repair.
Unknown `ld_ro_compat_flags` allow read-only reporting but disable repair.

`fsck.kafs` must report per-replica status using the status terms above. Stale valid replicas and
corrupt non-selected replicas are warnings when a latest valid descriptor can be selected and shard
validation passes. Same-generation divergent replicas are errors because the newest logical layout
is ambiguous.

## Phase 4 Layout Dependencies

Phase 4 must keep the v6 descriptor as the single source of truth for metadata placement. Runtime
mount admission remains disabled until the following dependencies are implemented and covered by
`fsck.kafs`.

Common requirements:

- Descriptor discovery must succeed before any v5 prefix offset fallback is considered.
- Candidate replicas must not have same-generation divergence.
- Every shard descriptor range must be inside the image, block-aligned where required, and owned by
  an existing group.
- For each shard type that can be split, logical ranges must have no gaps or overlaps.
- `fsck.kafs` must validate descriptor coverage before mount enables v6 write paths.

Block bitmap shards:

- Current mkfs v6 scaffold maps the root block namespace `[0, s_r_blkcnt)` to bitmap storage so
  existing bitmap semantics remain checkable before runtime v6 mount is enabled.
- Runtime allocation/free work may narrow operational lookup to data blocks, but it must continue
  to reject descriptor gaps and overlaps against the authoritative bitmap namespace.
- Allocation/free must route a data block to exactly one bitmap shard.
- `fsck.kafs` detects duplicate logical coverage, missing coverage, and bitmap physical range
  overlap for the selected descriptor.

Inode table shards:

- `KAFS_META_REGION_INODE_TABLE` shard logical ranges map inode numbers to physical inode records.
- `sd_record_bytes` must match `kafs_inode_bytes_for_format(6)`.
- The root inode number must be covered by exactly one inode shard.
- `fsck.kafs` must validate inode count coverage and reject overlapping inode ranges.

Allocator summary shards:

- `KAFS_META_REGION_ALLOCATOR_SUMMARY` shards must correspond to bitmap/data-block groups.
- Summary rebuild must be possible from the authoritative bitmap shards.
- Allocation scan diagnostics must identify the shard that owns each summary record.

HRL shards:

- `KAFS_META_REGION_HRL_INDEX` and `KAFS_META_REGION_HRL_ENTRIES` shards must define a deterministic
  hash-bucket or group-local mapping.
- Lookup/put/inc/dec paths must route through descriptor mapping before touching HRL storage.
- `fsck.kafs` must validate chain bounds within the owning HRL entry shard.

Journal distribution:

- `KAFS_META_REGION_JOURNAL_HEADER` and `KAFS_META_REGION_JOURNAL_DATA` may appear as multiple
  segment shards in Phase 4.
- Replay must scan segment generations in deterministic order across metadata groups.
- Torn segment/header writes must leave at least one valid replay path or fail closed.

## Phase 3 Follow-Ups

- `SDW-P3-T2` is complete in this document: candidate locations, latest-valid selection,
  replica descriptor table, and stale/corrupt reporting are fixed.
- `SDW-P3-T3` adds `mkfs.kafs --format-version 6` scaffold output, parser constants/accessors, and
  explicit runtime mount rejection.
- `SDW-P3-T4` fixes parser/replica-selection tests for corrupt, stale, unsupported, and divergent
  descriptor cases.
- `SDW-P4` fills shard-internal formats and enables runtime mount only after fsck can validate the
  shard boundaries, logical coverage, and routing dependencies above.
