#pragma once

#include "kafs.h"
#include "kafs_hash.h"
#include "kafs_inode.h"
#include "kafs_journal.h"
#include "kafs_meta_region.h"
#include "kafs_offline_summary.h"
#include "kafs_superblock.h"
#include "kafs_tool_util.h"

#include <errno.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define KAFS_V6_SUPERBLOCK_ANCHOR_MAGIC 0x4B365341u /* 'K6SA' */
#define KAFS_V6_SUPERBLOCK_ANCHOR_VERSION 1u
#define KAFS_V6_LAYOUT_MAGIC 0x4B364C44u /* 'K6LD' */
#define KAFS_V6_LAYOUT_VERSION 1u
#define KAFS_V6_LAYOUT_HEADER_BYTES 128u
#define KAFS_V6_LAYOUT_MAX_BYTES (16u * 1024u * 1024u)
#define KAFS_V6_REPLICA_DESC_BYTES 32u
#define KAFS_V6_LAYOUT_REPLICA_MIN_COUNT 2u
#define KAFS_V6_LAYOUT_REPLICA_MAX_COUNT 3u
#define KAFS_V6_GROUP_DESC_BYTES 64u
#define KAFS_V6_SHARD_DESC_BYTES 96u
#define KAFS_V6_MAPPING_POLICY_GROUP_LOCAL_RANGE 0u
#define KAFS_V6_REPLICA_ROLE_PRIMARY 0u
#define KAFS_V6_REPLICA_ROLE_TAIL_BACKUP 1u
#define KAFS_V6_REPLICA_ROLE_MID_BACKUP 2u
#define KAFS_V6_SHARD_TYPE_LAYOUT_DESCRIPTOR 11u
#define KAFS_V6_MKFS_GROUP_COUNT 1u
#define KAFS_V6_MKFS_BASE_SHARD_COUNT 9u
#define KAFS_V6_MKFS_GENERATION 1u

typedef struct kafs_sv6_superblock_anchor
{
  kafs_su32_t va_magic;
  uint16_t va_version;
  uint16_t va_flags;
  kafs_su64_t va_primary_desc_off;
  kafs_su32_t va_primary_desc_bytes;
  kafs_su32_t va_candidate_count;
  kafs_su32_t va_anchor_crc32;
  kafs_su32_t va_reserved0;
} __attribute__((packed)) kafs_sv6_superblock_anchor_t;

typedef struct kafs_sv6_layout_desc_header
{
  kafs_su32_t ld_magic;
  uint16_t ld_version;
  uint16_t ld_header_bytes;
  kafs_su32_t ld_descriptor_bytes;
  kafs_su32_t ld_flags;
  kafs_su64_t ld_generation;
  kafs_su64_t ld_image_size_bytes;
  kafs_su32_t ld_block_size;
  kafs_su32_t ld_group_count;
  kafs_su32_t ld_group_desc_off;
  uint16_t ld_group_desc_bytes;
  uint16_t ld_mapping_policy;
  kafs_su32_t ld_shard_count;
  kafs_su32_t ld_shard_desc_off;
  uint16_t ld_shard_desc_bytes;
  uint16_t ld_reserved0;
  kafs_su32_t ld_replica_count;
  kafs_su32_t ld_replica_desc_off;
  uint16_t ld_replica_desc_bytes;
  uint16_t ld_reserved1;
  kafs_su64_t ld_feature_flags;
  kafs_su64_t ld_incompat_flags;
  kafs_su64_t ld_ro_compat_flags;
  kafs_su64_t ld_mapping_seed;
  kafs_su32_t ld_descriptor_crc32;
  kafs_su32_t ld_reserved2;
  kafs_su64_t ld_reserved3;
  kafs_su64_t ld_reserved4;
} __attribute__((packed)) kafs_sv6_layout_desc_header_t;

typedef struct kafs_sv6_group_desc
{
  kafs_su32_t gd_group_id;
  kafs_su32_t gd_flags;
  kafs_su32_t gd_metadata_start_blo;
  kafs_su32_t gd_metadata_block_count;
  kafs_su32_t gd_data_start_blo;
  kafs_su32_t gd_data_block_count;
  kafs_su32_t gd_first_shard_index;
  kafs_su32_t gd_shard_count;
  kafs_su64_t gd_generation_floor;
  kafs_su64_t gd_reserved0;
  kafs_su64_t gd_reserved1;
  kafs_su64_t gd_reserved2;
} __attribute__((packed)) kafs_sv6_group_desc_t;

typedef struct kafs_sv6_shard_desc
{
  uint16_t sd_type;
  uint16_t sd_flags;
  kafs_su32_t sd_group_id;
  kafs_su64_t sd_physical_off;
  kafs_su64_t sd_physical_bytes;
  kafs_su64_t sd_logical_start;
  kafs_su64_t sd_logical_count;
  kafs_su32_t sd_record_bytes;
  kafs_su32_t sd_header_bytes;
  kafs_su64_t sd_generation_floor;
  kafs_su64_t sd_mapping_seed;
  kafs_su64_t sd_reserved0;
  kafs_su64_t sd_reserved1;
  kafs_su64_t sd_reserved2;
  kafs_su64_t sd_reserved3;
} __attribute__((packed)) kafs_sv6_shard_desc_t;

typedef struct kafs_sv6_replica_desc
{
  kafs_su32_t rd_replica_id;
  uint16_t rd_role;
  uint16_t rd_flags;
  kafs_su64_t rd_physical_off;
  kafs_su32_t rd_descriptor_bytes;
  kafs_su32_t rd_reserved0;
  kafs_su64_t rd_reserved1;
} __attribute__((packed)) kafs_sv6_replica_desc_t;

_Static_assert(sizeof(kafs_sv6_superblock_anchor_t) == 32,
               "kafs_sv6_superblock_anchor_t must be 32 bytes");
_Static_assert(sizeof(kafs_sv6_layout_desc_header_t) == KAFS_V6_LAYOUT_HEADER_BYTES,
               "kafs_sv6_layout_desc_header_t must be 128 bytes");
_Static_assert(sizeof(kafs_sv6_group_desc_t) == KAFS_V6_GROUP_DESC_BYTES,
               "kafs_sv6_group_desc_t must be 64 bytes");
_Static_assert(sizeof(kafs_sv6_shard_desc_t) == KAFS_V6_SHARD_DESC_BYTES,
               "kafs_sv6_shard_desc_t must be 96 bytes");
_Static_assert(sizeof(kafs_sv6_replica_desc_t) == KAFS_V6_REPLICA_DESC_BYTES,
               "kafs_sv6_replica_desc_t must be 32 bytes");

typedef enum kafs_v6_replica_status
{
  KAFS_V6_REPLICA_STATUS_MISSING = 0,
  KAFS_V6_REPLICA_STATUS_CORRUPT,
  KAFS_V6_REPLICA_STATUS_UNSUPPORTED,
  KAFS_V6_REPLICA_STATUS_STALE,
  KAFS_V6_REPLICA_STATUS_VALID,
  KAFS_V6_REPLICA_STATUS_SELECTED,
  KAFS_V6_REPLICA_STATUS_DIVERGENT,
} kafs_v6_replica_status_t;

typedef struct kafs_v6_replica_report
{
  uint32_t replica_id;
  uint16_t role;
  uint64_t offset;
  uint32_t bytes;
  kafs_v6_replica_status_t status;
  uint64_t generation;
  uint32_t descriptor_crc32;
  int crc_ok;
  int selected;
} kafs_v6_replica_report_t;

typedef struct kafs_v6_layout_report
{
  int anchor_valid;
  int selected_found;
  int divergent;
  int unsupported_only;
  uint32_t replica_count;
  uint32_t selected_replica;
  uint64_t selected_generation;
  uint32_t selected_crc32;
  uint32_t descriptor_bytes;
  uint32_t group_count;
  uint32_t shard_count;
  uint64_t image_size_bytes;
  uint32_t block_size;
  uint64_t ro_compat_flags;
  kafs_v6_replica_report_t replicas[KAFS_V6_LAYOUT_REPLICA_MAX_COUNT];
} kafs_v6_layout_report_t;

static inline const char *kafs_v6_replica_status_name(kafs_v6_replica_status_t status)
{
  switch (status)
  {
  case KAFS_V6_REPLICA_STATUS_MISSING:
    return "missing";
  case KAFS_V6_REPLICA_STATUS_CORRUPT:
    return "corrupt";
  case KAFS_V6_REPLICA_STATUS_UNSUPPORTED:
    return "unsupported";
  case KAFS_V6_REPLICA_STATUS_STALE:
    return "stale";
  case KAFS_V6_REPLICA_STATUS_VALID:
    return "valid";
  case KAFS_V6_REPLICA_STATUS_SELECTED:
    return "selected";
  case KAFS_V6_REPLICA_STATUS_DIVERGENT:
    return "divergent";
  default:
    return "unknown";
  }
}

static inline const char *kafs_v6_replica_role_name(uint16_t role)
{
  switch (role)
  {
  case KAFS_V6_REPLICA_ROLE_PRIMARY:
    return "primary";
  case KAFS_V6_REPLICA_ROLE_TAIL_BACKUP:
    return "tail_backup";
  case KAFS_V6_REPLICA_ROLE_MID_BACKUP:
    return "mid_backup";
  default:
    return "unknown";
  }
}

static inline void kafs_v6_replica_summary(char *buf, size_t buf_sz,
                                           const kafs_v6_replica_report_t *replica)
{
  if (!buf || buf_sz == 0)
    return;
  if (!replica)
  {
    snprintf(buf, buf_sz, "unavailable");
    return;
  }

  snprintf(buf, buf_sz,
           "role=%s offset=%" PRIu64 " bytes=%" PRIu32 " status=%s generation=%" PRIu64
           " crc_ok=%s selected=%s",
           kafs_v6_replica_role_name(replica->role), replica->offset, replica->bytes,
           kafs_v6_replica_status_name(replica->status), replica->generation,
           replica->crc_ok ? "true" : "false", replica->selected ? "true" : "false");
}

static inline uint64_t kafs_v6_align_down_u64(uint64_t v, uint64_t align)
{
  if (align == 0)
    return v;
  return v & ~(align - 1u);
}

static inline uint32_t kafs_v6_descriptor_bytes_for_block(uint32_t block_size)
{
  uint64_t bytes =
      KAFS_V6_LAYOUT_HEADER_BYTES + KAFS_V6_GROUP_DESC_BYTES +
      ((uint64_t)KAFS_V6_MKFS_BASE_SHARD_COUNT + (uint64_t)KAFS_V6_LAYOUT_REPLICA_MAX_COUNT) *
          KAFS_V6_SHARD_DESC_BYTES +
      (uint64_t)KAFS_V6_LAYOUT_REPLICA_MAX_COUNT * KAFS_V6_REPLICA_DESC_BYTES;

  bytes = kafs_offline_align_up_u64(bytes, block_size);
  return (bytes > UINT32_MAX) ? 0u : (uint32_t)bytes;
}

static inline int kafs_v6_candidate_offsets(uint64_t image_size, uint32_t block_size,
                                            uint64_t primary_off, uint32_t desc_bytes,
                                            uint64_t out[KAFS_V6_LAYOUT_REPLICA_MAX_COUNT],
                                            uint32_t *out_count)
{
  if (!out || !out_count || block_size == 0 || desc_bytes == 0)
    return -EINVAL;
  if (image_size < (uint64_t)desc_bytes)
    return -ERANGE;

  uint64_t primary_end = primary_off + (uint64_t)desc_bytes;
  if (primary_end < primary_off || primary_end > image_size)
    return -ERANGE;

  uint64_t tail = kafs_v6_align_down_u64(image_size - (uint64_t)desc_bytes, block_size);
  if (tail < primary_end)
    return -ERANGE;

  out[0] = primary_off;
  out[1] = tail;
  *out_count = 2;

  uint64_t mid = kafs_v6_align_down_u64(image_size / 2u, block_size);
  if (mid >= primary_end && mid + (uint64_t)desc_bytes <= tail)
  {
    out[2] = mid;
    *out_count = 3;
  }
  return 0;
}

static inline uint32_t kafs_v6_anchor_crc_calc(const kafs_sv6_superblock_anchor_t *anchor)
{
  kafs_sv6_superblock_anchor_t tmp = *anchor;

  tmp.va_anchor_crc32 = kafs_u32_htos(0);
  return kj_crc32(&tmp, sizeof(tmp));
}

static inline int kafs_v6_anchor_crc_ok(const kafs_sv6_superblock_anchor_t *anchor)
{
  return kafs_v6_anchor_crc_calc(anchor) == kafs_u32_stoh(anchor->va_anchor_crc32);
}

static inline uint32_t kafs_v6_layout_crc_calc(const void *desc, uint32_t desc_bytes)
{
  if (!desc || desc_bytes < sizeof(kafs_sv6_layout_desc_header_t))
    return 0;

  const uint8_t *bytes = (const uint8_t *)desc;
  const size_t crc_off = offsetof(kafs_sv6_layout_desc_header_t, ld_descriptor_crc32);
  const uint32_t zero = 0;
  uint32_t crc = kj_crc32_update(0, bytes, crc_off);

  crc = kj_crc32_update(crc, (const uint8_t *)&zero, sizeof(zero));
  crc = kj_crc32_update(crc, bytes + crc_off + sizeof(zero), desc_bytes - crc_off - sizeof(zero));
  return crc;
}

static inline int kafs_v6_table_bounds(uint32_t off, uint64_t count, uint32_t entry_bytes,
                                       uint32_t desc_bytes)
{
  uint64_t bytes;

  if (entry_bytes == 0)
    return -EINVAL;
  if (count > UINT64_MAX / (uint64_t)entry_bytes)
    return -ERANGE;
  bytes = count * (uint64_t)entry_bytes;
  if (off > desc_bytes || bytes > (uint64_t)desc_bytes - off)
    return -ERANGE;
  return 0;
}

static inline int kafs_v6_shard_type_known(uint16_t type)
{
  return type <= KAFS_META_REGION_TAIL_METADATA || type == KAFS_V6_SHARD_TYPE_LAYOUT_DESCRIPTOR;
}

static inline int kafs_v6_validate_one_descriptor(const void *desc, uint32_t desc_bytes,
                                                  const kafs_ssuperblock_t *sb, uint64_t file_size,
                                                  const uint64_t candidates[], uint32_t candidate,
                                                  uint32_t candidate_count,
                                                  kafs_v6_layout_report_t *report,
                                                  uint32_t *out_crc)
{
  const kafs_sv6_layout_desc_header_t *hdr = (const kafs_sv6_layout_desc_header_t *)desc;
  uint32_t block_size = (uint32_t)kafs_sb_blksize_get(sb);
  uint32_t group_count;
  uint32_t shard_count;
  uint32_t replica_count;
  uint32_t group_off;
  uint32_t shard_off;
  uint32_t replica_off;
  uint32_t crc;

  if (!desc || !sb || !candidates || candidate >= candidate_count)
    return -EINVAL;
  if (desc_bytes < sizeof(*hdr))
    return -ERANGE;
  if (kafs_u32_stoh(hdr->ld_magic) != KAFS_V6_LAYOUT_MAGIC)
    return -EINVAL;
  if (le16toh(hdr->ld_version) != KAFS_V6_LAYOUT_VERSION)
    return -ENOTSUP;
  if (le16toh(hdr->ld_header_bytes) != KAFS_V6_LAYOUT_HEADER_BYTES)
    return -EINVAL;
  if (kafs_u32_stoh(hdr->ld_descriptor_bytes) != desc_bytes)
    return -EINVAL;
  if (kafs_u32_stoh(hdr->ld_flags) != 0u || kafs_u64_stoh(hdr->ld_incompat_flags) != 0u)
    return -ENOTSUP;
  if (le16toh(hdr->ld_reserved0) != 0u || le16toh(hdr->ld_reserved1) != 0u ||
      kafs_u32_stoh(hdr->ld_reserved2) != 0u || kafs_u64_stoh(hdr->ld_reserved3) != 0u ||
      kafs_u64_stoh(hdr->ld_reserved4) != 0u)
    return -EINVAL;
  if (kafs_u64_stoh(hdr->ld_image_size_bytes) != file_size)
    return -EINVAL;
  if (kafs_u32_stoh(hdr->ld_block_size) != block_size)
    return -EINVAL;
  if ((desc_bytes % block_size) != 0u || desc_bytes > KAFS_V6_LAYOUT_MAX_BYTES)
    return -EINVAL;

  group_count = kafs_u32_stoh(hdr->ld_group_count);
  shard_count = kafs_u32_stoh(hdr->ld_shard_count);
  replica_count = kafs_u32_stoh(hdr->ld_replica_count);
  group_off = kafs_u32_stoh(hdr->ld_group_desc_off);
  shard_off = kafs_u32_stoh(hdr->ld_shard_desc_off);
  replica_off = kafs_u32_stoh(hdr->ld_replica_desc_off);
  if (group_count == 0u || shard_count == 0u || replica_count != candidate_count)
    return -EINVAL;
  if (replica_count < KAFS_V6_LAYOUT_REPLICA_MIN_COUNT ||
      replica_count > KAFS_V6_LAYOUT_REPLICA_MAX_COUNT)
    return -EINVAL;
  if (le16toh(hdr->ld_group_desc_bytes) != KAFS_V6_GROUP_DESC_BYTES ||
      le16toh(hdr->ld_shard_desc_bytes) != KAFS_V6_SHARD_DESC_BYTES ||
      le16toh(hdr->ld_replica_desc_bytes) != KAFS_V6_REPLICA_DESC_BYTES)
    return -EINVAL;
  if (le16toh(hdr->ld_mapping_policy) != KAFS_V6_MAPPING_POLICY_GROUP_LOCAL_RANGE)
    return -EINVAL;
  if (kafs_v6_table_bounds(group_off, group_count, KAFS_V6_GROUP_DESC_BYTES, desc_bytes) != 0 ||
      kafs_v6_table_bounds(shard_off, shard_count, KAFS_V6_SHARD_DESC_BYTES, desc_bytes) != 0 ||
      kafs_v6_table_bounds(replica_off, replica_count, KAFS_V6_REPLICA_DESC_BYTES, desc_bytes) != 0)
    return -ERANGE;

  crc = kafs_v6_layout_crc_calc(desc, desc_bytes);
  if (out_crc)
    *out_crc = crc;
  if (crc != kafs_u32_stoh(hdr->ld_descriptor_crc32))
    return -EINVAL;

  const kafs_sv6_group_desc_t *groups =
      (const kafs_sv6_group_desc_t *)((const char *)desc + group_off);
  const kafs_sv6_shard_desc_t *shards =
      (const kafs_sv6_shard_desc_t *)((const char *)desc + shard_off);
  const kafs_sv6_replica_desc_t *replicas =
      (const kafs_sv6_replica_desc_t *)((const char *)desc + replica_off);
  uint64_t r_blkcnt = (uint64_t)kafs_sb_r_blkcnt_get(sb);
  uint32_t type_seen_mask = 0;
  uint32_t layout_shards = 0;

  for (uint32_t i = 0; i < group_count; ++i)
  {
    uint32_t meta_start = kafs_u32_stoh(groups[i].gd_metadata_start_blo);
    uint32_t meta_count = kafs_u32_stoh(groups[i].gd_metadata_block_count);
    uint32_t data_start = kafs_u32_stoh(groups[i].gd_data_start_blo);
    uint32_t data_count = kafs_u32_stoh(groups[i].gd_data_block_count);
    uint32_t first_shard = kafs_u32_stoh(groups[i].gd_first_shard_index);
    uint32_t group_shards = kafs_u32_stoh(groups[i].gd_shard_count);

    if (kafs_u32_stoh(groups[i].gd_group_id) != i || kafs_u32_stoh(groups[i].gd_flags) != 0u)
      return -EINVAL;
    if (kafs_u64_stoh(groups[i].gd_reserved0) != 0u ||
        kafs_u64_stoh(groups[i].gd_reserved1) != 0u || kafs_u64_stoh(groups[i].gd_reserved2) != 0u)
      return -EINVAL;
    if (meta_count == 0u || (uint64_t)meta_start + meta_count > r_blkcnt)
      return -ERANGE;
    if ((uint64_t)data_start + data_count > r_blkcnt)
      return -ERANGE;
    if ((uint64_t)first_shard + group_shards > shard_count)
      return -ERANGE;
  }

  for (uint32_t i = 0; i < shard_count; ++i)
  {
    uint16_t type = le16toh(shards[i].sd_type);
    uint16_t flags = le16toh(shards[i].sd_flags);
    uint32_t group_id = kafs_u32_stoh(shards[i].sd_group_id);
    uint64_t off = kafs_u64_stoh(shards[i].sd_physical_off);
    uint64_t bytes = kafs_u64_stoh(shards[i].sd_physical_bytes);
    uint64_t logical_count = kafs_u64_stoh(shards[i].sd_logical_count);

    if (!kafs_v6_shard_type_known(type) || flags != 0u || group_id >= group_count)
      return -EINVAL;
    if (kafs_u64_stoh(shards[i].sd_reserved0) != 0u ||
        kafs_u64_stoh(shards[i].sd_reserved1) != 0u ||
        kafs_u64_stoh(shards[i].sd_reserved2) != 0u || kafs_u64_stoh(shards[i].sd_reserved3) != 0u)
      return -EINVAL;
    if (bytes == 0u || logical_count == 0u)
      return -EINVAL;
    if (kafs_offline_check_bounds(off, bytes, file_size) != 0)
      return -ERANGE;
    if (type != KAFS_META_REGION_SUPERBLOCK_CHECKPOINT &&
        type != KAFS_V6_SHARD_TYPE_LAYOUT_DESCRIPTOR &&
        ((off % block_size) != 0u || (bytes % block_size) != 0u))
      return -EINVAL;
    if (type <= KAFS_META_REGION_PENDING_LOG)
      type_seen_mask |= (1u << type);
    if (type == KAFS_V6_SHARD_TYPE_LAYOUT_DESCRIPTOR)
      layout_shards++;
  }
  if ((type_seen_mask & 0x1ffu) != 0x1ffu || layout_shards != replica_count)
    return -EINVAL;

  for (uint32_t i = 0; i < replica_count; ++i)
  {
    uint16_t expected_role = (i == 0u) ? KAFS_V6_REPLICA_ROLE_PRIMARY
                                       : ((i == 1u) ? KAFS_V6_REPLICA_ROLE_TAIL_BACKUP
                                                    : KAFS_V6_REPLICA_ROLE_MID_BACKUP);
    if (kafs_u32_stoh(replicas[i].rd_replica_id) != i ||
        le16toh(replicas[i].rd_role) != expected_role || le16toh(replicas[i].rd_flags) != 0u ||
        kafs_u64_stoh(replicas[i].rd_physical_off) != candidates[i] ||
        kafs_u32_stoh(replicas[i].rd_descriptor_bytes) != desc_bytes ||
        kafs_u32_stoh(replicas[i].rd_reserved0) != 0u ||
        kafs_u64_stoh(replicas[i].rd_reserved1) != 0u)
      return -EINVAL;
  }

  if (report)
  {
    report->group_count = group_count;
    report->shard_count = shard_count;
    report->descriptor_bytes = desc_bytes;
    report->image_size_bytes = kafs_u64_stoh(hdr->ld_image_size_bytes);
    report->block_size = block_size;
    report->ro_compat_flags = kafs_u64_stoh(hdr->ld_ro_compat_flags);
  }
  return 0;
}

static inline int kafs_v6_discover_layout(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                                          kafs_v6_layout_report_t *report)
{
  kafs_sv6_superblock_anchor_t anchor;
  uint32_t desc_bytes;
  uint32_t candidate_count;
  uint32_t computed_count = 0;
  uint32_t valid_count = 0;
  uint64_t candidates[KAFS_V6_LAYOUT_REPLICA_MAX_COUNT] = {0};
  struct
  {
    int valid;
    uint64_t generation;
    uint32_t crc;
  } valid[KAFS_V6_LAYOUT_REPLICA_MAX_COUNT] = {{0}};

  if (!sb || !report)
    return -EINVAL;
  memset(report, 0, sizeof(*report));
  memcpy(&anchor, sb->s_reserved, sizeof(anchor));

  if (kafs_u32_stoh(anchor.va_magic) != KAFS_V6_SUPERBLOCK_ANCHOR_MAGIC ||
      le16toh(anchor.va_version) != KAFS_V6_SUPERBLOCK_ANCHOR_VERSION ||
      le16toh(anchor.va_flags) != 0u || kafs_u32_stoh(anchor.va_reserved0) != 0u ||
      !kafs_v6_anchor_crc_ok(&anchor))
  {
    report->replica_count = 0;
    return -EINVAL;
  }
  report->anchor_valid = 1;

  desc_bytes = kafs_u32_stoh(anchor.va_primary_desc_bytes);
  candidate_count = kafs_u32_stoh(anchor.va_candidate_count);
  if (desc_bytes == 0u || desc_bytes > KAFS_V6_LAYOUT_MAX_BYTES ||
      candidate_count < KAFS_V6_LAYOUT_REPLICA_MIN_COUNT ||
      candidate_count > KAFS_V6_LAYOUT_REPLICA_MAX_COUNT)
    return -EINVAL;

  if (kafs_v6_candidate_offsets(file_size, (uint32_t)kafs_sb_blksize_get(sb),
                                kafs_u64_stoh(anchor.va_primary_desc_off), desc_bytes, candidates,
                                &computed_count) != 0 ||
      computed_count != candidate_count)
    return -ERANGE;
  report->replica_count = candidate_count;

  for (uint32_t i = 0; i < candidate_count; ++i)
  {
    report->replicas[i].replica_id = i;
    report->replicas[i].role = (i == 0u) ? KAFS_V6_REPLICA_ROLE_PRIMARY
                                         : ((i == 1u) ? KAFS_V6_REPLICA_ROLE_TAIL_BACKUP
                                                      : KAFS_V6_REPLICA_ROLE_MID_BACKUP);
    report->replicas[i].offset = candidates[i];
    report->replicas[i].bytes = desc_bytes;
    report->replicas[i].status = KAFS_V6_REPLICA_STATUS_MISSING;

    if (kafs_offline_check_bounds(candidates[i], desc_bytes, file_size) != 0)
      continue;

    void *buf = malloc(desc_bytes);
    if (!buf)
      return -ENOMEM;
    int rc = kafs_pread_all(fd, buf, desc_bytes, (off_t)candidates[i]);
    if (rc != 0)
    {
      free(buf);
      continue;
    }

    const kafs_sv6_layout_desc_header_t *hdr = (const kafs_sv6_layout_desc_header_t *)buf;
    report->replicas[i].generation = kafs_u64_stoh(hdr->ld_generation);
    report->replicas[i].descriptor_crc32 = kafs_u32_stoh(hdr->ld_descriptor_crc32);
    uint32_t crc = 0;
    rc = kafs_v6_validate_one_descriptor(buf, desc_bytes, sb, file_size, candidates, i,
                                         candidate_count, report, &crc);
    report->replicas[i].crc_ok = (rc == 0);
    if (rc == 0)
    {
      valid[i].valid = 1;
      valid[i].generation = report->replicas[i].generation;
      valid[i].crc = crc;
      valid_count++;
      report->replicas[i].status = KAFS_V6_REPLICA_STATUS_VALID;
    }
    else if (rc == -ENOTSUP)
    {
      report->replicas[i].status = KAFS_V6_REPLICA_STATUS_UNSUPPORTED;
    }
    else
    {
      report->replicas[i].status = KAFS_V6_REPLICA_STATUS_CORRUPT;
    }
    free(buf);
  }

  if (valid_count == 0u)
  {
    report->unsupported_only = 1;
    for (uint32_t i = 0; i < candidate_count; ++i)
      if (report->replicas[i].status != KAFS_V6_REPLICA_STATUS_UNSUPPORTED)
        report->unsupported_only = 0;
    return report->unsupported_only ? -ENOTSUP : -EINVAL;
  }

  uint32_t selected = UINT32_MAX;
  for (uint32_t i = 0; i < candidate_count; ++i)
  {
    if (!valid[i].valid)
      continue;
    if (selected == UINT32_MAX || valid[i].generation > valid[selected].generation ||
        (valid[i].generation == valid[selected].generation && i < selected))
      selected = i;
  }

  for (uint32_t i = 0; i < candidate_count; ++i)
  {
    if (!valid[i].valid)
      continue;
    if (valid[i].generation < valid[selected].generation)
      report->replicas[i].status = KAFS_V6_REPLICA_STATUS_STALE;
    else if (valid[i].crc != valid[selected].crc)
    {
      report->replicas[i].status = KAFS_V6_REPLICA_STATUS_DIVERGENT;
      report->divergent = 1;
    }
  }
  if (report->divergent)
    return -EINVAL;

  report->selected_found = 1;
  report->selected_replica = selected;
  report->selected_generation = valid[selected].generation;
  report->selected_crc32 = valid[selected].crc;
  report->replicas[selected].selected = 1;
  report->replicas[selected].status = KAFS_V6_REPLICA_STATUS_SELECTED;
  return 0;
}

static inline void kafs_v6_anchor_init(kafs_ssuperblock_t *sb, uint64_t primary_off,
                                       uint32_t desc_bytes, uint32_t candidate_count)
{
  kafs_sv6_superblock_anchor_t anchor;

  memset(&anchor, 0, sizeof(anchor));
  anchor.va_magic = kafs_u32_htos(KAFS_V6_SUPERBLOCK_ANCHOR_MAGIC);
  anchor.va_version = htole16(KAFS_V6_SUPERBLOCK_ANCHOR_VERSION);
  anchor.va_primary_desc_off = kafs_u64_htos(primary_off);
  anchor.va_primary_desc_bytes = kafs_u32_htos(desc_bytes);
  anchor.va_candidate_count = kafs_u32_htos(candidate_count);
  anchor.va_anchor_crc32 = kafs_u32_htos(kafs_v6_anchor_crc_calc(&anchor));
  memcpy(sb->s_reserved, &anchor, sizeof(anchor));
}

static inline void kafs_v6_shard_set(kafs_sv6_shard_desc_t *shard, uint16_t type, uint32_t group_id,
                                     uint64_t physical_off, uint64_t physical_bytes,
                                     uint64_t logical_start, uint64_t logical_count,
                                     uint32_t record_bytes, uint32_t header_bytes)
{
  memset(shard, 0, sizeof(*shard));
  shard->sd_type = htole16(type);
  shard->sd_group_id = kafs_u32_htos(group_id);
  shard->sd_physical_off = kafs_u64_htos(physical_off);
  shard->sd_physical_bytes = kafs_u64_htos(physical_bytes);
  shard->sd_logical_start = kafs_u64_htos(logical_start);
  shard->sd_logical_count = kafs_u64_htos(logical_count);
  shard->sd_record_bytes = kafs_u32_htos(record_bytes);
  shard->sd_header_bytes = kafs_u32_htos(header_bytes);
}

static inline void kafs_v6_replica_set(kafs_sv6_replica_desc_t *replica, uint32_t id,
                                       uint64_t physical_off, uint32_t desc_bytes)
{
  uint16_t role = (id == 0u) ? KAFS_V6_REPLICA_ROLE_PRIMARY
                             : ((id == 1u) ? KAFS_V6_REPLICA_ROLE_TAIL_BACKUP
                                           : KAFS_V6_REPLICA_ROLE_MID_BACKUP);

  memset(replica, 0, sizeof(*replica));
  replica->rd_replica_id = kafs_u32_htos(id);
  replica->rd_role = htole16(role);
  replica->rd_physical_off = kafs_u64_htos(physical_off);
  replica->rd_descriptor_bytes = kafs_u32_htos(desc_bytes);
}

static inline int kafs_v6_build_mkfs_descriptor(void *buf, uint32_t desc_bytes,
                                                const kafs_ssuperblock_t *sb, uint64_t image_size,
                                                uint64_t desc_candidates[],
                                                uint32_t candidate_count, uint64_t bitmap_off,
                                                uint64_t bitmap_bytes, uint64_t inode_off,
                                                uint64_t inode_bytes)
{
  if (!buf || !sb || !desc_candidates || candidate_count < KAFS_V6_LAYOUT_REPLICA_MIN_COUNT ||
      candidate_count > KAFS_V6_LAYOUT_REPLICA_MAX_COUNT)
    return -EINVAL;

  memset(buf, 0, desc_bytes);
  uint32_t shard_count = KAFS_V6_MKFS_BASE_SHARD_COUNT + candidate_count;
  uint32_t group_off = KAFS_V6_LAYOUT_HEADER_BYTES;
  uint32_t shard_off = group_off + KAFS_V6_GROUP_DESC_BYTES;
  uint32_t replica_off = shard_off + shard_count * KAFS_V6_SHARD_DESC_BYTES;
  if (kafs_v6_table_bounds(replica_off, candidate_count, KAFS_V6_REPLICA_DESC_BYTES, desc_bytes) !=
      0)
    return -ERANGE;

  kafs_sv6_layout_desc_header_t *hdr = (kafs_sv6_layout_desc_header_t *)buf;
  hdr->ld_magic = kafs_u32_htos(KAFS_V6_LAYOUT_MAGIC);
  hdr->ld_version = htole16(KAFS_V6_LAYOUT_VERSION);
  hdr->ld_header_bytes = htole16(KAFS_V6_LAYOUT_HEADER_BYTES);
  hdr->ld_descriptor_bytes = kafs_u32_htos(desc_bytes);
  hdr->ld_generation = kafs_u64_htos(KAFS_V6_MKFS_GENERATION);
  hdr->ld_image_size_bytes = kafs_u64_htos(image_size);
  hdr->ld_block_size = kafs_u32_htos((uint32_t)kafs_sb_blksize_get(sb));
  hdr->ld_group_count = kafs_u32_htos(KAFS_V6_MKFS_GROUP_COUNT);
  hdr->ld_group_desc_off = kafs_u32_htos(group_off);
  hdr->ld_group_desc_bytes = htole16(KAFS_V6_GROUP_DESC_BYTES);
  hdr->ld_mapping_policy = htole16(KAFS_V6_MAPPING_POLICY_GROUP_LOCAL_RANGE);
  hdr->ld_shard_count = kafs_u32_htos(shard_count);
  hdr->ld_shard_desc_off = kafs_u32_htos(shard_off);
  hdr->ld_shard_desc_bytes = htole16(KAFS_V6_SHARD_DESC_BYTES);
  hdr->ld_replica_count = kafs_u32_htos(candidate_count);
  hdr->ld_replica_desc_off = kafs_u32_htos(replica_off);
  hdr->ld_replica_desc_bytes = htole16(KAFS_V6_REPLICA_DESC_BYTES);

  kafs_sv6_group_desc_t *group = (kafs_sv6_group_desc_t *)((char *)buf + group_off);
  uint32_t first_data = (uint32_t)kafs_sb_first_data_block_get(sb);
  uint32_t r_blkcnt = (uint32_t)kafs_sb_r_blkcnt_get(sb);
  group->gd_group_id = kafs_u32_htos(0);
  group->gd_metadata_start_blo = kafs_u32_htos(0);
  group->gd_metadata_block_count = kafs_u32_htos(first_data ? first_data : 1u);
  group->gd_data_start_blo = kafs_u32_htos(first_data);
  group->gd_data_block_count = kafs_u32_htos((r_blkcnt > first_data) ? (r_blkcnt - first_data) : 0);
  group->gd_first_shard_index = kafs_u32_htos(0);
  group->gd_shard_count = kafs_u32_htos(shard_count);

  kafs_sv6_shard_desc_t *shards = (kafs_sv6_shard_desc_t *)((char *)buf + shard_off);
  uint32_t block_size = (uint32_t)kafs_sb_blksize_get(sb);
  bitmap_bytes = kafs_offline_align_up_u64(bitmap_bytes, block_size);
  inode_bytes = kafs_offline_align_up_u64(inode_bytes, block_size);
  uint64_t allocator_off = kafs_sb_allocator_offset_get(sb);
  uint64_t allocator_bytes = kafs_sb_allocator_size_get(sb);
  uint64_t hrl_index_off = kafs_sb_hrl_index_offset_get(sb);
  uint64_t hrl_index_bytes = kafs_sb_hrl_index_size_get(sb);
  uint64_t hrl_entry_off = kafs_sb_hrl_entry_offset_get(sb);
  uint64_t hrl_entry_count = kafs_sb_hrl_entry_cnt_get(sb);
  uint64_t hrl_entry_bytes = hrl_entry_count * (uint64_t)sizeof(kafs_hrl_entry_t);
  hrl_entry_bytes = kafs_offline_align_up_u64(hrl_entry_bytes, block_size);
  uint64_t journal_off = kafs_sb_journal_offset_get(sb);
  uint64_t journal_size = kafs_sb_journal_size_get(sb);
  uint64_t journal_header_bytes = block_size;
  uint64_t pending_off = kafs_sb_pendinglog_offset_get(sb);
  uint64_t pending_bytes = kafs_sb_pendinglog_size_get(sb);

  kafs_v6_shard_set(&shards[0], KAFS_META_REGION_SUPERBLOCK_CHECKPOINT, 0, 0,
                    sizeof(kafs_ssuperblock_t), 0, 1, sizeof(kafs_ssuperblock_t), 0);
  kafs_v6_shard_set(&shards[1], KAFS_META_REGION_BLOCK_BITMAP, 0, bitmap_off, bitmap_bytes, 0,
                    kafs_sb_r_blkcnt_get(sb), 0, 0);
  kafs_v6_shard_set(&shards[2], KAFS_META_REGION_INODE_TABLE, 0, inode_off, inode_bytes, 0,
                    kafs_sb_inocnt_get(sb),
                    (uint32_t)kafs_inode_bytes_for_format(kafs_sb_format_version_get(sb)), 0);
  kafs_v6_shard_set(&shards[3], KAFS_META_REGION_ALLOCATOR_SUMMARY, 0, allocator_off,
                    allocator_bytes, 0, kafs_sb_r_blkcnt_get(sb), 0, 0);
  kafs_v6_shard_set(&shards[4], KAFS_META_REGION_HRL_INDEX, 0, hrl_index_off, hrl_index_bytes, 0,
                    hrl_index_bytes / sizeof(uint32_t), sizeof(uint32_t), 0);
  kafs_v6_shard_set(&shards[5], KAFS_META_REGION_HRL_ENTRIES, 0, hrl_entry_off, hrl_entry_bytes, 0,
                    hrl_entry_count, sizeof(kafs_hrl_entry_t), 0);
  kafs_v6_shard_set(&shards[6], KAFS_META_REGION_JOURNAL_HEADER, 0, journal_off,
                    journal_header_bytes, 0, 1, sizeof(kj_header_t), 0);
  kafs_v6_shard_set(
      &shards[7], KAFS_META_REGION_JOURNAL_DATA, 0, journal_off + journal_header_bytes,
      (journal_size > journal_header_bytes) ? (journal_size - journal_header_bytes) : block_size, 0,
      1, 0, 0);
  kafs_v6_shard_set(&shards[8], KAFS_META_REGION_PENDING_LOG, 0, pending_off, pending_bytes, 0, 1,
                    0, 0);
  for (uint32_t i = 0; i < candidate_count; ++i)
    kafs_v6_shard_set(&shards[KAFS_V6_MKFS_BASE_SHARD_COUNT + i],
                      KAFS_V6_SHARD_TYPE_LAYOUT_DESCRIPTOR, 0, desc_candidates[i], desc_bytes, i, 1,
                      desc_bytes, 0);

  kafs_sv6_replica_desc_t *replicas = (kafs_sv6_replica_desc_t *)((char *)buf + replica_off);
  for (uint32_t i = 0; i < candidate_count; ++i)
    kafs_v6_replica_set(&replicas[i], i, desc_candidates[i], desc_bytes);

  hdr->ld_descriptor_crc32 = kafs_u32_htos(kafs_v6_layout_crc_calc(buf, desc_bytes));
  return 0;
}
