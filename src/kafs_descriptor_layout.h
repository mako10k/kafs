#pragma once

#include "kafs_v6_layout.h"

/*
 * Format-neutral facade for the descriptor scaffold.
 *
 * The descriptor wire scaffold was introduced by the experimental v6 work.
 * Format v7 keeps the same descriptor table shape for this slice, but owns
 * separate superblock-anchor and layout-descriptor magic values in
 * kafs_v7_layout.h. This header is the low-level explicit-wire scaffold used
 * by format-specific layout entrypoints; it is not the public v7 layout
 * entrypoint.
 */

#define KAFS_DESCRIPTOR_LAYOUT_HEADER_BYTES KAFS_V6_LAYOUT_HEADER_BYTES
#define KAFS_DESCRIPTOR_LAYOUT_MAX_BYTES KAFS_V6_LAYOUT_MAX_BYTES
#define KAFS_DESCRIPTOR_REPLICA_DESC_BYTES KAFS_V6_REPLICA_DESC_BYTES
#define KAFS_DESCRIPTOR_LAYOUT_REPLICA_MIN_COUNT KAFS_V6_LAYOUT_REPLICA_MIN_COUNT
#define KAFS_DESCRIPTOR_LAYOUT_REPLICA_MAX_COUNT KAFS_V6_LAYOUT_REPLICA_MAX_COUNT
#define KAFS_DESCRIPTOR_GROUP_DESC_BYTES KAFS_V6_GROUP_DESC_BYTES
#define KAFS_DESCRIPTOR_SHARD_DESC_BYTES KAFS_V6_SHARD_DESC_BYTES
#define KAFS_DESCRIPTOR_MAPPING_POLICY_GROUP_LOCAL_RANGE KAFS_V6_MAPPING_POLICY_GROUP_LOCAL_RANGE
#define KAFS_DESCRIPTOR_REPLICA_ROLE_PRIMARY KAFS_V6_REPLICA_ROLE_PRIMARY
#define KAFS_DESCRIPTOR_REPLICA_ROLE_TAIL_BACKUP KAFS_V6_REPLICA_ROLE_TAIL_BACKUP
#define KAFS_DESCRIPTOR_REPLICA_ROLE_MID_BACKUP KAFS_V6_REPLICA_ROLE_MID_BACKUP
#define KAFS_DESCRIPTOR_SHARD_TYPE_LAYOUT_DESCRIPTOR KAFS_V6_SHARD_TYPE_LAYOUT_DESCRIPTOR
#define KAFS_DESCRIPTOR_EXTENT_STORAGE_FIXED_RECORD KAFS_V6_EXTENT_STORAGE_FIXED_RECORD
#define KAFS_DESCRIPTOR_EXTENT_STORAGE_BIT_PACKED KAFS_V6_EXTENT_STORAGE_BIT_PACKED
#define KAFS_DESCRIPTOR_EXTENT_STORAGE_ALLOCATOR_SUMMARY KAFS_V6_EXTENT_STORAGE_ALLOCATOR_SUMMARY
#define KAFS_DESCRIPTOR_EXTENT_STORAGE_BYTE_SPAN KAFS_V6_EXTENT_STORAGE_BYTE_SPAN
#define KAFS_DESCRIPTOR_MKFS_GROUP_COUNT KAFS_V6_MKFS_GROUP_COUNT
#define KAFS_DESCRIPTOR_MKFS_BASE_SHARD_COUNT KAFS_V6_MKFS_BASE_SHARD_COUNT
#define KAFS_DESCRIPTOR_MKFS_GENERATION KAFS_V6_MKFS_GENERATION

typedef kafs_sv6_superblock_anchor_t kafs_sdescriptor_superblock_anchor_t;
typedef kafs_sv6_layout_desc_header_t kafs_sdescriptor_layout_desc_header_t;
typedef kafs_sv6_group_desc_t kafs_sdescriptor_group_desc_t;
typedef kafs_sv6_shard_desc_t kafs_sdescriptor_shard_desc_t;
typedef kafs_sv6_replica_desc_t kafs_sdescriptor_replica_desc_t;

typedef kafs_v6_replica_status_t kafs_descriptor_replica_status_t;
typedef kafs_v6_replica_report_t kafs_descriptor_replica_report_t;
typedef kafs_v6_layout_report_t kafs_descriptor_layout_report_t;
typedef kafs_v6_extent_coverage_t kafs_descriptor_extent_coverage_t;
typedef kafs_v6_extent_shard_view_t kafs_descriptor_extent_shard_view_t;

typedef kafs_v6_bitmap_lookup_t kafs_descriptor_bitmap_lookup_t;
typedef kafs_v6_bitmap_coverage_report_t kafs_descriptor_bitmap_coverage_report_t;
typedef kafs_v6_inode_lookup_t kafs_descriptor_inode_lookup_t;
typedef kafs_v6_inode_coverage_report_t kafs_descriptor_inode_coverage_report_t;
typedef kafs_v6_allocator_summary_lookup_t kafs_descriptor_allocator_summary_lookup_t;
typedef kafs_v6_allocator_summary_coverage_report_t
    kafs_descriptor_allocator_summary_coverage_report_t;
typedef kafs_v6_hrl_index_lookup_t kafs_descriptor_hrl_index_lookup_t;
typedef kafs_v6_hrl_index_coverage_report_t kafs_descriptor_hrl_index_coverage_report_t;
typedef kafs_v6_hrl_entry_lookup_t kafs_descriptor_hrl_entry_lookup_t;
typedef kafs_v6_hrl_entries_coverage_report_t kafs_descriptor_hrl_entries_coverage_report_t;
typedef kafs_v6_hrl_chain_report_t kafs_descriptor_hrl_chain_report_t;
typedef kafs_v6_journal_header_lookup_t kafs_descriptor_journal_header_lookup_t;
typedef kafs_v6_journal_header_coverage_report_t kafs_descriptor_journal_header_coverage_report_t;
typedef kafs_v6_journal_data_lookup_t kafs_descriptor_journal_data_lookup_t;
typedef kafs_v6_journal_data_coverage_report_t kafs_descriptor_journal_data_coverage_report_t;
typedef kafs_v6_journal_segment_lookup_t kafs_descriptor_journal_segment_lookup_t;
typedef kafs_v6_journal_segment_report_t kafs_descriptor_journal_segment_report_t;

typedef struct kafs_descriptor_wire_format
{
  uint32_t anchor_magic;
  uint16_t anchor_version;
  uint32_t layout_magic;
  uint16_t layout_version;
} kafs_descriptor_wire_format_t;

static inline const char *
kafs_descriptor_replica_status_name(kafs_descriptor_replica_status_t status)
{
  return kafs_v6_replica_status_name(status);
}

static inline const char *kafs_descriptor_replica_role_name(uint16_t role)
{
  return kafs_v6_replica_role_name(role);
}

static inline void kafs_descriptor_replica_summary(char *buf, size_t buf_sz,
                                                   const kafs_descriptor_replica_report_t *replica)
{
  kafs_v6_replica_summary(buf, buf_sz, replica);
}

static inline uint32_t kafs_descriptor_bytes_for_block(uint32_t block_size)
{
  return kafs_v6_descriptor_bytes_for_block(block_size);
}

static inline int kafs_descriptor_allocator_summary_shape(uint64_t block_count,
                                                          uint64_t *out_l0_bytes,
                                                          uint64_t *out_l1_bytes,
                                                          uint64_t *out_l2_bytes,
                                                          uint64_t *out_total_bytes)
{
  return kafs_v6_allocator_summary_shape(block_count, out_l0_bytes, out_l1_bytes, out_l2_bytes,
                                         out_total_bytes);
}

static inline uint32_t kafs_descriptor_layout_crc_calc(const void *desc, uint32_t desc_bytes)
{
  return kafs_v6_layout_crc_calc(desc, desc_bytes);
}

static inline const kafs_sdescriptor_shard_desc_t *
kafs_descriptor_shard_table(const void *desc, uint32_t desc_bytes, uint32_t *out_count)
{
  return kafs_v6_shard_table(desc, desc_bytes, out_count);
}

static inline int
kafs_descriptor_read_selected_descriptor(int fd, const kafs_descriptor_layout_report_t *report,
                                         void **out_desc, uint32_t *out_bytes)
{
  return kafs_v6_read_selected_descriptor(fd, report, out_desc, out_bytes);
}

static inline int kafs_descriptor_extent_next_shard(const kafs_sdescriptor_shard_desc_t *shards,
                                                    uint32_t shard_count, uint16_t shard_type,
                                                    uint32_t expected_record_bytes, int storage,
                                                    uint32_t *index,
                                                    kafs_descriptor_extent_shard_view_t *view)
{
  return kafs_v6_extent_next_shard(shards, shard_count, shard_type, expected_record_bytes, storage,
                                   index, view);
}

static inline int kafs_descriptor_bitmap_lookup(const void *desc, uint32_t desc_bytes, uint64_t blo,
                                                kafs_descriptor_bitmap_lookup_t *out)
{
  return kafs_v6_bitmap_lookup(desc, desc_bytes, blo, out);
}

static inline int kafs_descriptor_inode_lookup(const void *desc, uint32_t desc_bytes, uint64_t ino,
                                               kafs_descriptor_inode_lookup_t *out)
{
  return kafs_v6_inode_lookup(desc, desc_bytes, ino, out);
}

static inline int
kafs_descriptor_allocator_summary_lookup(const void *desc, uint32_t desc_bytes, uint64_t blo,
                                         kafs_descriptor_allocator_summary_lookup_t *out)
{
  return kafs_v6_allocator_summary_lookup(desc, desc_bytes, blo, out);
}

static inline int kafs_descriptor_hrl_index_lookup(const void *desc, uint32_t desc_bytes,
                                                   uint64_t bucket,
                                                   kafs_descriptor_hrl_index_lookup_t *out)
{
  return kafs_v6_hrl_index_lookup(desc, desc_bytes, bucket, out);
}

static inline int kafs_descriptor_hrl_entry_lookup(const void *desc, uint32_t desc_bytes,
                                                   uint64_t entry_id,
                                                   kafs_descriptor_hrl_entry_lookup_t *out)
{
  return kafs_v6_hrl_entry_lookup(desc, desc_bytes, entry_id, out);
}

static inline int
kafs_descriptor_journal_segment_lookup(const void *desc, uint32_t desc_bytes, uint64_t segment_id,
                                       kafs_descriptor_journal_segment_lookup_t *out)
{
  return kafs_v6_journal_segment_lookup(desc, desc_bytes, segment_id, out);
}

static inline int
kafs_descriptor_bitmap_validate_coverage(const void *desc, uint32_t desc_bytes,
                                         const kafs_ssuperblock_t *sb, uint64_t file_size,
                                         kafs_descriptor_bitmap_coverage_report_t *report)
{
  return kafs_v6_bitmap_validate_coverage(desc, desc_bytes, sb, file_size, report);
}

static inline int
kafs_descriptor_inode_validate_coverage(const void *desc, uint32_t desc_bytes,
                                        const kafs_ssuperblock_t *sb, uint64_t file_size,
                                        kafs_descriptor_inode_coverage_report_t *report)
{
  return kafs_v6_inode_validate_coverage(desc, desc_bytes, sb, file_size, report);
}

static inline int kafs_descriptor_allocator_summary_validate_coverage(
    const void *desc, uint32_t desc_bytes, const kafs_ssuperblock_t *sb, uint64_t file_size,
    kafs_descriptor_allocator_summary_coverage_report_t *report)
{
  return kafs_v6_allocator_summary_validate_coverage(desc, desc_bytes, sb, file_size, report);
}

static inline int
kafs_descriptor_hrl_index_validate_coverage(const void *desc, uint32_t desc_bytes,
                                            const kafs_ssuperblock_t *sb, uint64_t file_size,
                                            kafs_descriptor_hrl_index_coverage_report_t *report)
{
  return kafs_v6_hrl_index_validate_coverage(desc, desc_bytes, sb, file_size, report);
}

static inline int
kafs_descriptor_hrl_entries_validate_coverage(const void *desc, uint32_t desc_bytes,
                                              const kafs_ssuperblock_t *sb, uint64_t file_size,
                                              kafs_descriptor_hrl_entries_coverage_report_t *report)
{
  return kafs_v6_hrl_entries_validate_coverage(desc, desc_bytes, sb, file_size, report);
}

static inline int
kafs_descriptor_hrl_validate_chain_bounds_fd(int fd, const void *desc, uint32_t desc_bytes,
                                             const kafs_ssuperblock_t *sb, uint64_t file_size,
                                             kafs_descriptor_hrl_chain_report_t *report)
{
  return kafs_v6_hrl_validate_chain_bounds_fd(fd, desc, desc_bytes, sb, file_size, report);
}

static inline int kafs_descriptor_journal_header_validate_coverage(
    const void *desc, uint32_t desc_bytes, const kafs_ssuperblock_t *sb, uint64_t file_size,
    kafs_descriptor_journal_header_coverage_report_t *report)
{
  return kafs_v6_journal_header_validate_coverage(desc, desc_bytes, sb, file_size, report);
}

static inline int kafs_descriptor_journal_data_validate_coverage(
    const void *desc, uint32_t desc_bytes, const kafs_ssuperblock_t *sb, uint64_t file_size,
    kafs_descriptor_journal_data_coverage_report_t *report)
{
  return kafs_v6_journal_data_validate_coverage(desc, desc_bytes, sb, file_size, report);
}

static inline int
kafs_descriptor_journal_validate_segments_fd(int fd, const void *desc, uint32_t desc_bytes,
                                             const kafs_ssuperblock_t *sb, uint64_t file_size,
                                             kafs_descriptor_journal_segment_report_t *report)
{
  return kafs_v6_journal_validate_segments_fd(fd, desc, desc_bytes, sb, file_size, report);
}

static inline uint32_t
kafs_descriptor_anchor_crc_calc(const kafs_sdescriptor_superblock_anchor_t *anchor)
{
  return kafs_v6_anchor_crc_calc(anchor);
}

static inline int kafs_descriptor_anchor_crc_ok(const kafs_sdescriptor_superblock_anchor_t *anchor)
{
  return kafs_descriptor_anchor_crc_calc(anchor) == kafs_u32_stoh(anchor->va_anchor_crc32);
}

static inline int kafs_descriptor_validate_one_descriptor_wire(
    const void *desc, uint32_t desc_bytes, const kafs_ssuperblock_t *sb, uint64_t file_size,
    const uint64_t candidates[], uint32_t candidate, uint32_t candidate_count,
    const kafs_descriptor_wire_format_t *wire, kafs_descriptor_layout_report_t *report,
    uint32_t *out_crc)
{
  const kafs_sdescriptor_layout_desc_header_t *hdr =
      (const kafs_sdescriptor_layout_desc_header_t *)desc;
  uint32_t block_size = (uint32_t)kafs_sb_blksize_get(sb);
  uint32_t group_count;
  uint32_t shard_count;
  uint32_t replica_count;
  uint32_t group_off;
  uint32_t shard_off;
  uint32_t replica_off;
  uint32_t crc;

  if (!desc || !sb || !candidates || !wire || candidate >= candidate_count)
    return -EINVAL;
  if (desc_bytes < sizeof(*hdr))
    return -ERANGE;
  if (kafs_u32_stoh(hdr->ld_magic) != wire->layout_magic)
    return -EINVAL;
  if (le16toh(hdr->ld_version) != wire->layout_version)
    return -ENOTSUP;
  if (le16toh(hdr->ld_header_bytes) != KAFS_DESCRIPTOR_LAYOUT_HEADER_BYTES)
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
  if ((desc_bytes % block_size) != 0u || desc_bytes > KAFS_DESCRIPTOR_LAYOUT_MAX_BYTES)
    return -EINVAL;

  group_count = kafs_u32_stoh(hdr->ld_group_count);
  shard_count = kafs_u32_stoh(hdr->ld_shard_count);
  replica_count = kafs_u32_stoh(hdr->ld_replica_count);
  group_off = kafs_u32_stoh(hdr->ld_group_desc_off);
  shard_off = kafs_u32_stoh(hdr->ld_shard_desc_off);
  replica_off = kafs_u32_stoh(hdr->ld_replica_desc_off);
  if (group_count == 0u || shard_count == 0u || replica_count != candidate_count)
    return -EINVAL;
  if (replica_count < KAFS_DESCRIPTOR_LAYOUT_REPLICA_MIN_COUNT ||
      replica_count > KAFS_DESCRIPTOR_LAYOUT_REPLICA_MAX_COUNT)
    return -EINVAL;
  if (le16toh(hdr->ld_group_desc_bytes) != KAFS_DESCRIPTOR_GROUP_DESC_BYTES ||
      le16toh(hdr->ld_shard_desc_bytes) != KAFS_DESCRIPTOR_SHARD_DESC_BYTES ||
      le16toh(hdr->ld_replica_desc_bytes) != KAFS_DESCRIPTOR_REPLICA_DESC_BYTES)
    return -EINVAL;
  if (le16toh(hdr->ld_mapping_policy) != KAFS_DESCRIPTOR_MAPPING_POLICY_GROUP_LOCAL_RANGE)
    return -EINVAL;
  if (kafs_v6_table_bounds(group_off, group_count, KAFS_DESCRIPTOR_GROUP_DESC_BYTES, desc_bytes) !=
          0 ||
      kafs_v6_table_bounds(shard_off, shard_count, KAFS_DESCRIPTOR_SHARD_DESC_BYTES, desc_bytes) !=
          0 ||
      kafs_v6_table_bounds(replica_off, replica_count, KAFS_DESCRIPTOR_REPLICA_DESC_BYTES,
                           desc_bytes) != 0)
    return -ERANGE;

  crc = kafs_descriptor_layout_crc_calc(desc, desc_bytes);
  if (out_crc)
    *out_crc = crc;
  if (crc != kafs_u32_stoh(hdr->ld_descriptor_crc32))
    return -EINVAL;

  const kafs_sdescriptor_group_desc_t *groups =
      (const kafs_sdescriptor_group_desc_t *)((const char *)desc + group_off);
  const kafs_sdescriptor_shard_desc_t *shards =
      (const kafs_sdescriptor_shard_desc_t *)((const char *)desc + shard_off);
  const kafs_sdescriptor_replica_desc_t *replicas =
      (const kafs_sdescriptor_replica_desc_t *)((const char *)desc + replica_off);
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
        type != KAFS_DESCRIPTOR_SHARD_TYPE_LAYOUT_DESCRIPTOR &&
        ((off % block_size) != 0u || (bytes % block_size) != 0u))
      return -EINVAL;
    if (type <= KAFS_META_REGION_PENDING_LOG)
      type_seen_mask |= (1u << type);
    if (type == KAFS_DESCRIPTOR_SHARD_TYPE_LAYOUT_DESCRIPTOR)
      layout_shards++;
  }
  if ((type_seen_mask & 0x1ffu) != 0x1ffu || layout_shards != replica_count)
    return -EINVAL;

  for (uint32_t i = 0; i < replica_count; ++i)
  {
    uint16_t expected_role = (i == 0u) ? KAFS_DESCRIPTOR_REPLICA_ROLE_PRIMARY
                                       : ((i == 1u) ? KAFS_DESCRIPTOR_REPLICA_ROLE_TAIL_BACKUP
                                                    : KAFS_DESCRIPTOR_REPLICA_ROLE_MID_BACKUP);
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

static inline int kafs_descriptor_discover_layout_wire(int fd, const kafs_ssuperblock_t *sb,
                                                       uint64_t file_size,
                                                       const kafs_descriptor_wire_format_t *wire,
                                                       kafs_descriptor_layout_report_t *report)
{
  kafs_sdescriptor_superblock_anchor_t anchor;
  uint32_t desc_bytes;
  uint32_t candidate_count;
  uint32_t computed_count = 0;
  uint32_t valid_count = 0;
  uint64_t candidates[KAFS_DESCRIPTOR_LAYOUT_REPLICA_MAX_COUNT] = {0};
  struct
  {
    int valid;
    uint64_t generation;
    uint32_t crc;
  } valid[KAFS_DESCRIPTOR_LAYOUT_REPLICA_MAX_COUNT] = {{0}};

  if (!sb || !wire || !report)
    return -EINVAL;
  memset(report, 0, sizeof(*report));
  memcpy(&anchor, sb->s_reserved, sizeof(anchor));

  if (kafs_u32_stoh(anchor.va_magic) != wire->anchor_magic ||
      le16toh(anchor.va_version) != wire->anchor_version || le16toh(anchor.va_flags) != 0u ||
      kafs_u32_stoh(anchor.va_reserved0) != 0u || !kafs_descriptor_anchor_crc_ok(&anchor))
  {
    report->replica_count = 0;
    return -EINVAL;
  }
  report->anchor_valid = 1;

  desc_bytes = kafs_u32_stoh(anchor.va_primary_desc_bytes);
  candidate_count = kafs_u32_stoh(anchor.va_candidate_count);
  if (desc_bytes == 0u || desc_bytes > KAFS_DESCRIPTOR_LAYOUT_MAX_BYTES ||
      candidate_count < KAFS_DESCRIPTOR_LAYOUT_REPLICA_MIN_COUNT ||
      candidate_count > KAFS_DESCRIPTOR_LAYOUT_REPLICA_MAX_COUNT)
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
    report->replicas[i].role = (i == 0u) ? KAFS_DESCRIPTOR_REPLICA_ROLE_PRIMARY
                                         : ((i == 1u) ? KAFS_DESCRIPTOR_REPLICA_ROLE_TAIL_BACKUP
                                                      : KAFS_DESCRIPTOR_REPLICA_ROLE_MID_BACKUP);
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

    const kafs_sdescriptor_layout_desc_header_t *hdr =
        (const kafs_sdescriptor_layout_desc_header_t *)buf;
    report->replicas[i].generation = kafs_u64_stoh(hdr->ld_generation);
    report->replicas[i].descriptor_crc32 = kafs_u32_stoh(hdr->ld_descriptor_crc32);
    uint32_t crc = 0;
    rc = kafs_descriptor_validate_one_descriptor_wire(buf, desc_bytes, sb, file_size, candidates, i,
                                                      candidate_count, wire, report, &crc);
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
  report->selected_offset = candidates[selected];
  report->replicas[selected].selected = 1;
  report->replicas[selected].status = KAFS_V6_REPLICA_STATUS_SELECTED;
  return 0;
}

static inline void kafs_descriptor_anchor_init_wire(kafs_ssuperblock_t *sb, uint64_t primary_off,
                                                    uint32_t desc_bytes, uint32_t candidate_count,
                                                    const kafs_descriptor_wire_format_t *wire)
{
  if (!sb || !wire)
    return;

  kafs_sdescriptor_superblock_anchor_t anchor;
  memset(&anchor, 0, sizeof(anchor));
  anchor.va_magic = kafs_u32_htos(wire->anchor_magic);
  anchor.va_version = htole16(wire->anchor_version);
  anchor.va_primary_desc_off = kafs_u64_htos(primary_off);
  anchor.va_primary_desc_bytes = kafs_u32_htos(desc_bytes);
  anchor.va_candidate_count = kafs_u32_htos(candidate_count);
  anchor.va_anchor_crc32 = kafs_u32_htos(kafs_descriptor_anchor_crc_calc(&anchor));
  memcpy(sb->s_reserved, &anchor, sizeof(anchor));
}

static inline void kafs_descriptor_shard_set(kafs_sdescriptor_shard_desc_t *shard, uint16_t type,
                                             uint32_t group_id, uint64_t physical_off,
                                             uint64_t physical_bytes, uint64_t logical_start,
                                             uint64_t logical_count, uint32_t record_bytes,
                                             uint32_t header_bytes)
{
  kafs_v6_shard_set(shard, type, group_id, physical_off, physical_bytes, logical_start,
                    logical_count, record_bytes, header_bytes);
}

static inline void kafs_descriptor_replica_set(kafs_sdescriptor_replica_desc_t *replica,
                                               uint32_t id, uint64_t physical_off,
                                               uint32_t desc_bytes)
{
  kafs_v6_replica_set(replica, id, physical_off, desc_bytes);
}

static inline int kafs_descriptor_build_mkfs_descriptor_wire(
    void *buf, uint32_t desc_bytes, const kafs_ssuperblock_t *sb, uint64_t image_size,
    uint64_t desc_candidates[], uint32_t candidate_count, uint64_t bitmap_off,
    uint64_t bitmap_bytes, uint64_t inode_off, uint64_t inode_bytes,
    const kafs_descriptor_wire_format_t *wire)
{
  if (!buf || !sb || !desc_candidates || !wire ||
      candidate_count < KAFS_DESCRIPTOR_LAYOUT_REPLICA_MIN_COUNT ||
      candidate_count > KAFS_DESCRIPTOR_LAYOUT_REPLICA_MAX_COUNT)
    return -EINVAL;

  memset(buf, 0, desc_bytes);
  uint32_t shard_count = KAFS_DESCRIPTOR_MKFS_BASE_SHARD_COUNT + candidate_count;
  uint32_t group_off = KAFS_DESCRIPTOR_LAYOUT_HEADER_BYTES;
  uint32_t shard_off = group_off + KAFS_DESCRIPTOR_GROUP_DESC_BYTES;
  uint32_t replica_off = shard_off + shard_count * KAFS_DESCRIPTOR_SHARD_DESC_BYTES;
  if (kafs_v6_table_bounds(replica_off, candidate_count, KAFS_DESCRIPTOR_REPLICA_DESC_BYTES,
                           desc_bytes) != 0)
    return -ERANGE;

  kafs_sdescriptor_layout_desc_header_t *hdr = (kafs_sdescriptor_layout_desc_header_t *)buf;
  hdr->ld_magic = kafs_u32_htos(wire->layout_magic);
  hdr->ld_version = htole16(wire->layout_version);
  hdr->ld_header_bytes = htole16(KAFS_DESCRIPTOR_LAYOUT_HEADER_BYTES);
  hdr->ld_descriptor_bytes = kafs_u32_htos(desc_bytes);
  hdr->ld_generation = kafs_u64_htos(KAFS_DESCRIPTOR_MKFS_GENERATION);
  hdr->ld_image_size_bytes = kafs_u64_htos(image_size);
  hdr->ld_block_size = kafs_u32_htos((uint32_t)kafs_sb_blksize_get(sb));
  hdr->ld_group_count = kafs_u32_htos(KAFS_DESCRIPTOR_MKFS_GROUP_COUNT);
  hdr->ld_group_desc_off = kafs_u32_htos(group_off);
  hdr->ld_group_desc_bytes = htole16(KAFS_DESCRIPTOR_GROUP_DESC_BYTES);
  hdr->ld_mapping_policy = htole16(KAFS_DESCRIPTOR_MAPPING_POLICY_GROUP_LOCAL_RANGE);
  hdr->ld_shard_count = kafs_u32_htos(shard_count);
  hdr->ld_shard_desc_off = kafs_u32_htos(shard_off);
  hdr->ld_shard_desc_bytes = htole16(KAFS_DESCRIPTOR_SHARD_DESC_BYTES);
  hdr->ld_replica_count = kafs_u32_htos(candidate_count);
  hdr->ld_replica_desc_off = kafs_u32_htos(replica_off);
  hdr->ld_replica_desc_bytes = htole16(KAFS_DESCRIPTOR_REPLICA_DESC_BYTES);

  kafs_sdescriptor_group_desc_t *group = (kafs_sdescriptor_group_desc_t *)((char *)buf + group_off);
  uint32_t first_data = (uint32_t)kafs_sb_first_data_block_get(sb);
  uint32_t r_blkcnt = (uint32_t)kafs_sb_r_blkcnt_get(sb);
  group->gd_group_id = kafs_u32_htos(0);
  group->gd_metadata_start_blo = kafs_u32_htos(0);
  group->gd_metadata_block_count = kafs_u32_htos(first_data ? first_data : 1u);
  group->gd_data_start_blo = kafs_u32_htos(first_data);
  group->gd_data_block_count = kafs_u32_htos((r_blkcnt > first_data) ? (r_blkcnt - first_data) : 0);
  group->gd_first_shard_index = kafs_u32_htos(0);
  group->gd_shard_count = kafs_u32_htos(shard_count);

  kafs_sdescriptor_shard_desc_t *shards =
      (kafs_sdescriptor_shard_desc_t *)((char *)buf + shard_off);
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

  kafs_descriptor_shard_set(&shards[0], KAFS_META_REGION_SUPERBLOCK_CHECKPOINT, 0, 0,
                            sizeof(kafs_ssuperblock_t), 0, 1, sizeof(kafs_ssuperblock_t), 0);
  kafs_descriptor_shard_set(&shards[1], KAFS_META_REGION_BLOCK_BITMAP, 0, bitmap_off, bitmap_bytes,
                            0, kafs_sb_r_blkcnt_get(sb), 0, 0);
  kafs_descriptor_shard_set(&shards[2], KAFS_META_REGION_INODE_TABLE, 0, inode_off, inode_bytes, 0,
                            kafs_sb_inocnt_get(sb),
                            (uint32_t)kafs_inode_bytes_for_format(kafs_sb_format_version_get(sb)),
                            0);
  kafs_descriptor_shard_set(&shards[3], KAFS_META_REGION_ALLOCATOR_SUMMARY, 0, allocator_off,
                            allocator_bytes, 0, kafs_sb_r_blkcnt_get(sb), 0, 0);
  kafs_descriptor_shard_set(&shards[4], KAFS_META_REGION_HRL_INDEX, 0, hrl_index_off,
                            hrl_index_bytes, 0, hrl_index_bytes / sizeof(uint32_t),
                            sizeof(uint32_t), 0);
  kafs_descriptor_shard_set(&shards[5], KAFS_META_REGION_HRL_ENTRIES, 0, hrl_entry_off,
                            hrl_entry_bytes, 0, hrl_entry_count, sizeof(kafs_hrl_entry_t), 0);
  kafs_descriptor_shard_set(&shards[6], KAFS_META_REGION_JOURNAL_HEADER, 0, journal_off,
                            journal_header_bytes, 0, 1, sizeof(kj_header_t), 0);
  kafs_descriptor_shard_set(
      &shards[7], KAFS_META_REGION_JOURNAL_DATA, 0, journal_off + journal_header_bytes,
      (journal_size > journal_header_bytes) ? (journal_size - journal_header_bytes) : block_size, 0,
      1, 0, 0);
  kafs_descriptor_shard_set(&shards[8], KAFS_META_REGION_PENDING_LOG, 0, pending_off, pending_bytes,
                            0, 1, 0, 0);
  for (uint32_t i = 0; i < candidate_count; ++i)
    kafs_descriptor_shard_set(&shards[KAFS_DESCRIPTOR_MKFS_BASE_SHARD_COUNT + i],
                              KAFS_DESCRIPTOR_SHARD_TYPE_LAYOUT_DESCRIPTOR, 0, desc_candidates[i],
                              desc_bytes, i, 1, desc_bytes, 0);

  kafs_sdescriptor_replica_desc_t *replicas =
      (kafs_sdescriptor_replica_desc_t *)((char *)buf + replica_off);
  for (uint32_t i = 0; i < candidate_count; ++i)
    kafs_descriptor_replica_set(&replicas[i], i, desc_candidates[i], desc_bytes);

  hdr->ld_descriptor_crc32 = kafs_u32_htos(kafs_descriptor_layout_crc_calc(buf, desc_bytes));
  return 0;
}
