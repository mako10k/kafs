#pragma once

#include "kafs_v6_layout.h"

/*
 * Format-neutral facade for the descriptor scaffold.
 *
 * The current descriptor wire scaffold was introduced by the experimental v6
 * work and still uses the same on-disk magic/version values. v7 and shared
 * descriptor-backed runtime code should depend on this facade rather than on
 * v6 entrypoint names directly. A later wire-format break can swap the backing
 * implementation behind this boundary.
 */

#define KAFS_DESCRIPTOR_SUPERBLOCK_ANCHOR_MAGIC KAFS_V6_SUPERBLOCK_ANCHOR_MAGIC
#define KAFS_DESCRIPTOR_SUPERBLOCK_ANCHOR_VERSION KAFS_V6_SUPERBLOCK_ANCHOR_VERSION
#define KAFS_DESCRIPTOR_LAYOUT_MAGIC KAFS_V6_LAYOUT_MAGIC
#define KAFS_DESCRIPTOR_LAYOUT_VERSION KAFS_V6_LAYOUT_VERSION
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

static inline int kafs_descriptor_discover_layout(int fd, const kafs_ssuperblock_t *sb,
                                                  uint64_t file_size,
                                                  kafs_descriptor_layout_report_t *report)
{
  return kafs_v6_discover_layout(fd, sb, file_size, report);
}

static inline void kafs_descriptor_anchor_init(kafs_ssuperblock_t *sb, uint64_t primary_off,
                                               uint32_t desc_bytes, uint32_t candidate_count)
{
  kafs_v6_anchor_init(sb, primary_off, desc_bytes, candidate_count);
}

static inline int kafs_descriptor_build_mkfs_descriptor(
    void *buf, uint32_t desc_bytes, const kafs_ssuperblock_t *sb, uint64_t image_size,
    uint64_t desc_candidates[], uint32_t candidate_count, uint64_t bitmap_off,
    uint64_t bitmap_bytes, uint64_t inode_off, uint64_t inode_bytes)
{
  return kafs_v6_build_mkfs_descriptor(buf, desc_bytes, sb, image_size, desc_candidates,
                                       candidate_count, bitmap_off, bitmap_bytes, inode_off,
                                       inode_bytes);
}
