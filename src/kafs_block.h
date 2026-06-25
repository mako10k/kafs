#pragma once
#include "kafs_config.h"
#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_locks.h"
#include "kafs_v6_layout.h"
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fuse.h>
#ifdef __linux__
#include <sys/syscall.h>
#include <linux/falloc.h>
#endif

/// ブロック番号のうち存在しないことを表す値
#define KAFS_BLO_NONE ((kafs_blkcnt_t)0)
/// ブロックマスクのビット数
#define KAFS_BLKMASK_BITS (sizeof(kafs_blkmask_t) << 3)
/// ブロックマスクのビット数の log2 の値
#define KAFS_BLKMASK_LOG_BITS (__builtin_ctz(sizeof(kafs_blkmask_t)) + 3)
/// ブロックマスクのビット数のマスク値
#define KAFS_BLKMASK_MASK_BITS (KAFS_BLKMASK_BITS - 1)

static kafs_blkcnt_t kafs_get_free_blkmask(kafs_blkmask_t bm)
{
  if (sizeof(kafs_blkmask_t) <= sizeof(unsigned int))
    return __builtin_ctz(bm);
  if (sizeof(kafs_blkmask_t) <= sizeof(unsigned long))
    return __builtin_ctzl(bm);
  return __builtin_ctzll(bm);
}

static inline uint64_t kafs_blk_now_ns(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static int kafs_meta_bitmap_words_enabled(const struct kafs_context *ctx)
{
  if (!ctx)
    return KAFS_FALSE;
  if (!ctx->c_meta_delta_enabled)
    return KAFS_FALSE;
  if (!ctx->c_meta_bitmap_words_enabled)
    return KAFS_FALSE;
  if (!ctx->c_meta_bitmap_words || !ctx->c_meta_bitmap_dirty)
    return KAFS_FALSE;
  return KAFS_TRUE;
}

static const kafs_blkmask_t *kafs_meta_bitmap_tbl_const(const struct kafs_context *ctx)
{
  if (kafs_meta_bitmap_words_enabled(ctx) && ctx->c_meta_bitmap_wordcnt > 0)
    return ctx->c_meta_bitmap_words;
  return ctx->c_blkmasktbl;
}

static kafs_blkmask_t *kafs_meta_bitmap_tbl_mut(struct kafs_context *ctx)
{
  if (kafs_meta_bitmap_words_enabled(ctx) && ctx->c_meta_bitmap_wordcnt > 0)
    return ctx->c_meta_bitmap_words;
  return ctx->c_blkmasktbl;
}

static void kafs_meta_bitmap_mark_dirty(struct kafs_context *ctx, kafs_blkcnt_t blod)
{
  if (!kafs_meta_bitmap_words_enabled(ctx))
    return;
  if ((size_t)blod >= ctx->c_meta_bitmap_wordcnt)
    return;
  if (!ctx->c_meta_bitmap_dirty[blod])
  {
    ctx->c_meta_bitmap_dirty[blod] = 1u;
    ctx->c_meta_bitmap_dirty_count++;
  }
}

typedef struct kafs_bitmap_word_ref
{
  kafs_ssuperblock_t *sb;
  kafs_blkcnt_t dirty_word_index;
  kafs_blkcnt_t word_logical_start;
  kafs_blkmask_t bit;
  kafs_blkmask_t *word_ptr;
  kafs_blkmask_t word;
  int meta_overlay;
  int count_block_bitmap_write;
} kafs_bitmap_word_ref_t;

static int kafs_bitmap_descriptor_mapping_enabled(const struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_superblock)
    return KAFS_FALSE;
  if (kafs_sb_format_version_get(ctx->c_superblock) != KAFS_FORMAT_VERSION_V6)
    return KAFS_FALSE;
  if (!ctx->c_v6_bitmap_mapping_enabled || !ctx->c_v6_layout_desc ||
      ctx->c_v6_layout_desc_bytes == 0u)
    return KAFS_FALSE;
  if (!ctx->c_img_base || ctx->c_img_size == 0u)
    return KAFS_FALSE;
  return KAFS_TRUE;
}

static void kafs_bitmap_descriptor_mapping_clear(struct kafs_context *ctx)
{
  if (!ctx)
    return;
  if (ctx->c_v6_layout_desc_owned && ctx->c_v6_layout_desc)
    free((void *)ctx->c_v6_layout_desc);
  free(ctx->c_v6_inode_shards);
  free(ctx->c_v6_alloc_summary_shards);
  free(ctx->c_v6_hrl_index_shards);
  free(ctx->c_v6_hrl_entry_shards);
  ctx->c_v6_bitmap_mapping_enabled = 0;
  ctx->c_v6_inode_mapping_enabled = 0;
  ctx->c_v6_alloc_summary_mapping_enabled = 0;
  ctx->c_v6_hrl_mapping_enabled = 0;
  ctx->c_v6_layout_desc = NULL;
  ctx->c_v6_layout_desc_bytes = 0;
  ctx->c_v6_layout_desc_owned = 0;
  ctx->c_v6_inode_shards = NULL;
  ctx->c_v6_inode_shard_count = 0;
  ctx->c_v6_alloc_summary_shards = NULL;
  ctx->c_v6_alloc_summary_shard_count = 0;
  ctx->c_v6_hrl_index_shards = NULL;
  ctx->c_v6_hrl_index_shard_count = 0;
  ctx->c_v6_hrl_entry_shards = NULL;
  ctx->c_v6_hrl_entry_shard_count = 0;
}

static int kafs_v6_inode_runtime_shards_build(const void *desc, uint32_t desc_bytes,
                                              uint64_t file_size,
                                              kafs_v6_inode_runtime_shard_t *out_shards,
                                              uint32_t out_cap, uint32_t *out_count)
{
  if (!desc || !out_count || (out_cap > 0u && !out_shards))
    return -EINVAL;

  uint32_t descriptor_shard_count = 0;
  const kafs_sv6_shard_desc_t *descriptor_shards =
      kafs_v6_shard_table(desc, desc_bytes, &descriptor_shard_count);
  if (!descriptor_shards)
    return -EINVAL;

  uint32_t record_bytes = (uint32_t)kafs_inode_bytes_for_format(KAFS_FORMAT_VERSION_V6);
  uint32_t used = 0;
  for (uint32_t i = 0; i < descriptor_shard_count; ++i)
  {
    if (le16toh(descriptor_shards[i].sd_type) != KAFS_META_REGION_INODE_TABLE)
      continue;
    if (used >= out_cap)
      return -ERANGE;

    uint64_t logical_start = kafs_u64_stoh(descriptor_shards[i].sd_logical_start);
    uint64_t logical_count = kafs_u64_stoh(descriptor_shards[i].sd_logical_count);
    uint64_t physical_off = kafs_u64_stoh(descriptor_shards[i].sd_physical_off);
    uint64_t physical_bytes = kafs_u64_stoh(descriptor_shards[i].sd_physical_bytes);
    uint32_t shard_record_bytes = kafs_u32_stoh(descriptor_shards[i].sd_record_bytes);
    if (shard_record_bytes != record_bytes || record_bytes == 0u || logical_count == 0u)
      return -EINVAL;
    if (logical_count > UINT64_MAX / (uint64_t)record_bytes)
      return -ERANGE;
    uint64_t required_bytes = logical_count * (uint64_t)record_bytes;
    if (required_bytes == 0u || required_bytes > physical_bytes)
      return -ERANGE;
    if (physical_off > UINT64_MAX - required_bytes)
      return -ERANGE;
    if (kafs_offline_check_bounds(physical_off, physical_bytes, file_size) != 0)
      return -ERANGE;

    out_shards[used].logical_start = logical_start;
    out_shards[used].logical_count = logical_count;
    out_shards[used].physical_off = physical_off;
    out_shards[used].physical_end = physical_off + required_bytes;
    out_shards[used].record_bytes = record_bytes;
    used++;
  }

  *out_count = used;
  return 0;
}

static int kafs_v6_alloc_summary_runtime_shards_build(
    const void *desc, uint32_t desc_bytes, uint64_t file_size,
    kafs_v6_alloc_summary_runtime_shard_t *out_shards, uint32_t out_cap, uint32_t *out_count)
{
  if (!desc || !out_count || (out_cap > 0u && !out_shards))
    return -EINVAL;

  uint32_t descriptor_shard_count = 0;
  const kafs_sv6_shard_desc_t *descriptor_shards =
      kafs_v6_shard_table(desc, desc_bytes, &descriptor_shard_count);
  if (!descriptor_shards)
    return -EINVAL;

  uint32_t used = 0;
  for (uint32_t i = 0; i < descriptor_shard_count; ++i)
  {
    if (le16toh(descriptor_shards[i].sd_type) != KAFS_META_REGION_ALLOCATOR_SUMMARY)
      continue;
    if (used >= out_cap)
      return -ERANGE;

    uint64_t logical_start = kafs_u64_stoh(descriptor_shards[i].sd_logical_start);
    uint64_t logical_count = kafs_u64_stoh(descriptor_shards[i].sd_logical_count);
    uint64_t physical_off = kafs_u64_stoh(descriptor_shards[i].sd_physical_off);
    uint64_t physical_bytes = kafs_u64_stoh(descriptor_shards[i].sd_physical_bytes);
    uint32_t record_bytes = kafs_u32_stoh(descriptor_shards[i].sd_record_bytes);
    uint64_t l0_bytes = 0;
    uint64_t l1_bytes = 0;
    uint64_t l2_bytes = 0;
    uint64_t summary_bytes = 0;
    if (record_bytes != 0u)
      return -EINVAL;
    int rc = kafs_v6_allocator_summary_shape(logical_count, &l0_bytes, &l1_bytes, &l2_bytes,
                                             &summary_bytes);
    if (rc != 0)
      return rc;
    if (summary_bytes == 0u || summary_bytes > physical_bytes)
      return -ERANGE;
    if (physical_off > UINT64_MAX - summary_bytes)
      return -ERANGE;
    if (kafs_offline_check_bounds(physical_off, physical_bytes, file_size) != 0)
      return -ERANGE;

    out_shards[used].logical_start = logical_start;
    out_shards[used].logical_count = logical_count;
    out_shards[used].physical_off = physical_off;
    out_shards[used].physical_end = physical_off + summary_bytes;
    out_shards[used].l0_bytes = l0_bytes;
    out_shards[used].l1_bytes = l1_bytes;
    out_shards[used].l2_bytes = l2_bytes;
    used++;
  }

  *out_count = used;
  return 0;
}

static int kafs_v6_hrl_runtime_shards_build(const void *desc, uint32_t desc_bytes,
                                            uint64_t file_size, uint16_t shard_type,
                                            uint32_t record_bytes,
                                            kafs_v6_hrl_runtime_shard_t *out_shards,
                                            uint32_t out_cap, uint32_t *out_count)
{
  if (!desc || !out_count || record_bytes == 0u || (out_cap > 0u && !out_shards))
    return -EINVAL;

  uint32_t descriptor_shard_count = 0;
  const kafs_sv6_shard_desc_t *descriptor_shards =
      kafs_v6_shard_table(desc, desc_bytes, &descriptor_shard_count);
  if (!descriptor_shards)
    return -EINVAL;

  uint32_t used = 0;
  uint32_t index = 0;
  for (;;)
  {
    kafs_v6_extent_shard_view_t shard;
    int rc = kafs_v6_extent_next_shard(descriptor_shards, descriptor_shard_count, shard_type,
                                       record_bytes, KAFS_V6_EXTENT_STORAGE_FIXED_RECORD, &index,
                                       &shard);
    if (rc == -ENOENT)
      break;
    if (rc != 0)
      return rc;
    if (used >= out_cap)
      return -ERANGE;
    if (kafs_offline_check_bounds(shard.physical_off, shard.physical_bytes, file_size) != 0)
      return -ERANGE;
    if (shard.physical_off > UINT64_MAX - shard.required_bytes)
      return -ERANGE;

    out_shards[used].logical_start = shard.logical_start;
    out_shards[used].logical_count = shard.logical_count;
    out_shards[used].physical_off = shard.physical_off;
    out_shards[used].physical_end = shard.physical_off + shard.required_bytes;
    out_shards[used].record_bytes = record_bytes;
    out_shards[used].group_id = shard.group_id;
    used++;
  }

  *out_count = used;
  return 0;
}

static int kafs_v6_descriptor_mapping_read_fd(struct kafs_context *ctx, int fd, uint64_t file_size,
                                              void **out_desc, uint32_t *out_desc_bytes)
{
  if (!ctx || !ctx->c_superblock || fd < 0 || !out_desc || !out_desc_bytes)
    return -EINVAL;
  if (kafs_sb_format_version_get(ctx->c_superblock) != KAFS_FORMAT_VERSION_V6)
    return -EPROTONOSUPPORT;

  kafs_v6_layout_report_t layout;
  int rc = kafs_v6_discover_layout(fd, ctx->c_superblock, file_size, &layout);
  if (rc == 0)
    rc = kafs_v6_read_selected_descriptor(fd, &layout, out_desc, out_desc_bytes);
  return rc;
}

static int kafs_bitmap_descriptor_mapping_admit_desc(struct kafs_context *ctx, void *desc,
                                                     uint32_t desc_bytes, uint64_t file_size,
                                                     kafs_v6_bitmap_coverage_report_t *out_report)
{
  if (!ctx || !ctx->c_superblock || !desc || desc_bytes == 0u)
    return -EINVAL;
  if (kafs_sb_format_version_get(ctx->c_superblock) != KAFS_FORMAT_VERSION_V6)
    return -EPROTONOSUPPORT;

  kafs_v6_bitmap_coverage_report_t report;
  int rc =
      kafs_v6_bitmap_validate_coverage(desc, desc_bytes, ctx->c_superblock, file_size, &report);
  if (out_report)
    *out_report = report;
  if (rc != 0)
    return rc;

  kafs_bitmap_descriptor_mapping_clear(ctx);

  ctx->c_v6_layout_desc = desc;
  ctx->c_v6_layout_desc_bytes = desc_bytes;
  ctx->c_v6_layout_desc_owned = 1u;
  ctx->c_v6_bitmap_mapping_enabled = 1u;
  return 0;
}

static int kafs_v6_descriptor_mapping_admit_desc(
    struct kafs_context *ctx, void *desc, uint32_t desc_bytes, uint64_t file_size,
    kafs_v6_bitmap_coverage_report_t *out_bitmap, kafs_v6_inode_coverage_report_t *out_inode,
    kafs_v6_allocator_summary_coverage_report_t *out_alloc_summary,
    kafs_v6_hrl_index_coverage_report_t *out_hrl_index,
    kafs_v6_hrl_entries_coverage_report_t *out_hrl_entries)
{
  if (!ctx || !ctx->c_superblock || !desc || desc_bytes == 0u)
    return -EINVAL;
  if (kafs_sb_format_version_get(ctx->c_superblock) != KAFS_FORMAT_VERSION_V6)
    return -EPROTONOSUPPORT;

  kafs_v6_bitmap_coverage_report_t bitmap_report;
  int rc = kafs_v6_bitmap_validate_coverage(desc, desc_bytes, ctx->c_superblock, file_size,
                                            &bitmap_report);
  if (out_bitmap)
    *out_bitmap = bitmap_report;
  if (rc != 0)
    return rc;

  kafs_v6_inode_coverage_report_t inode_report;
  rc = kafs_v6_inode_validate_coverage(desc, desc_bytes, ctx->c_superblock, file_size,
                                       &inode_report);
  if (out_inode)
    *out_inode = inode_report;
  if (rc != 0)
    return rc;

  kafs_v6_allocator_summary_coverage_report_t alloc_summary_report;
  rc = kafs_v6_allocator_summary_validate_coverage(desc, desc_bytes, ctx->c_superblock, file_size,
                                                   &alloc_summary_report);
  if (out_alloc_summary)
    *out_alloc_summary = alloc_summary_report;
  if (rc != 0)
    return rc;

  kafs_v6_hrl_index_coverage_report_t hrl_index_report;
  rc = kafs_v6_hrl_index_validate_coverage(desc, desc_bytes, ctx->c_superblock, file_size,
                                           &hrl_index_report);
  if (out_hrl_index)
    *out_hrl_index = hrl_index_report;
  if (rc != 0)
    return rc;

  kafs_v6_hrl_entries_coverage_report_t hrl_entries_report;
  rc = kafs_v6_hrl_entries_validate_coverage(desc, desc_bytes, ctx->c_superblock, file_size,
                                             &hrl_entries_report);
  if (out_hrl_entries)
    *out_hrl_entries = hrl_entries_report;
  if (rc != 0)
    return rc;

  kafs_v6_journal_header_coverage_report_t journal_header_report;
  rc = kafs_v6_journal_header_validate_coverage(desc, desc_bytes, ctx->c_superblock, file_size,
                                                &journal_header_report);
  if (rc != 0)
    return rc;

  kafs_v6_journal_data_coverage_report_t journal_data_report;
  rc = kafs_v6_journal_data_validate_coverage(desc, desc_bytes, ctx->c_superblock, file_size,
                                              &journal_data_report);
  if (rc != 0)
    return rc;

  kafs_v6_inode_runtime_shard_t *inode_shards =
      calloc(inode_report.shard_count, sizeof(*inode_shards));
  if (!inode_shards)
    return -ENOMEM;

  uint32_t inode_shard_count = 0;
  rc = kafs_v6_inode_runtime_shards_build(desc, desc_bytes, file_size, inode_shards,
                                          inode_report.shard_count, &inode_shard_count);
  if (rc == 0 && inode_shard_count != inode_report.shard_count)
    rc = -EINVAL;
  if (rc != 0)
  {
    free(inode_shards);
    return rc;
  }

  kafs_v6_alloc_summary_runtime_shard_t *alloc_summary_shards =
      calloc(alloc_summary_report.shard_count, sizeof(*alloc_summary_shards));
  if (!alloc_summary_shards)
  {
    free(inode_shards);
    return -ENOMEM;
  }

  uint32_t alloc_summary_shard_count = 0;
  rc = kafs_v6_alloc_summary_runtime_shards_build(desc, desc_bytes, file_size, alloc_summary_shards,
                                                  alloc_summary_report.shard_count,
                                                  &alloc_summary_shard_count);
  if (rc == 0 && alloc_summary_shard_count != alloc_summary_report.shard_count)
    rc = -EINVAL;
  if (rc != 0)
  {
    free(alloc_summary_shards);
    free(inode_shards);
    return rc;
  }

  kafs_v6_hrl_runtime_shard_t *hrl_index_shards =
      calloc(hrl_index_report.shard_count, sizeof(*hrl_index_shards));
  if (!hrl_index_shards)
  {
    free(alloc_summary_shards);
    free(inode_shards);
    return -ENOMEM;
  }

  uint32_t hrl_index_shard_count = 0;
  rc = kafs_v6_hrl_runtime_shards_build(desc, desc_bytes, file_size, KAFS_META_REGION_HRL_INDEX,
                                        sizeof(uint32_t), hrl_index_shards,
                                        hrl_index_report.shard_count, &hrl_index_shard_count);
  if (rc == 0 && hrl_index_shard_count != hrl_index_report.shard_count)
    rc = -EINVAL;
  if (rc != 0)
  {
    free(hrl_index_shards);
    free(alloc_summary_shards);
    free(inode_shards);
    return rc;
  }

  kafs_v6_hrl_runtime_shard_t *hrl_entry_shards =
      calloc(hrl_entries_report.shard_count, sizeof(*hrl_entry_shards));
  if (!hrl_entry_shards)
  {
    free(hrl_index_shards);
    free(alloc_summary_shards);
    free(inode_shards);
    return -ENOMEM;
  }

  uint32_t hrl_entry_shard_count = 0;
  rc = kafs_v6_hrl_runtime_shards_build(desc, desc_bytes, file_size, KAFS_META_REGION_HRL_ENTRIES,
                                        sizeof(kafs_hrl_entry_t), hrl_entry_shards,
                                        hrl_entries_report.shard_count, &hrl_entry_shard_count);
  if (rc == 0 && hrl_entry_shard_count != hrl_entries_report.shard_count)
    rc = -EINVAL;
  if (rc != 0)
  {
    free(hrl_entry_shards);
    free(hrl_index_shards);
    free(alloc_summary_shards);
    free(inode_shards);
    return rc;
  }

  if (hrl_index_report.expected_count > UINT32_MAX)
  {
    free(hrl_entry_shards);
    free(hrl_index_shards);
    free(alloc_summary_shards);
    free(inode_shards);
    return -ERANGE;
  }

  kafs_bitmap_descriptor_mapping_clear(ctx);

  ctx->c_v6_layout_desc = desc;
  ctx->c_v6_layout_desc_bytes = desc_bytes;
  ctx->c_v6_layout_desc_owned = 1u;
  ctx->c_v6_bitmap_mapping_enabled = 1u;
  ctx->c_v6_inode_mapping_enabled = 1u;
  ctx->c_v6_alloc_summary_mapping_enabled = 1u;
  ctx->c_v6_hrl_mapping_enabled = 1u;
  ctx->c_v6_inode_shards = inode_shards;
  ctx->c_v6_inode_shard_count = inode_shard_count;
  ctx->c_v6_alloc_summary_shards = alloc_summary_shards;
  ctx->c_v6_alloc_summary_shard_count = alloc_summary_shard_count;
  ctx->c_v6_hrl_index_shards = hrl_index_shards;
  ctx->c_v6_hrl_index_shard_count = hrl_index_shard_count;
  ctx->c_v6_hrl_entry_shards = hrl_entry_shards;
  ctx->c_v6_hrl_entry_shard_count = hrl_entry_shard_count;
  ctx->c_hrl_bucket_cnt = (uint32_t)hrl_index_report.expected_count;
  return 0;
}

static int kafs_bitmap_descriptor_mapping_admit_fd(struct kafs_context *ctx, int fd,
                                                   uint64_t file_size,
                                                   kafs_v6_bitmap_coverage_report_t *out_report)
{
  if (!ctx || !ctx->c_superblock || fd < 0)
    return -EINVAL;
  if (kafs_sb_format_version_get(ctx->c_superblock) != KAFS_FORMAT_VERSION_V6)
    return -EPROTONOSUPPORT;

  kafs_bitmap_descriptor_mapping_clear(ctx);

  void *desc = NULL;
  uint32_t desc_bytes = 0;
  int rc = kafs_v6_descriptor_mapping_read_fd(ctx, fd, file_size, &desc, &desc_bytes);
  if (rc == 0)
    rc = kafs_bitmap_descriptor_mapping_admit_desc(ctx, desc, desc_bytes, file_size, out_report);
  if (rc != 0)
    free(desc);
  return rc;
}

static int
kafs_v6_descriptor_mapping_admit_fd(struct kafs_context *ctx, int fd, uint64_t file_size,
                                    kafs_v6_bitmap_coverage_report_t *out_bitmap,
                                    kafs_v6_inode_coverage_report_t *out_inode,
                                    kafs_v6_allocator_summary_coverage_report_t *out_alloc_summary,
                                    kafs_v6_hrl_index_coverage_report_t *out_hrl_index,
                                    kafs_v6_hrl_entries_coverage_report_t *out_hrl_entries)
{
  if (!ctx || !ctx->c_superblock || fd < 0)
    return -EINVAL;
  if (kafs_sb_format_version_get(ctx->c_superblock) != KAFS_FORMAT_VERSION_V6)
    return -EPROTONOSUPPORT;

  kafs_bitmap_descriptor_mapping_clear(ctx);

  void *desc = NULL;
  uint32_t desc_bytes = 0;
  int rc = kafs_v6_descriptor_mapping_read_fd(ctx, fd, file_size, &desc, &desc_bytes);
  if (rc == 0)
    rc = kafs_v6_descriptor_mapping_admit_desc(ctx, desc, desc_bytes, file_size, out_bitmap,
                                               out_inode, out_alloc_summary, out_hrl_index,
                                               out_hrl_entries);
  if (rc != 0)
    free(desc);
  return rc;
}

static int kafs_bitmap_word_ref_from_contiguous(struct kafs_context *ctx, kafs_blkcnt_t blo,
                                                kafs_bitmap_word_ref_t *ref)
{
  if (!ctx || !ctx->c_superblock || !ref)
    return -EINVAL;

  kafs_ssuperblock_t *sb = ctx->c_superblock;
  assert(blo < kafs_sb_blkcnt_get(sb));

  kafs_blkcnt_t blod = blo >> KAFS_BLKMASK_LOG_BITS;
  kafs_blkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS;
  kafs_blkmask_t *blkmasktbl = kafs_meta_bitmap_tbl_mut(ctx);
  if (!blkmasktbl)
    return -EINVAL;

  memset(ref, 0, sizeof(*ref));
  ref->sb = sb;
  ref->dirty_word_index = blod;
  ref->word_logical_start = blod << KAFS_BLKMASK_LOG_BITS;
  ref->bit = (kafs_blkmask_t)1 << blor;
  ref->word_ptr = &blkmasktbl[blod];
  ref->word = *ref->word_ptr;
  ref->meta_overlay = (blkmasktbl != ctx->c_blkmasktbl);
  ref->count_block_bitmap_write = (blkmasktbl == ctx->c_blkmasktbl);
  return 0;
}

static int kafs_bitmap_word_ref_from_v6_descriptor(struct kafs_context *ctx, kafs_blkcnt_t blo,
                                                   kafs_bitmap_word_ref_t *ref)
{
  if (!ctx || !ctx->c_superblock || !ref)
    return -EINVAL;

  kafs_v6_bitmap_lookup_t lookup;
  int rc = kafs_v6_bitmap_lookup(ctx->c_v6_layout_desc, ctx->c_v6_layout_desc_bytes, (uint64_t)blo,
                                 &lookup);
  if (rc != 0)
    return rc;

  uint64_t bit_delta = (uint64_t)blo - lookup.logical_start;
  uint64_t byte_delta = bit_delta >> 3;
  uint64_t word_back = byte_delta & (uint64_t)(sizeof(kafs_blkmask_t) - 1u);
  if (lookup.bitmap_byte_off < word_back)
    return -ERANGE;

  uint64_t word_off = lookup.bitmap_byte_off - word_back;
  if (word_off > (uint64_t)ctx->c_img_size ||
      (uint64_t)sizeof(kafs_blkmask_t) > (uint64_t)ctx->c_img_size - word_off)
    return -ERANGE;
  if ((word_off & (uint64_t)(sizeof(kafs_blkmask_t) - 1u)) != 0u)
    return -ERANGE;

  memset(ref, 0, sizeof(*ref));
  ref->sb = ctx->c_superblock;
  ref->dirty_word_index = (kafs_blkcnt_t)(bit_delta >> KAFS_BLKMASK_LOG_BITS);
  ref->word_logical_start =
      (kafs_blkcnt_t)(lookup.logical_start + (bit_delta & ~(uint64_t)KAFS_BLKMASK_MASK_BITS));
  ref->bit = (kafs_blkmask_t)1 << (bit_delta & KAFS_BLKMASK_MASK_BITS);
  ref->word_ptr = (kafs_blkmask_t *)((uint8_t *)ctx->c_img_base + word_off);
  ref->word = *ref->word_ptr;
  ref->meta_overlay = KAFS_FALSE;
  ref->count_block_bitmap_write = KAFS_TRUE;
  return 0;
}

static int kafs_bitmap_word_ref_init(struct kafs_context *ctx, kafs_blkcnt_t blo,
                                     kafs_bitmap_word_ref_t *ref)
{
  if (!ctx || !ctx->c_superblock || !ref)
    return -EINVAL;
  assert(blo < kafs_sb_blkcnt_get(ctx->c_superblock));

  if (kafs_bitmap_descriptor_mapping_enabled(ctx))
    return kafs_bitmap_word_ref_from_v6_descriptor(ctx, blo, ref);
  return kafs_bitmap_word_ref_from_contiguous(ctx, blo, ref);
}

/// @brief 指定されたブロック番号の利用状況を取得する
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @return 0: 未使用, != 0: 使用中
static int kafs_blk_get_usage(const struct kafs_context *ctx, kafs_blkcnt_t blo)
{
  assert(ctx != NULL);
  assert(blo < kafs_sb_blkcnt_get(ctx->c_superblock));
  kafs_bitmap_word_ref_t ref;
  int rc = kafs_bitmap_word_ref_init((struct kafs_context *)ctx, blo, &ref);
  assert(rc == 0);
  if (rc != 0)
    return KAFS_FALSE;
  int ret = (ref.word & ref.bit) != 0;
  kafs_log(KAFS_LOG_DEBUG, "%s(blo=%" PRIuFAST32 ") returns %s\n", __func__, blo,
           ret ? "true" : "false");
  return ret;
}

// MTセーフに読みたい場合のラッパ（ビットマップロックを取得）
__attribute_maybe_unused__ static int kafs_blk_get_usage_locked(struct kafs_context *ctx,
                                                                kafs_blkcnt_t blo)
{
  kafs_bitmap_lock(ctx);
  int ret = kafs_blk_get_usage((const struct kafs_context *)ctx, blo);
  kafs_bitmap_unlock(ctx);
  return ret;
}

typedef struct kafs_alloc_v3_summary_view
{
  uint64_t logical_start;
  uint64_t logical_count;
  uint8_t *l1;
  uint8_t *l2;
  const uint8_t *l0;
  size_t l0_bytes;
  size_t l1_bytes;
  size_t l2_bytes;
  int descriptor_backed;
} kafs_alloc_v3_summary_view_t;

static int kafs_alloc_v3_summary_contiguous_enabled(const struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_superblock)
    return KAFS_FALSE;
  if (kafs_sb_format_version_get(ctx->c_superblock) != KAFS_FORMAT_VERSION)
    return KAFS_FALSE;
  if (kafs_sb_allocator_version_get(ctx->c_superblock) < 2u)
    return KAFS_FALSE;
  if ((kafs_sb_feature_flags_get(ctx->c_superblock) & KAFS_FEATURE_ALLOC_V2) == 0)
    return KAFS_FALSE;
  if (kafs_sb_allocator_size_get(ctx->c_superblock) == 0u)
    return KAFS_FALSE;
  return KAFS_TRUE;
}

static int kafs_alloc_v3_summary_descriptor_enabled(const struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_superblock)
    return KAFS_FALSE;
  if (kafs_sb_format_version_get(ctx->c_superblock) != KAFS_FORMAT_VERSION_V6)
    return KAFS_FALSE;
  if (!ctx->c_v6_alloc_summary_mapping_enabled || !ctx->c_v6_alloc_summary_shards ||
      ctx->c_v6_alloc_summary_shard_count == 0u)
    return KAFS_FALSE;
  if (!ctx->c_img_base || ctx->c_img_size == 0u)
    return KAFS_FALSE;
  return KAFS_TRUE;
}

static int kafs_alloc_v3_summary_enabled(const struct kafs_context *ctx)
{
  return kafs_alloc_v3_summary_contiguous_enabled(ctx) ||
         kafs_alloc_v3_summary_descriptor_enabled(ctx);
}

static int kafs_alloc_v3_summary_view_contiguous(struct kafs_context *ctx,
                                                 kafs_alloc_v3_summary_view_t *view)
{
  if (!ctx || !ctx->c_superblock || !view)
    return -EINVAL;
  if (!kafs_alloc_v3_summary_contiguous_enabled(ctx))
    return -ENOENT;

  kafs_blkcnt_t blocnt = kafs_sb_blkcnt_get(ctx->c_superblock);
  uint64_t l0_bytes_u64 = ((uint64_t)blocnt + 7u) >> 3;
  uint64_t l1_bytes_u64 = (l0_bytes_u64 + 7u) >> 3;
  uint64_t l2_bytes_u64 = (l1_bytes_u64 + 7u) >> 3;
  uint64_t off = kafs_sb_allocator_offset_get(ctx->c_superblock);
  uint64_t sz = kafs_sb_allocator_size_get(ctx->c_superblock);
  uint64_t need = l1_bytes_u64 + l2_bytes_u64;
  if (l0_bytes_u64 > SIZE_MAX || l1_bytes_u64 > SIZE_MAX || l2_bytes_u64 > SIZE_MAX)
    return -ERANGE;
  if (sz < need)
    return -EINVAL;
  if (off > (uint64_t)ctx->c_mapsize || need > (uint64_t)ctx->c_mapsize - off)
    return -EINVAL;

  const uint8_t *l0 = (const uint8_t *)kafs_meta_bitmap_tbl_const(ctx);
  if (!l0)
    return -EINVAL;

  memset(view, 0, sizeof(*view));
  view->logical_start = 0;
  view->logical_count = blocnt;
  view->l1 = (uint8_t *)ctx->c_superblock + (size_t)off;
  view->l2 = view->l1 + (size_t)l1_bytes_u64;
  view->l0 = l0;
  view->l0_bytes = (size_t)l0_bytes_u64;
  view->l1_bytes = (size_t)l1_bytes_u64;
  view->l2_bytes = (size_t)l2_bytes_u64;
  view->descriptor_backed = KAFS_FALSE;
  return 0;
}

static int
kafs_alloc_v3_summary_view_from_v6_shard(struct kafs_context *ctx,
                                         const kafs_v6_alloc_summary_runtime_shard_t *shard,
                                         kafs_alloc_v3_summary_view_t *view)
{
  if (!ctx || !shard || !view)
    return -EINVAL;
  if (shard->l0_bytes > SIZE_MAX || shard->l1_bytes > SIZE_MAX || shard->l2_bytes > SIZE_MAX)
    return -ERANGE;
  if (shard->physical_off > (uint64_t)ctx->c_img_size ||
      shard->physical_end > (uint64_t)ctx->c_img_size || shard->physical_end < shard->physical_off)
    return -ERANGE;
  if (shard->l1_bytes > UINT64_MAX - shard->l2_bytes ||
      shard->physical_off > UINT64_MAX - shard->l1_bytes - shard->l2_bytes)
    return -ERANGE;

  memset(view, 0, sizeof(*view));
  view->logical_start = shard->logical_start;
  view->logical_count = shard->logical_count;
  view->l1 = (uint8_t *)ctx->c_img_base + (size_t)shard->physical_off;
  view->l2 = view->l1 + (size_t)shard->l1_bytes;
  view->l0 = NULL;
  view->l0_bytes = (size_t)shard->l0_bytes;
  view->l1_bytes = (size_t)shard->l1_bytes;
  view->l2_bytes = (size_t)shard->l2_bytes;
  view->descriptor_backed = KAFS_TRUE;
  return 0;
}

static int kafs_alloc_v3_summary_view_for_blo(struct kafs_context *ctx, kafs_blkcnt_t blo,
                                              kafs_alloc_v3_summary_view_t *view)
{
  if (!ctx || !view)
    return -EINVAL;
  if (kafs_alloc_v3_summary_descriptor_enabled(ctx))
  {
    uint64_t blo64 = (uint64_t)blo;
    for (uint32_t i = 0; i < ctx->c_v6_alloc_summary_shard_count; ++i)
    {
      const kafs_v6_alloc_summary_runtime_shard_t *shard = &ctx->c_v6_alloc_summary_shards[i];
      uint64_t logical_end = shard->logical_start + shard->logical_count;
      if (blo64 < shard->logical_start || blo64 >= logical_end)
        continue;
      return kafs_alloc_v3_summary_view_from_v6_shard(ctx, shard, view);
    }
    return -ENOENT;
  }
  return kafs_alloc_v3_summary_view_contiguous(ctx, view);
}

static int kafs_alloc_v3_summary_l0_byte(struct kafs_context *ctx,
                                         const kafs_alloc_v3_summary_view_t *view, size_t l0_idx,
                                         uint8_t *out_byte)
{
  if (!ctx || !view || !out_byte || l0_idx >= view->l0_bytes)
    return -EINVAL;
  if (!view->descriptor_backed)
  {
    *out_byte = view->l0[l0_idx];
    return 0;
  }

  if ((uint64_t)l0_idx > UINT64_MAX / 8u ||
      view->logical_start > UINT64_MAX - ((uint64_t)l0_idx * 8u))
    return -ERANGE;
  uint64_t first_blo = view->logical_start + ((uint64_t)l0_idx * 8u);
  uint64_t covered = (uint64_t)l0_idx * 8u;
  uint64_t valid_bits = (covered < view->logical_count) ? (view->logical_count - covered) : 0u;
  if (valid_bits > 8u)
    valid_bits = 8u;

  uint8_t byte = 0xFFu;
  kafs_blkcnt_t blocnt = kafs_sb_blkcnt_get(ctx->c_superblock);
  for (uint64_t bit = 0; bit < valid_bits; ++bit)
  {
    if (first_blo > UINT64_MAX - bit)
      return -ERANGE;
    uint64_t blo = first_blo + bit;
    if (blo >= (uint64_t)blocnt)
      continue;
    if (!kafs_blk_get_usage(ctx, (kafs_blkcnt_t)blo))
      byte &= (uint8_t) ~(1u << bit);
  }
  *out_byte = byte;
  return 0;
}

static int kafs_alloc_v3_summary_sync_one(struct kafs_context *ctx, kafs_blkcnt_t blo)
{
  if (!ctx || !ctx->c_superblock)
    return -EINVAL;
  if (!kafs_alloc_v3_summary_enabled(ctx))
    return 0;
  if (ctx->c_alloc_v3_summary_dirty)
    return 0;

  kafs_alloc_v3_summary_view_t view;
  int rc = kafs_alloc_v3_summary_view_for_blo(ctx, blo, &view);
  if (rc != 0)
    return rc;
  if ((uint64_t)blo < view.logical_start ||
      (uint64_t)blo >= view.logical_start + view.logical_count)
    return -ERANGE;

  uint64_t local_blo = (uint64_t)blo - view.logical_start;
  uint64_t l0_idx_u64 = local_blo >> 3;
  uint64_t l1_idx_u64 = l0_idx_u64 >> 3;
  uint64_t l2_idx_u64 = l1_idx_u64 >> 3;
  if (l0_idx_u64 > SIZE_MAX || l1_idx_u64 > SIZE_MAX || l2_idx_u64 > SIZE_MAX)
    return -ERANGE;
  size_t l0_idx = (size_t)l0_idx_u64;
  size_t l1_idx = (size_t)l1_idx_u64;
  size_t l2_idx = (size_t)l2_idx_u64;
  if (l0_idx >= view.l0_bytes)
    return -EINVAL;
  if (l1_idx >= view.l1_bytes)
    return -EINVAL;
  if (l2_idx >= view.l2_bytes)
    return -EINVAL;

  uint8_t l0_byte = 0;
  rc = kafs_alloc_v3_summary_l0_byte(ctx, &view, l0_idx, &l0_byte);
  if (rc != 0)
    return rc;
  uint8_t l1_mask = (uint8_t)(1u << (l0_idx & 7u));
  if (l0_byte == 0xFFu)
    view.l1[l1_idx] &= (uint8_t)~l1_mask;
  else
    view.l1[l1_idx] |= l1_mask;

  uint8_t l2_mask = (uint8_t)(1u << (l1_idx & 7u));
  if (view.l1[l1_idx] == 0u)
    view.l2[l2_idx] &= (uint8_t)~l2_mask;
  else
    view.l2[l2_idx] |= l2_mask;

  kafs_ctx_meta_write_count(ctx, KAFS_META_REGION_ALLOCATOR_SUMMARY, 2u);
  return 0;
}

static void kafs_blk_account_bit_update(struct kafs_context *ctx, const kafs_bitmap_word_ref_t *ref,
                                        kafs_blkmask_t word)
{
  uint64_t t_bit0 = kafs_blk_now_ns();
  *ref->word_ptr = word;
  if (ref->meta_overlay)
    kafs_meta_bitmap_mark_dirty(ctx, ref->dirty_word_index);
  if (ref->count_block_bitmap_write)
    kafs_ctx_meta_write_count(ctx, KAFS_META_REGION_BLOCK_BITMAP, sizeof(*ref->word_ptr));
  uint64_t t_bit1 = kafs_blk_now_ns();
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_bit_update, t_bit1 - t_bit0, __ATOMIC_RELAXED);
}

static void kafs_blk_account_meta_update(struct kafs_context *ctx, kafs_ssuperblock_t *sb,
                                         kafs_blkcnt_t blo, int free_delta)
{
  uint64_t t_free0 = kafs_blk_now_ns();
  if (ctx->c_meta_delta_enabled)
  {
    ctx->c_meta_delta_free_blocks += free_delta;
  }
  else
  {
    kafs_blkcnt_t blkcnt_free = kafs_sb_blkcnt_free_get(sb);
    if (free_delta < 0)
    {
      if (blkcnt_free > 0)
        kafs_sb_blkcnt_free_set(sb, blkcnt_free - 1);
    }
    else if (free_delta > 0)
    {
      kafs_sb_blkcnt_free_set(sb, blkcnt_free + 1);
    }
    kafs_ctx_meta_write_count(ctx, KAFS_META_REGION_SUPERBLOCK_CHECKPOINT,
                              sizeof(sb->s_blkcnt_free));
  }
  uint64_t t_free1 = kafs_blk_now_ns();
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_freecnt_update, t_free1 - t_free0,
                     __ATOMIC_RELAXED);

  uint64_t t_wtime0 = kafs_blk_now_ns();
  if (ctx->c_meta_delta_enabled)
  {
    ctx->c_meta_delta_wtime_dirty = 1u;
  }
  else
  {
    kafs_sb_wtime_set(sb, kafs_now());
    kafs_ctx_meta_write_count(ctx, KAFS_META_REGION_SUPERBLOCK_CHECKPOINT, sizeof(sb->s_wtime));
  }
  uint64_t t_wtime1 = kafs_blk_now_ns();
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_wtime_update, t_wtime1 - t_wtime0,
                     __ATOMIC_RELAXED);

  if (kafs_alloc_v3_summary_sync_one(ctx, blo) < 0)
    ctx->c_alloc_v3_summary_dirty = 1u;
}

static int kafs_blk_load_word(struct kafs_context *ctx, kafs_blkcnt_t blo,
                              kafs_bitmap_word_ref_t *out_ref)
{
  return kafs_bitmap_word_ref_init(ctx, blo, out_ref);
}

/// @brief 指定されたブロックの利用状況をセットする
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @param usage KAFS_FALSE: フラグをクリア, KAFS_TRUE: フラグをセット
/// @return 0: 成功, < 0: 失敗 (-errno)
// Internal helper without taking bitmap lock (callers must hold bitmap lock)
static int kafs_blk_set_usage_nolock(struct kafs_context *ctx, kafs_blkcnt_t blo, kafs_bool_t usage)
{
  kafs_log(KAFS_LOG_DEBUG, "%s(blo=%" PRIuFAST32 ", usage=%s)\n", __func__, blo,
           usage ? "true" : "false");
  kafs_bitmap_word_ref_t ref;
  int rc = kafs_blk_load_word(ctx, blo, &ref);
  if (rc != 0)
    return rc;

  kafs_blkmask_t word = ref.word;
  int was_used = (word & ref.bit) != 0;
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_calls, 1u, __ATOMIC_RELAXED);
  if (usage == KAFS_TRUE)
  {
    __atomic_add_fetch(&ctx->c_stat_blk_set_usage_alloc_calls, 1u, __ATOMIC_RELAXED);
    if (!was_used)
    {
      word |= ref.bit;
      kafs_blk_account_bit_update(ctx, &ref, word);
      kafs_blk_account_meta_update(ctx, ref.sb, blo, -1);
    }
  }
  else
  {
    __atomic_add_fetch(&ctx->c_stat_blk_set_usage_free_calls, 1u, __ATOMIC_RELAXED);
    if (was_used)
    {
      word &= (kafs_blkmask_t)~ref.bit;
      kafs_blk_account_bit_update(ctx, &ref, word);
      kafs_blk_account_meta_update(ctx, ref.sb, blo, +1);

      if (ctx->c_trim_on_free)
      {
        kafs_blkcnt_t fdb = kafs_sb_first_data_block_get(ref.sb);
        if (blo >= fdb)
        {
#ifdef __linux__
#ifdef SYS_fallocate
          off_t off = (off_t)blo << kafs_sb_log_blksize_get(ref.sb);
          off_t len = (off_t)kafs_sb_blksize_get(ref.sb);
          if (syscall(SYS_fallocate, ctx->c_fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, off,
                      len) == 0)
          {
            __atomic_add_fetch(&ctx->c_stat_trim_issued, 1u, __ATOMIC_RELAXED);
          }
          else
          {
            int e = errno;
            __atomic_add_fetch(&ctx->c_stat_trim_failed, 1u, __ATOMIC_RELAXED);
            // EOPNOTSUPP/ENOSYS are expected on some backing filesystems.
            kafs_log(KAFS_LOG_DEBUG,
                     "%s: trim failed blo=%" PRIuFAST32 " off=%" PRIuFAST64 " len=%" PRIuFAST64
                     " errno=%d\n",
                     __func__, blo, (uint64_t)off, (uint64_t)len, e);
          }
#else
          (void)ctx;
#endif
#endif
        }
      }
    }
  }
  return KAFS_SUCCESS;
}

// Fast claim helper for allocation path (caller must hold bitmap lock).
// Returns 1 when claimed, 0 when already used.
static int kafs_blk_try_claim_nolock(struct kafs_context *ctx, kafs_blkcnt_t blo)
{
  kafs_bitmap_word_ref_t ref;
  int rc = kafs_blk_load_word(ctx, blo, &ref);
  if (rc != 0)
    return rc;

  kafs_blkmask_t word = ref.word;
  if ((word & ref.bit) != 0)
    return 0;

  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_calls, 1u, __ATOMIC_RELAXED);
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_alloc_calls, 1u, __ATOMIC_RELAXED);

  word |= ref.bit;
  kafs_blk_account_bit_update(ctx, &ref, word);
  kafs_blk_account_meta_update(ctx, ref.sb, blo, -1);

  return 1;
}

static int kafs_blk_claim_candidate(struct kafs_context *ctx, kafs_blkcnt_t candidate,
                                    kafs_blkcnt_t *pblo)
{
  uint64_t t_claim0 = kafs_blk_now_ns();
  kafs_bitmap_lock(ctx);
  uint64_t t_set0 = kafs_blk_now_ns();
  int claimed = kafs_blk_try_claim_nolock(ctx, candidate);
  uint64_t t_set1 = kafs_blk_now_ns();
  __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_set_usage, t_set1 - t_set0, __ATOMIC_RELAXED);
  if (claimed < 0)
  {
    kafs_bitmap_unlock(ctx);
    uint64_t t_claim1 = kafs_blk_now_ns();
    __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_claim, t_claim1 - t_claim0, __ATOMIC_RELAXED);
    return claimed;
  }
  if (claimed > 0)
  {
    ctx->c_blo_search = candidate;
    *pblo = candidate;
    kafs_bitmap_unlock(ctx);
    uint64_t t_claim1 = kafs_blk_now_ns();
    __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_claim, t_claim1 - t_claim0, __ATOMIC_RELAXED);
    return 1;
  }
  kafs_bitmap_unlock(ctx);
  uint64_t t_claim1 = kafs_blk_now_ns();
  __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_claim, t_claim1 - t_claim0, __ATOMIC_RELAXED);
  __atomic_add_fetch(&ctx->c_stat_blk_alloc_claim_retries, 1u, __ATOMIC_RELAXED);
  return 0;
}

// Public wrapper that takes bitmap lock
__attribute_maybe_unused__ static int kafs_blk_set_usage(struct kafs_context *ctx,
                                                         kafs_blkcnt_t blo, kafs_bool_t usage)
{
  kafs_bitmap_lock(ctx);
  int rc = kafs_blk_set_usage_nolock(ctx, blo, usage);
  kafs_bitmap_unlock(ctx);
  return rc;
}

/// @brief v3 allocator backend を使用する条件を満たすか判定する
/// @param ctx コンテキスト
/// @return KAFS_TRUE: v3 backend, KAFS_FALSE: legacy backend
static int kafs_blk_alloc_backend_is_v3(const struct kafs_context *ctx)
{
  return kafs_alloc_v3_summary_enabled(ctx);
}

static int kafs_blk_alloc_prepare(struct kafs_context *ctx, kafs_blkcnt_t *pblo,
                                  kafs_blkcnt_t *out_blocnt, kafs_blkcnt_t *out_fdb)
{
  assert(ctx != NULL);
  assert(pblo != NULL);
  assert(*pblo == KAFS_BLO_NONE);

  __atomic_add_fetch(&ctx->c_stat_blk_alloc_calls, 1u, __ATOMIC_RELAXED);

  *out_blocnt = kafs_sb_blkcnt_get(ctx->c_superblock);
  *out_fdb = kafs_sb_first_data_block_get(ctx->c_superblock);
  if (*out_fdb >= *out_blocnt)
    return -ENOSPC;
  return 0;
}

/// @brief 未使用のブロック番号を取得し、使用中フラグをつける（legacy）
/// @param ctx コンテキスト
/// @param pblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int kafs_blk_alloc_legacy(struct kafs_context *ctx, kafs_blkcnt_t *pblo)
{
  kafs_blkcnt_t blocnt = 0;
  kafs_blkcnt_t fdb = 0;
  int prepare_rc = kafs_blk_alloc_prepare(ctx, pblo, &blocnt, &fdb);
  if (prepare_rc != 0)
    return prepare_rc;

  kafs_blkcnt_t blo_search = ctx->c_blo_search;
  if (blo_search < fdb)
    blo_search = fdb;
  kafs_blkcnt_t blo = blo_search + 1;
  if (blo < fdb)
    blo = fdb;

  uint64_t t_scan_start = kafs_blk_now_ns();

  while (blo_search != blo)
  {
    if (blo >= blocnt)
      blo = fdb;

    kafs_bitmap_word_ref_t scan_ref;
    int rc = kafs_blk_load_word(ctx, blo, &scan_ref);
    if (rc != 0)
      return rc;
    kafs_blkcnt_t blor = blo - scan_ref.word_logical_start;
    kafs_blkmask_t blkmask = ~scan_ref.word;

    if (fdb > scan_ref.word_logical_start && fdb < scan_ref.word_logical_start + KAFS_BLKMASK_BITS)
    {
      kafs_blkcnt_t fdb_r = fdb - scan_ref.word_logical_start;
      blkmask &= (kafs_blkmask_t)(~(((kafs_blkmask_t)1u << fdb_r) - 1u));
    }

    if (blkmask != 0)
    {
      kafs_blkcnt_t blor_found = kafs_get_free_blkmask(blkmask);
      kafs_blkcnt_t blo_found = scan_ref.word_logical_start + blor_found;
      if (blo_found >= fdb && blo_found < blocnt)
      {
        uint64_t t_scan_stop = kafs_blk_now_ns();
        __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_scan, t_scan_stop - t_scan_start,
                           __ATOMIC_RELAXED);

        int claim_rc = kafs_blk_claim_candidate(ctx, blo_found, pblo);
        if (claim_rc < 0)
          return claim_rc;
        if (claim_rc > 0)
          return KAFS_SUCCESS;
        t_scan_start = kafs_blk_now_ns();
      }
    }
    blo += KAFS_BLKMASK_BITS - blor;
  }

  uint64_t t_scan_end = kafs_blk_now_ns();
  __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_scan, t_scan_end - t_scan_start, __ATOMIC_RELAXED);

  return -ENOSPC;
}

static int kafs_alloc_v3_rebuild_summary_view(struct kafs_context *ctx,
                                              kafs_alloc_v3_summary_view_t *view)
{
  if (!ctx || !view)
    return -EINVAL;

  memset(view->l1, 0, view->l1_bytes);
  memset(view->l2, 0, view->l2_bytes);
  for (size_t i = 0; i < view->l0_bytes; ++i)
  {
    uint8_t l0_byte = 0;
    int rc = kafs_alloc_v3_summary_l0_byte(ctx, view, i, &l0_byte);
    if (rc != 0)
      return rc;
    if (l0_byte != 0xFFu)
      view->l1[i >> 3] |= (uint8_t)(1u << (i & 7u));
  }
  for (size_t i = 0; i < view->l1_bytes; ++i)
  {
    if (view->l1[i] != 0u)
      view->l2[i >> 3] |= (uint8_t)(1u << (i & 7u));
  }
  return 0;
}

static int kafs_alloc_v3_rebuild_summary_if_dirty(struct kafs_context *ctx)
{
  if (!ctx)
    return -EINVAL;
  if (!ctx->c_alloc_v3_summary_dirty)
    return 0;

  if (kafs_alloc_v3_summary_descriptor_enabled(ctx))
  {
    for (uint32_t i = 0; i < ctx->c_v6_alloc_summary_shard_count; ++i)
    {
      kafs_alloc_v3_summary_view_t view;
      int rc =
          kafs_alloc_v3_summary_view_from_v6_shard(ctx, &ctx->c_v6_alloc_summary_shards[i], &view);
      if (rc != 0)
        return rc;
      rc = kafs_alloc_v3_rebuild_summary_view(ctx, &view);
      if (rc != 0)
        return rc;
    }
  }
  else
  {
    kafs_alloc_v3_summary_view_t view;
    int rc = kafs_alloc_v3_summary_view_contiguous(ctx, &view);
    if (rc != 0)
      return rc;
    rc = kafs_alloc_v3_rebuild_summary_view(ctx, &view);
    if (rc != 0)
      return rc;
  }

  ctx->c_alloc_v3_summary_dirty = 0u;
  return 0;
}

static int kafs_alloc_v3_find_in_view(struct kafs_context *ctx,
                                      const kafs_alloc_v3_summary_view_t *view, kafs_blkcnt_t start,
                                      kafs_blkcnt_t end, kafs_blkcnt_t *out_blo)
{
  if (!ctx || !view || !out_blo || start > end)
    return 0;
  if ((uint64_t)start < view->logical_start ||
      (uint64_t)end >= view->logical_start + view->logical_count)
    return 0;

  uint64_t local_start = (uint64_t)start - view->logical_start;
  uint64_t local_end = (uint64_t)end - view->logical_start;
  uint64_t l0_start_byte_u64 = local_start >> 3;
  uint64_t l0_end_byte_u64 = local_end >> 3;
  uint64_t l1_start_u64 = l0_start_byte_u64 >> 3;
  uint64_t l1_end_u64 = l0_end_byte_u64 >> 3;
  if (l0_start_byte_u64 > SIZE_MAX || l0_end_byte_u64 > SIZE_MAX || l1_start_u64 > SIZE_MAX ||
      l1_end_u64 > SIZE_MAX)
    return 0;
  size_t l0_start_byte = (size_t)l0_start_byte_u64;
  size_t l0_end_byte = (size_t)l0_end_byte_u64;
  size_t l1_start = (size_t)l1_start_u64;
  size_t l1_end = (size_t)l1_end_u64;

  for (size_t l1_idx = l1_start; l1_idx <= l1_end && l1_idx < view->l1_bytes; ++l1_idx)
  {
    size_t l2_idx = l1_idx >> 3;
    if (l2_idx >= view->l2_bytes)
      continue;
    if ((view->l2[l2_idx] & (uint8_t)(1u << (l1_idx & 7u))) == 0)
      continue;

    uint8_t l1_bits = view->l1[l1_idx];
    if (l1_bits == 0u)
      continue;

    size_t l0_min = l1_idx << 3;
    size_t l0_max = l0_min + 7u;
    if (l0_min < l0_start_byte)
      l1_bits &= (uint8_t)(0xFFu << (l0_start_byte - l0_min));
    if (l0_max > l0_end_byte)
    {
      size_t keep = l0_end_byte - l0_min + 1u;
      if (keep < 8u)
        l1_bits &= (uint8_t)((1u << keep) - 1u);
    }
    if (l1_bits == 0u)
      continue;

    while (l1_bits)
    {
      unsigned b = (unsigned)__builtin_ctz((unsigned)l1_bits);
      l1_bits &= (uint8_t)(l1_bits - 1u);

      size_t l0_idx = l0_min + b;
      if (l0_idx >= view->l0_bytes)
        continue;

      uint8_t l0_byte = 0;
      int rc = kafs_alloc_v3_summary_l0_byte(ctx, view, l0_idx, &l0_byte);
      if (rc != 0)
        return 0;
      uint8_t free_bits = (uint8_t)~l0_byte;
      if (l0_idx == l0_start_byte)
        free_bits &= (uint8_t)(0xFFu << ((size_t)(local_start & 7u)));
      if (l0_idx == l0_end_byte)
      {
        size_t end_bit = (size_t)(local_end & 7u);
        if (end_bit < 7u)
          free_bits &= (uint8_t)((1u << (end_bit + 1u)) - 1u);
      }
      if (free_bits == 0u)
        continue;

      unsigned bit = (unsigned)__builtin_ctz((unsigned)free_bits);
      uint64_t blo64 = view->logical_start + (((uint64_t)l0_idx << 3) + (uint64_t)bit);
      if (blo64 > UINT32_MAX)
        continue;
      kafs_blkcnt_t blo = (kafs_blkcnt_t)blo64;
      if (blo < start || blo > end)
        continue;
      *out_blo = blo;
      return 1;
    }
  }
  return 0;
}

static int kafs_alloc_v3_find_in_range(struct kafs_context *ctx, kafs_blkcnt_t start,
                                       kafs_blkcnt_t end, kafs_blkcnt_t *out_blo)
{
  if (!ctx || !out_blo || start > end)
    return 0;

  kafs_blkcnt_t cursor = start;
  while (cursor <= end)
  {
    kafs_alloc_v3_summary_view_t view;
    if (kafs_alloc_v3_summary_view_for_blo(ctx, cursor, &view) != 0)
      return 0;
    uint64_t view_last = view.logical_start + view.logical_count - 1u;
    kafs_blkcnt_t segment_end = end;
    if (view_last < (uint64_t)segment_end)
      segment_end = (kafs_blkcnt_t)view_last;
    if (kafs_alloc_v3_find_in_view(ctx, &view, cursor, segment_end, out_blo))
      return 1;
    if (segment_end == UINT32_MAX)
      break;
    cursor = segment_end + 1u;
  }
  return 0;
}

static int kafs_blk_alloc_v3(struct kafs_context *ctx, kafs_blkcnt_t *pblo)
{
  kafs_blkcnt_t blocnt = 0;
  kafs_blkcnt_t fdb = 0;
  int prepare_rc = kafs_blk_alloc_prepare(ctx, pblo, &blocnt, &fdb);
  if (prepare_rc != 0)
    return prepare_rc;

  kafs_blkcnt_t search_start = ctx->c_blo_search + 1;
  if (search_start < fdb || search_start >= blocnt)
    search_start = fdb;

  uint64_t t_scan_start = kafs_blk_now_ns();
  int rebuilt_after_miss = 0;
  for (;;)
  {
    if (kafs_alloc_v3_rebuild_summary_if_dirty(ctx) < 0)
      return kafs_blk_alloc_legacy(ctx, pblo);

    kafs_blkcnt_t candidate = KAFS_BLO_NONE;
    int found = kafs_alloc_v3_find_in_range(ctx, search_start, blocnt - 1, &candidate);
    if (!found && search_start > fdb)
      found = kafs_alloc_v3_find_in_range(ctx, fdb, search_start - 1, &candidate);
    if (!found && !rebuilt_after_miss)
    {
      ctx->c_alloc_v3_summary_dirty = 1u;
      rebuilt_after_miss = 1;
      continue;
    }
    if (!found)
    {
      uint64_t t_scan_end = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_scan, t_scan_end - t_scan_start,
                         __ATOMIC_RELAXED);
      return -ENOSPC;
    }

    uint64_t t_scan_stop = kafs_blk_now_ns();
    __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_scan, t_scan_stop - t_scan_start,
                       __ATOMIC_RELAXED);

    int claim_rc = kafs_blk_claim_candidate(ctx, candidate, pblo);
    if (claim_rc < 0)
      return claim_rc;
    if (claim_rc > 0)
      return KAFS_SUCCESS;

    search_start = candidate + 1;
    if (search_start < fdb || search_start >= blocnt)
      search_start = fdb;
    t_scan_start = kafs_blk_now_ns();
    rebuilt_after_miss = 0;
  }
}

static int kafs_blk_alloc(struct kafs_context *ctx, kafs_blkcnt_t *pblo)
{
  if (kafs_blk_alloc_backend_is_v3(ctx))
    return kafs_blk_alloc_v3(ctx, pblo);
  return kafs_blk_alloc_legacy(ctx, pblo);
}
