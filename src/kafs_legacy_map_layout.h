#pragma once

#include "kafs_hash.h"
#include "kafs_inode.h"
#include "kafs_portability.h"
#include "kafs_superblock.h"

typedef struct kafs_legacy_map_layout
{
  size_t metadata_size;
  size_t image_size;
  size_t block_bitmap_offset;
  size_t inode_table_offset;
} kafs_legacy_map_layout_t;

static inline int kafs_legacy_map_layout_compute(const kafs_ssuperblock_t *sb,
                                                 kafs_legacy_map_layout_t *out)
{
  uint64_t log_block_size;
  uint64_t block_size;
  uint64_t block_count;
  uint64_t inode_count;
  uint64_t metadata_size;
  uint64_t block_bitmap_offset;
  uint64_t inode_table_offset;
  uint64_t image_size;
  uint64_t region_end;
  uint64_t max_end = 0u;
  uint64_t entry_bytes;
  uint64_t inode_bytes;
  uint64_t bitmap_bytes;
  uint64_t offsets[4];
  uint64_t sizes[4];

  if (!sb || !out)
    return -EINVAL;

  log_block_size = kafs_sb_log_blksize_get(sb);
  if (log_block_size >= 32u)
    return -EOVERFLOW;
  block_size = UINT64_C(1) << log_block_size;
  block_count = kafs_blkcnt_stoh(sb->s_r_blkcnt);
  inode_count = kafs_inocnt_stoh(sb->s_inocnt);

  metadata_size = sizeof(kafs_ssuperblock_t);
  if (kafs_u64_align_up(metadata_size, block_size, &metadata_size) != 0)
    return -EOVERFLOW;
  block_bitmap_offset = metadata_size;
  bitmap_bytes = (block_count + 7u) >> 3;
  if (kafs_u64_add(metadata_size, bitmap_bytes, &metadata_size) != 0 ||
      kafs_u64_align_up(metadata_size, 8u, &metadata_size) != 0 ||
      kafs_u64_align_up(metadata_size, block_size, &metadata_size) != 0)
    return -EOVERFLOW;
  inode_table_offset = metadata_size;
  inode_bytes = kafs_inode_table_bytes_for_format(kafs_sb_format_version_get(sb), inode_count);
  if (kafs_u64_add(metadata_size, inode_bytes, &metadata_size) != 0 ||
      kafs_u64_align_up(metadata_size, block_size, &metadata_size) != 0)
    return -EOVERFLOW;

  if (block_count > (UINT64_MAX >> log_block_size))
    return -EOVERFLOW;
  image_size = block_count << log_block_size;

  offsets[0] = kafs_sb_hrl_index_offset_get(sb);
  sizes[0] = kafs_sb_hrl_index_size_get(sb);
  offsets[1] = kafs_sb_hrl_entry_offset_get(sb);
  entry_bytes = (uint64_t)kafs_sb_hrl_entry_cnt_get(sb) * sizeof(kafs_hrl_entry_t);
  sizes[1] = entry_bytes;
  offsets[2] = kafs_sb_journal_offset_get(sb);
  sizes[2] = kafs_sb_journal_size_get(sb);
  offsets[3] = kafs_sb_pendinglog_offset_get(sb);
  sizes[3] = kafs_sb_pendinglog_size_get(sb);

  for (size_t i = 0; i < sizeof(offsets) / sizeof(offsets[0]); ++i)
  {
    if (offsets[i] == 0u || sizes[i] == 0u)
      continue;
    if (kafs_u64_add(offsets[i], sizes[i], &region_end) != 0)
      return -EOVERFLOW;
    if (region_end > max_end)
      max_end = region_end;
  }
  if (max_end > image_size)
    image_size = max_end;
  if (kafs_u64_align_up(image_size, block_size, &image_size) != 0)
    return -EOVERFLOW;

  if (kafs_u64_to_size(metadata_size, &out->metadata_size) != 0 ||
      kafs_u64_to_size(image_size, &out->image_size) != 0 ||
      kafs_u64_to_size(block_bitmap_offset, &out->block_bitmap_offset) != 0 ||
      kafs_u64_to_size(inode_table_offset, &out->inode_table_offset) != 0)
    return -EOVERFLOW;
  return 0;
}
