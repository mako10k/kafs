#pragma once

#include <stdint.h>

#define KAFS_META_REGION_COUNT 11u

enum kafs_meta_region_id
{
  KAFS_META_REGION_SUPERBLOCK_CHECKPOINT = 0,
  KAFS_META_REGION_BLOCK_BITMAP = 1,
  KAFS_META_REGION_INODE_TABLE = 2,
  KAFS_META_REGION_ALLOCATOR_SUMMARY = 3,
  KAFS_META_REGION_HRL_INDEX = 4,
  KAFS_META_REGION_HRL_ENTRIES = 5,
  KAFS_META_REGION_JOURNAL_HEADER = 6,
  KAFS_META_REGION_JOURNAL_DATA = 7,
  KAFS_META_REGION_PENDING_LOG = 8,
  KAFS_META_REGION_TAIL_METADATA = 9,
  KAFS_META_REGION_UNKNOWN = 10,
};

static inline const char *kafs_meta_region_name(uint32_t region)
{
  switch (region)
  {
  case KAFS_META_REGION_SUPERBLOCK_CHECKPOINT:
    return "superblock_checkpoint";
  case KAFS_META_REGION_BLOCK_BITMAP:
    return "block_bitmap";
  case KAFS_META_REGION_INODE_TABLE:
    return "inode_table";
  case KAFS_META_REGION_ALLOCATOR_SUMMARY:
    return "allocator_summary";
  case KAFS_META_REGION_HRL_INDEX:
    return "hrl_index";
  case KAFS_META_REGION_HRL_ENTRIES:
    return "hrl_entries";
  case KAFS_META_REGION_JOURNAL_HEADER:
    return "journal_header";
  case KAFS_META_REGION_JOURNAL_DATA:
    return "journal_data";
  case KAFS_META_REGION_PENDING_LOG:
    return "pending_log";
  case KAFS_META_REGION_TAIL_METADATA:
    return "tail_metadata";
  case KAFS_META_REGION_UNKNOWN:
    return "unknown";
  default:
    return "unknown";
  }
}
