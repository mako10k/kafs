#pragma once

#include "kafs_descriptor_layout.h"

#define KAFS_V7_SUPERBLOCK_ANCHOR_MAGIC 0x4B375341u /* 'K7SA' */
#define KAFS_V7_SUPERBLOCK_ANCHOR_VERSION 1u
#define KAFS_V7_LAYOUT_MAGIC 0x4B374C44u /* 'K7LD' */
#define KAFS_V7_LAYOUT_VERSION 1u

static inline const kafs_descriptor_wire_format_t *kafs_v7_layout_wire_format(void)
{
  static const kafs_descriptor_wire_format_t wire = {
      .anchor_magic = KAFS_V7_SUPERBLOCK_ANCHOR_MAGIC,
      .anchor_version = KAFS_V7_SUPERBLOCK_ANCHOR_VERSION,
      .layout_magic = KAFS_V7_LAYOUT_MAGIC,
      .layout_version = KAFS_V7_LAYOUT_VERSION,
  };
  return &wire;
}

static inline int kafs_v7_discover_layout(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                                          kafs_descriptor_layout_report_t *report)
{
  return kafs_descriptor_discover_layout_wire(fd, sb, file_size, kafs_v7_layout_wire_format(),
                                              report);
}

static inline void kafs_v7_anchor_init(kafs_ssuperblock_t *sb, uint64_t primary_off,
                                       uint32_t desc_bytes, uint32_t candidate_count)
{
  kafs_descriptor_anchor_init_wire(sb, primary_off, desc_bytes, candidate_count,
                                   kafs_v7_layout_wire_format());
}

static inline int kafs_v7_build_mkfs_descriptor(void *buf, uint32_t desc_bytes,
                                                const kafs_ssuperblock_t *sb, uint64_t image_size,
                                                uint64_t desc_candidates[],
                                                uint32_t candidate_count, uint64_t bitmap_off,
                                                uint64_t bitmap_bytes, uint64_t inode_off,
                                                uint64_t inode_bytes)
{
  return kafs_descriptor_build_mkfs_descriptor_wire(
      buf, desc_bytes, sb, image_size, desc_candidates, candidate_count, bitmap_off, bitmap_bytes,
      inode_off, inode_bytes, kafs_v7_layout_wire_format());
}
