#pragma once

#include <stddef.h>
#include <stdint.h>

typedef int (*kafs_v7_block_tree_read_fn)(void *opaque, uint64_t logical_block, void *block,
                                          size_t block_bytes);

typedef int (*kafs_v7_block_tree_visit_fn)(void *opaque, uint64_t logical_block,
                                           uint32_t remaining_levels);

typedef struct kafs_v7_block_tree
{
  uint32_t block_size;
  uint32_t reserved;
  uint64_t logical_block_count;
  kafs_v7_block_tree_read_fn read;
  void *read_opaque;
} kafs_v7_block_tree_t;

typedef struct kafs_v7_block_tree_path
{
  uint32_t inode_slot;
  uint32_t level_count;
  uint32_t indices[3];
} kafs_v7_block_tree_path_t;

/* Map a zero-based file data-block index to its inode slot and tree indices. */
int kafs_v7_block_tree_address(uint32_t block_size, uint64_t file_block,
                               kafs_v7_block_tree_path_t *path);

/*
 * Walk one plus-one encoded indirect root. remaining_levels is 1 for a
 * single-indirect root and 3 for a triple-indirect root. The visitor receives
 * zero for data blocks and a positive value for indirect blocks.
 */
int kafs_v7_block_tree_walk(const kafs_v7_block_tree_t *tree, uint32_t root_reference,
                            uint32_t remaining_levels, kafs_v7_block_tree_visit_fn visit,
                            void *visit_opaque);
