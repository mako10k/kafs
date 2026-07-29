#include "kafs_v7_block_tree.h"

#include "kafs_v7_layout.h"

#include <endian.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int kafs_v7_block_tree_address(uint32_t block_size, uint64_t file_block,
                               kafs_v7_block_tree_path_t *path)
{
  if (!path || block_size < sizeof(uint32_t) || block_size % sizeof(uint32_t) != 0u)
    return -EINVAL;
  memset(path, 0, sizeof(*path));
  if (file_block < KAFS_V7_INODE_DIRECT_REFERENCE_COUNT)
  {
    path->inode_slot = (uint32_t)file_block;
    return 0;
  }

  uint64_t remaining = file_block - KAFS_V7_INODE_DIRECT_REFERENCE_COUNT;
  uint64_t references_per_block = block_size / sizeof(uint32_t);
  uint64_t capacity = 1u;
  for (uint32_t levels = 1u; levels <= KAFS_V7_INODE_INDIRECT_REFERENCE_COUNT; ++levels)
  {
    int capacity_overflow = capacity > UINT64_MAX / references_per_block;
    if (!capacity_overflow)
      capacity *= references_per_block;
    if (!capacity_overflow && remaining >= capacity)
    {
      remaining -= capacity;
      continue;
    }

    path->inode_slot = KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + levels - 1u;
    path->level_count = levels;
    for (uint32_t level = levels; level > 0u; --level)
    {
      path->indices[level - 1u] = (uint32_t)(remaining % references_per_block);
      remaining /= references_per_block;
    }
    return 0;
  }
  return -EFBIG;
}

static int kafs_v7_block_tree_walk_reference(const kafs_v7_block_tree_t *tree, uint32_t reference,
                                             uint32_t remaining_levels,
                                             kafs_v7_block_tree_visit_fn visit, void *visit_opaque)
{
  if (reference == 0u)
    return 0;
  uint64_t logical_block = (uint64_t)reference - 1u;
  if (logical_block >= tree->logical_block_count)
    return -EUCLEAN;

  int rc = visit(visit_opaque, logical_block, remaining_levels);
  if (rc != 0 || remaining_levels == 0u)
    return rc;

  uint32_t *references = (uint32_t *)malloc(tree->block_size);
  if (!references)
    return -ENOMEM;
  rc = tree->read(tree->read_opaque, logical_block, references, tree->block_size);
  size_t reference_count = tree->block_size / sizeof(*references);
  for (size_t index = 0; rc == 0 && index < reference_count; ++index)
  {
    rc = kafs_v7_block_tree_walk_reference(tree, le32toh(references[index]), remaining_levels - 1u,
                                           visit, visit_opaque);
  }
  free(references);
  return rc;
}

int kafs_v7_block_tree_walk(const kafs_v7_block_tree_t *tree, uint32_t root_reference,
                            uint32_t remaining_levels, kafs_v7_block_tree_visit_fn visit,
                            void *visit_opaque)
{
  if (!tree || !tree->read || !visit || tree->logical_block_count == 0u ||
      tree->block_size < sizeof(uint32_t) || tree->block_size % sizeof(uint32_t) != 0u ||
      remaining_levels == 0u || remaining_levels > KAFS_V7_INODE_INDIRECT_REFERENCE_COUNT)
    return -EINVAL;
  return kafs_v7_block_tree_walk_reference(tree, root_reference, remaining_levels, visit,
                                           visit_opaque);
}
