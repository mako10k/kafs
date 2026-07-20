#include "kafs_v7_block_tree.h"

#include "kafs_v7_layout.h"

#include <endian.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>

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
