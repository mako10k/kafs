#include "kafs_v7_fuse_write.h"

#include "kafs_v7_block_tree.h"
#include "kafs_v7_fuse_policy.h"
#include "kafs_v7_layout.h"
#include "kafs_v7_runtime_transaction.h"
#include "kafs_tool_util.h"

#include <endian.h>
#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static uint32_t kafs_v7_fuse_inode_reference(const kafs_v7_inode_t *inode, uint32_t slot);

static void kafs_v7_fuse_clear_direct_references(kafs_v7_inode_t *inode, uint64_t keep_slots)
{
  uint64_t direct_keep = keep_slots < KAFS_V7_INODE_DIRECT_REFERENCE_COUNT
                             ? keep_slots
                             : KAFS_V7_INODE_DIRECT_REFERENCE_COUNT;
  for (uint64_t slot = direct_keep; slot < KAFS_V7_INODE_DIRECT_REFERENCE_COUNT; ++slot)
    memset(inode->inline_or_block_refs + slot * sizeof(uint32_t), 0, sizeof(uint32_t));
}

static int kafs_v7_fuse_regular_inode_state(kafs_context_t *ctx, kafs_inocnt_t ino,
                                            const kafs_v7_inode_runtime_shard_t **shard_out,
                                            const kafs_v7_inode_t **inode_out)
{
  if (!ctx || !shard_out || !inode_out)
    return -EINVAL;
  const kafs_v7_inode_runtime_shard_t *shard = kafs_ctx_v7_inode_shard_for_ino(ctx, ino);
  const kafs_v7_inode_t *inode = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(ctx, ino);
  if (!shard || !inode || !S_ISREG(le16toh(inode->mode)))
    return -EOPNOTSUPP;
  *shard_out = shard;
  *inode_out = inode;
  return 0;
}

typedef struct kafs_v7_fuse_double_view
{
  uint32_t references_per_block;
  uint64_t old_slots;
  uint64_t single_root_block;
  uint64_t double_root_block;
  uint32_t *single_root;
  uint32_t *double_root;
  uint32_t **double_leaves;
  uint64_t *double_leaf_blocks;
  size_t double_leaf_count;
} kafs_v7_fuse_double_view_t;

typedef struct kafs_v7_fuse_triple_middle
{
  uint32_t index;
  uint64_t logical_block;
  uint32_t *references;
  size_t plan_id;
} kafs_v7_fuse_triple_middle_t;

typedef struct kafs_v7_fuse_triple_leaf
{
  uint32_t middle_index;
  uint32_t leaf_index;
  uint64_t logical_block;
  uint32_t *references;
  size_t plan_id;
} kafs_v7_fuse_triple_leaf_t;

typedef struct kafs_v7_fuse_triple_view
{
  uint32_t references_per_block;
  uint64_t old_slots;
  uint64_t root_block;
  uint32_t *root;
  kafs_v7_fuse_triple_middle_t *middles;
  size_t middle_count;
  kafs_v7_fuse_triple_leaf_t *leaves;
  size_t leaf_count;
  size_t root_plan_id;
} kafs_v7_fuse_triple_view_t;

typedef struct kafs_v7_fuse_write_extent
{
  uint64_t file_size;
  uint64_t request_end;
  uint64_t first_slot;
  uint64_t last_slot;
  uint64_t existing_slots;
  uint64_t single_capacity;
  uint64_t double_capacity;
} kafs_v7_fuse_write_extent_t;

typedef struct kafs_v7_fuse_truncate_state
{
  const kafs_v7_inode_runtime_shard_t *shard;
  const kafs_v7_inode_t *inode;
  uint64_t old_size;
  uint64_t old_slots;
  uint64_t keep_slots;
} kafs_v7_fuse_truncate_state_t;

static int kafs_v7_fuse_capacities(uint32_t block_size, uint64_t *single_capacity_out,
                                   uint64_t *double_capacity_out)
{
  if (!single_capacity_out || !double_capacity_out || block_size < sizeof(uint32_t) ||
      block_size % sizeof(uint32_t) != 0u)
    return -EINVAL;
  uint64_t references_per_block = block_size / sizeof(uint32_t);
  if (references_per_block > UINT64_MAX / references_per_block)
    return -EOVERFLOW;
  uint64_t double_blocks = references_per_block * references_per_block;
  if (references_per_block > UINT64_MAX - KAFS_V7_INODE_DIRECT_REFERENCE_COUNT ||
      double_blocks > UINT64_MAX - KAFS_V7_INODE_DIRECT_REFERENCE_COUNT - references_per_block)
    return -EOVERFLOW;
  *single_capacity_out = KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + references_per_block;
  *double_capacity_out = *single_capacity_out + double_blocks;
  return 0;
}

static int kafs_v7_fuse_triple_capacity(uint32_t block_size, uint64_t *single_capacity_out,
                                        uint64_t *double_capacity_out,
                                        uint64_t *triple_capacity_out)
{
  if (!triple_capacity_out)
    return -EINVAL;
  int rc = kafs_v7_fuse_capacities(block_size, single_capacity_out, double_capacity_out);
  if (rc != 0)
    return rc;
  uint64_t references_per_block = block_size / sizeof(uint32_t);
  uint64_t double_blocks = references_per_block * references_per_block;
  if (double_blocks > UINT64_MAX / references_per_block)
    return -EOVERFLOW;
  uint64_t triple_blocks = double_blocks * references_per_block;
  if (triple_blocks > UINT64_MAX - *double_capacity_out)
    return -EOVERFLOW;
  *triple_capacity_out = *double_capacity_out + triple_blocks;
  return 0;
}

static int kafs_v7_fuse_expected_blocks(uint32_t block_size, uint64_t data_blocks,
                                        uint32_t *blocks_out)
{
  uint64_t single_capacity = 0u;
  uint64_t double_capacity = 0u;
  uint64_t triple_capacity = 0u;
  int rc = kafs_v7_fuse_triple_capacity(block_size, &single_capacity, &double_capacity,
                                        &triple_capacity);
  if (rc != 0)
    return rc;
  if (data_blocks > triple_capacity)
    return -EFBIG;
  uint64_t blocks = data_blocks;
  if (data_blocks > KAFS_V7_INODE_DIRECT_REFERENCE_COUNT)
    ++blocks;
  if (data_blocks > single_capacity)
  {
    uint64_t double_data = data_blocks - single_capacity;
    uint64_t references_per_block = block_size / sizeof(uint32_t);
    uint64_t double_data_capacity = references_per_block * references_per_block;
    if (double_data > double_data_capacity)
      double_data = double_data_capacity;
    uint64_t leaves = (double_data - 1u) / references_per_block + 1u;
    blocks += 1u + leaves;
  }
  if (data_blocks > double_capacity)
  {
    uint64_t triple_data = data_blocks - double_capacity;
    uint64_t references_per_block = block_size / sizeof(uint32_t);
    uint64_t leaves = (triple_data - 1u) / references_per_block + 1u;
    uint64_t middles = (leaves - 1u) / references_per_block + 1u;
    if (leaves > UINT64_MAX - middles - 1u || blocks > UINT64_MAX - leaves - middles - 1u)
      return -EOVERFLOW;
    blocks += 1u + middles + leaves;
  }
  if (blocks > UINT32_MAX)
    return -EOVERFLOW;
  *blocks_out = (uint32_t)blocks;
  return 0;
}

static int kafs_v7_fuse_write_extent_init(uint32_t block_size, uint64_t file_size, size_t size,
                                          uint64_t offset, kafs_v7_fuse_write_extent_t *extent)
{
  if (!extent || block_size == 0u || size == 0u || offset > file_size)
    return -EOPNOTSUPP;
  if (size > UINT64_MAX - offset)
    return -EFBIG;
  memset(extent, 0, sizeof(*extent));
  extent->file_size = file_size;
  extent->request_end = offset + size;
  extent->first_slot = offset / block_size;
  extent->last_slot = (extent->request_end - 1u) / block_size;
  extent->existing_slots = file_size == 0u ? 0u : (file_size - 1u) / block_size + 1u;
  return kafs_v7_fuse_capacities(block_size, &extent->single_capacity, &extent->double_capacity);
}

static int kafs_v7_fuse_write_state_init(kafs_context_t *ctx, kafs_inocnt_t ino, size_t size,
                                         uint64_t offset, int double_depth,
                                         const kafs_v7_inode_runtime_shard_t **shard_out,
                                         const kafs_v7_inode_t **inode_out,
                                         kafs_v7_fuse_write_extent_t *extent)
{
  if (!ctx || !ctx->c_v7_runtime_transactions || !kafs_v7_fuse_policy_controlled_write_active(ctx))
    return -EROFS;
  int rc = kafs_v7_fuse_regular_inode_state(ctx, ino, shard_out, inode_out);
  if (rc != 0)
    return rc;
  rc = kafs_v7_fuse_write_extent_init(ctx->c_v7_block_size, le64toh((*inode_out)->size), size,
                                      offset, extent);
  if (rc != 0)
    return rc;
  uint64_t triple_capacity = 0u;
  rc = kafs_v7_fuse_triple_capacity(ctx->c_v7_block_size, &extent->single_capacity,
                                    &extent->double_capacity, &triple_capacity);
  if (rc != 0)
    return rc;
  uint32_t triple_reference =
      kafs_v7_fuse_inode_reference(*inode_out, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 2u);
  if (extent->existing_slots > triple_capacity ||
      ((extent->existing_slots > extent->double_capacity) != (triple_reference != 0u)))
    return -EOPNOTSUPP;
  if (double_depth)
    return extent->last_slot >= extent->single_capacity &&
                   extent->last_slot < extent->double_capacity
               ? 0
               : -EOPNOTSUPP;
  return extent->first_slot < extent->single_capacity && extent->last_slot < extent->single_capacity
             ? 0
             : -EOPNOTSUPP;
}

static int kafs_v7_fuse_truncate_state_init(kafs_context_t *ctx, kafs_inocnt_t ino, uint64_t size,
                                            kafs_v7_fuse_truncate_state_t *state)
{
  if (!state)
    return -EINVAL;
  memset(state, 0, sizeof(*state));
  int rc = kafs_v7_fuse_regular_inode_state(ctx, ino, &state->shard, &state->inode);
  if (rc != 0)
    return rc;
  state->old_size = le64toh(state->inode->size);
  if (size > state->old_size)
    return -EOPNOTSUPP;
  state->old_slots =
      state->old_size == 0u ? 0u : (state->old_size - 1u) / ctx->c_v7_block_size + 1u;
  state->keep_slots = size == 0u ? 0u : (size - 1u) / ctx->c_v7_block_size + 1u;
  return 0;
}

static int kafs_v7_fuse_write_validate(const kafs_context_t *ctx, kafs_inocnt_t ino, size_t size,
                                       uint64_t offset, const kafs_v7_inode_t **inode_out,
                                       uint32_t *group_id_out, uint32_t *first_slot_out,
                                       size_t *block_count_out,
                                       uint64_t retained[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT])
{
  if (!ctx || !inode_out || !group_id_out || !first_slot_out || !block_count_out || !retained ||
      !ctx->c_v7_runtime_transactions || !kafs_v7_fuse_policy_controlled_write_active(ctx))
    return -EROFS;
  if (ctx->c_v7_block_size == 0u || size == 0u)
    return -EOPNOTSUPP;

  const kafs_v7_inode_runtime_shard_t *shard = kafs_ctx_v7_inode_shard_for_ino(ctx, ino);
  const kafs_v7_inode_t *inode =
      (const kafs_v7_inode_t *)kafs_ctx_v7_inode((kafs_context_t *)ctx, ino);
  if (!shard || !inode || !S_ISREG(le16toh(inode->mode)))
    return -EOPNOTSUPP;

  kafs_v7_fuse_write_extent_t extent;
  int rc = kafs_v7_fuse_write_extent_init(ctx->c_v7_block_size, le64toh(inode->size), size, offset,
                                          &extent);
  if (rc != 0)
    return rc;
  uint64_t single_capacity = 0u;
  uint64_t double_capacity = 0u;
  uint64_t triple_capacity = 0u;
  rc = kafs_v7_fuse_triple_capacity(ctx->c_v7_block_size, &single_capacity, &double_capacity,
                                    &triple_capacity);
  if (rc != 0)
    return rc;
  uint32_t triple_reference =
      kafs_v7_fuse_inode_reference(inode, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 2u);
  if (extent.first_slot >= KAFS_V7_INODE_DIRECT_REFERENCE_COUNT ||
      extent.last_slot >= KAFS_V7_INODE_DIRECT_REFERENCE_COUNT ||
      extent.existing_slots > triple_capacity ||
      ((extent.existing_slots > double_capacity) != (triple_reference != 0u)))
    return -EOPNOTSUPP;

  size_t block_count = (size_t)(extent.last_slot - extent.first_slot + 1u);
  for (size_t i = 0; i < block_count; ++i)
  {
    uint32_t reference = 0u;
    memcpy(&reference, inode->inline_or_block_refs + (extent.first_slot + i) * sizeof(reference),
           sizeof(reference));
    reference = le32toh(reference);
    if (reference == 0u)
    {
      uint64_t slot = extent.first_slot + i;
      if (slot < extent.existing_slots)
        return -EOPNOTSUPP;
      retained[i] = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK;
    }
    else
      retained[i] = (uint64_t)reference - 1u;
  }

  *inode_out = inode;
  *group_id_out = shard->group_id;
  *first_slot_out = (uint32_t)extent.first_slot;
  *block_count_out = block_count;
  return 0;
}

static int kafs_v7_fuse_commit_data_cow_inode(kafs_v7_runtime_data_cow_batch_t **operation,
                                              kafs_inocnt_t ino, const kafs_v7_inode_t *inode,
                                              int rc)
{
  kafs_v7_journal_patch_t inode_patch = {
      .target_type = KAFS_V7_JOURNAL_TARGET_INODE,
      .logical_index = ino,
      .patch_bytes = sizeof(*inode),
      .patch = inode,
  };
  kafs_v7_runtime_transaction_result_t transaction;
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_batch_commit(operation, &inode_patch, 1u, &transaction);
  if (*operation)
    (void)kafs_v7_runtime_data_cow_batch_abort(operation);
  return rc;
}

static int kafs_v7_fuse_commit_truncate_inode(kafs_context_t *ctx,
                                              kafs_v7_runtime_data_cow_batch_t **operation,
                                              size_t plan_count, kafs_inocnt_t ino,
                                              const kafs_v7_inode_t *inode)
{
  if (plan_count != 0u)
    return kafs_v7_fuse_commit_data_cow_inode(operation, ino, inode, 0);
  if (*operation)
    (void)kafs_v7_runtime_data_cow_batch_abort(operation);
  kafs_v7_journal_patch_t patch = {
      .target_type = KAFS_V7_JOURNAL_TARGET_INODE,
      .logical_index = ino,
      .patch_bytes = sizeof(*inode),
      .patch = inode,
  };
  kafs_v7_runtime_transaction_result_t transaction;
  return kafs_v7_runtime_transaction_commit(ctx->c_v7_runtime_transactions, &patch, 1u,
                                            &transaction);
}

static int kafs_v7_fuse_read_logical_block(const kafs_context_t *ctx, uint64_t logical_block,
                                           void *block)
{
  if (logical_block >= UINT32_MAX)
    return -ERANGE;
  uint64_t physical_off = 0u;
  int rc =
      kafs_ctx_v7_data_ref_physical_offset(ctx, (kafs_blkcnt_t)logical_block + 1u, &physical_off);
  if (rc == 0)
    rc = kafs_pread_all(ctx->c_fd, block, ctx->c_v7_block_size, (off_t)physical_off);
  return rc;
}

static void kafs_v7_fuse_double_view_clear(kafs_v7_fuse_double_view_t *view)
{
  if (!view)
    return;
  for (size_t i = 0u; i < view->double_leaf_count; ++i)
    free(view->double_leaves[i]);
  free(view->double_leaves);
  free(view->double_leaf_blocks);
  free(view->double_root);
  free(view->single_root);
  memset(view, 0, sizeof(*view));
}

static int kafs_v7_fuse_double_view_resize(kafs_v7_fuse_double_view_t *view, size_t leaf_count,
                                           uint32_t block_size)
{
  if (leaf_count <= view->double_leaf_count)
    return 0;
  if (leaf_count > view->references_per_block ||
      leaf_count > SIZE_MAX / sizeof(*view->double_leaves) ||
      leaf_count > SIZE_MAX / sizeof(*view->double_leaf_blocks))
    return -EFBIG;
  uint32_t **leaves = calloc(leaf_count, sizeof(*leaves));
  uint64_t *leaf_blocks = malloc(leaf_count * sizeof(*leaf_blocks));
  if (!leaves || !leaf_blocks)
  {
    free(leaves);
    free(leaf_blocks);
    return -ENOMEM;
  }
  for (size_t i = 0u; i < leaf_count; ++i)
    leaf_blocks[i] = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK;
  if (view->double_leaf_count != 0u)
  {
    memcpy(leaves, view->double_leaves, view->double_leaf_count * sizeof(*view->double_leaves));
    memcpy(leaf_blocks, view->double_leaf_blocks,
           view->double_leaf_count * sizeof(*view->double_leaf_blocks));
  }
  for (size_t i = view->double_leaf_count; i < leaf_count; ++i)
  {
    leaves[i] = calloc(1u, block_size);
    if (!leaves[i])
    {
      for (size_t j = view->double_leaf_count; j < i; ++j)
        free(leaves[j]);
      free(leaves);
      free(leaf_blocks);
      return -ENOMEM;
    }
  }
  free(view->double_leaves);
  free(view->double_leaf_blocks);
  view->double_leaves = leaves;
  view->double_leaf_blocks = leaf_blocks;
  view->double_leaf_count = leaf_count;
  return 0;
}

static int kafs_v7_fuse_double_view_init(kafs_context_t *ctx, const kafs_v7_inode_t *inode,
                                         kafs_v7_fuse_double_view_t *view)
{
  if (!ctx || !inode || !view || ctx->c_v7_block_size == 0u)
    return -EINVAL;
  memset(view, 0, sizeof(*view));
  view->single_root_block = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK;
  view->double_root_block = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK;
  view->references_per_block = ctx->c_v7_block_size / sizeof(uint32_t);
  if (view->references_per_block == 0u)
    return -EINVAL;
  uint64_t size = le64toh(inode->size);
  view->old_slots = size == 0u ? 0u : (size - 1u) / ctx->c_v7_block_size + 1u;
  uint64_t single_capacity = KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + view->references_per_block;

  view->single_root = calloc(1u, ctx->c_v7_block_size);
  view->double_root = calloc(1u, ctx->c_v7_block_size);
  if (!view->single_root || !view->double_root)
  {
    kafs_v7_fuse_double_view_clear(view);
    return -ENOMEM;
  }

  int rc = 0;
  if (view->old_slots > KAFS_V7_INODE_DIRECT_REFERENCE_COUNT)
  {
    uint32_t reference = kafs_v7_fuse_inode_reference(inode, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT);
    if (reference == 0u)
      rc = -EUCLEAN;
    else
    {
      view->single_root_block = (uint64_t)reference - 1u;
      rc = kafs_v7_fuse_read_logical_block(ctx, view->single_root_block, view->single_root);
    }
  }

  size_t leaf_count = 0u;
  if (rc == 0 && view->old_slots > single_capacity)
  {
    uint64_t double_data = view->old_slots - single_capacity;
    uint64_t double_data_capacity =
        (uint64_t)view->references_per_block * view->references_per_block;
    if (double_data > double_data_capacity)
      double_data = double_data_capacity;
    leaf_count = (size_t)((double_data - 1u) / view->references_per_block + 1u);
    uint32_t reference =
        kafs_v7_fuse_inode_reference(inode, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 1u);
    if (reference == 0u)
      rc = -EUCLEAN;
    else
    {
      view->double_root_block = (uint64_t)reference - 1u;
      rc = kafs_v7_fuse_read_logical_block(ctx, view->double_root_block, view->double_root);
    }
  }
  if (rc == 0)
    rc = kafs_v7_fuse_double_view_resize(view, leaf_count, ctx->c_v7_block_size);
  for (size_t i = 0u; rc == 0 && i < leaf_count; ++i)
  {
    uint32_t reference = le32toh(view->double_root[i]);
    if (reference == 0u)
      rc = -EUCLEAN;
    else
    {
      view->double_leaf_blocks[i] = (uint64_t)reference - 1u;
      rc =
          kafs_v7_fuse_read_logical_block(ctx, view->double_leaf_blocks[i], view->double_leaves[i]);
    }
  }
  if (rc != 0)
    kafs_v7_fuse_double_view_clear(view);
  return rc;
}

static int kafs_v7_fuse_double_view_reference(const kafs_v7_fuse_double_view_t *view,
                                              const kafs_v7_inode_t *inode, uint32_t block_size,
                                              uint64_t file_slot, uint32_t *reference_out)
{
  if (!view || !inode || !reference_out)
    return -EINVAL;
  kafs_v7_block_tree_path_t path;
  int rc = kafs_v7_block_tree_address(block_size, file_slot, &path);
  if (rc != 0)
    return rc;
  if (path.level_count == 0u)
    *reference_out = kafs_v7_fuse_inode_reference(inode, path.inode_slot);
  else if (path.level_count == 1u)
    *reference_out = le32toh(view->single_root[path.indices[0]]);
  else if (path.level_count == 2u && path.indices[0] < view->double_leaf_count)
    *reference_out = le32toh(view->double_leaves[path.indices[0]][path.indices[1]]);
  else
    return -EOPNOTSUPP;
  return 0;
}

static int kafs_v7_fuse_double_view_destination(kafs_v7_fuse_double_view_t *view,
                                                kafs_v7_inode_t *inode, uint32_t block_size,
                                                uint64_t file_slot, void **destination_out)
{
  if (!view || !inode || !destination_out)
    return -EINVAL;
  kafs_v7_block_tree_path_t path;
  int rc = kafs_v7_block_tree_address(block_size, file_slot, &path);
  if (rc != 0)
    return rc;
  if (path.level_count == 0u)
    *destination_out = inode->inline_or_block_refs + path.inode_slot * sizeof(uint32_t);
  else if (path.level_count == 1u)
    *destination_out = &view->single_root[path.indices[0]];
  else if (path.level_count == 2u && path.indices[0] < view->double_leaf_count)
    *destination_out = &view->double_leaves[path.indices[0]][path.indices[1]];
  else
    return -EOPNOTSUPP;
  return 0;
}

static void kafs_v7_fuse_triple_view_clear(kafs_v7_fuse_triple_view_t *view)
{
  if (!view)
    return;
  for (size_t i = 0u; i < view->leaf_count; ++i)
    free(view->leaves[i].references);
  for (size_t i = 0u; i < view->middle_count; ++i)
    free(view->middles[i].references);
  free(view->leaves);
  free(view->middles);
  free(view->root);
  memset(view, 0, sizeof(*view));
}

static int kafs_v7_fuse_triple_view_init(kafs_context_t *ctx, const kafs_v7_inode_t *inode,
                                         uint64_t double_capacity, kafs_v7_fuse_triple_view_t *view)
{
  if (!ctx || !inode || !view || ctx->c_v7_block_size < sizeof(uint32_t))
    return -EINVAL;
  memset(view, 0, sizeof(*view));
  view->root_block = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK;
  view->root_plan_id = SIZE_MAX;
  view->references_per_block = ctx->c_v7_block_size / sizeof(uint32_t);
  view->root = calloc(1u, ctx->c_v7_block_size);
  if (!view->root)
    return -ENOMEM;
  uint64_t size = le64toh(inode->size);
  view->old_slots = size == 0u ? 0u : (size - 1u) / ctx->c_v7_block_size + 1u;
  uint32_t reference =
      kafs_v7_fuse_inode_reference(inode, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 2u);
  if (view->old_slots <= double_capacity)
  {
    if (reference != 0u)
    {
      kafs_v7_fuse_triple_view_clear(view);
      return -EUCLEAN;
    }
    return 0;
  }
  if (reference == 0u)
  {
    kafs_v7_fuse_triple_view_clear(view);
    return -EUCLEAN;
  }
  view->root_block = (uint64_t)reference - 1u;
  int rc = kafs_v7_fuse_read_logical_block(ctx, view->root_block, view->root);
  if (rc != 0)
    kafs_v7_fuse_triple_view_clear(view);
  return rc;
}

static int kafs_v7_fuse_triple_middle_get(kafs_context_t *ctx, kafs_v7_fuse_triple_view_t *view,
                                          uint32_t index, int existing_required,
                                          kafs_v7_fuse_triple_middle_t **middle_out)
{
  if (!ctx || !view || !middle_out || index >= view->references_per_block)
    return -EINVAL;
  for (size_t i = 0u; i < view->middle_count; ++i)
    if (view->middles[i].index == index)
    {
      *middle_out = &view->middles[i];
      return 0;
    }

  uint32_t *references = calloc(1u, ctx->c_v7_block_size);
  if (!references)
    return -ENOMEM;
  uint32_t reference = le32toh(view->root[index]);
  if (reference == 0u && existing_required)
  {
    free(references);
    return -EUCLEAN;
  }
  uint64_t logical_block =
      reference == 0u ? KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK : (uint64_t)reference - 1u;
  int rc = reference == 0u ? 0 : kafs_v7_fuse_read_logical_block(ctx, logical_block, references);
  if (rc != 0)
  {
    free(references);
    return rc;
  }
  if (view->middle_count == SIZE_MAX || view->middle_count + 1u > SIZE_MAX / sizeof(*view->middles))
  {
    free(references);
    return -EOVERFLOW;
  }
  kafs_v7_fuse_triple_middle_t *middles =
      realloc(view->middles, (view->middle_count + 1u) * sizeof(*view->middles));
  if (!middles)
  {
    free(references);
    return -ENOMEM;
  }
  view->middles = middles;
  kafs_v7_fuse_triple_middle_t *middle = &view->middles[view->middle_count++];
  *middle = (kafs_v7_fuse_triple_middle_t){
      .index = index,
      .logical_block = logical_block,
      .references = references,
      .plan_id = SIZE_MAX,
  };
  *middle_out = middle;
  return 0;
}

static int kafs_v7_fuse_triple_leaf_get(kafs_context_t *ctx, kafs_v7_fuse_triple_view_t *view,
                                        kafs_v7_fuse_triple_middle_t *middle, uint32_t leaf_index,
                                        int existing_required,
                                        kafs_v7_fuse_triple_leaf_t **leaf_out)
{
  if (!ctx || !view || !middle || !leaf_out || leaf_index >= view->references_per_block)
    return -EINVAL;
  for (size_t i = 0u; i < view->leaf_count; ++i)
    if (view->leaves[i].middle_index == middle->index && view->leaves[i].leaf_index == leaf_index)
    {
      *leaf_out = &view->leaves[i];
      return 0;
    }

  uint32_t *references = calloc(1u, ctx->c_v7_block_size);
  if (!references)
    return -ENOMEM;
  uint32_t reference = le32toh(middle->references[leaf_index]);
  if (reference == 0u && existing_required)
  {
    free(references);
    return -EUCLEAN;
  }
  uint64_t logical_block =
      reference == 0u ? KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK : (uint64_t)reference - 1u;
  int rc = reference == 0u ? 0 : kafs_v7_fuse_read_logical_block(ctx, logical_block, references);
  if (rc != 0)
  {
    free(references);
    return rc;
  }
  if (view->leaf_count == SIZE_MAX || view->leaf_count + 1u > SIZE_MAX / sizeof(*view->leaves))
  {
    free(references);
    return -EOVERFLOW;
  }
  kafs_v7_fuse_triple_leaf_t *leaves =
      realloc(view->leaves, (view->leaf_count + 1u) * sizeof(*view->leaves));
  if (!leaves)
  {
    free(references);
    return -ENOMEM;
  }
  view->leaves = leaves;
  kafs_v7_fuse_triple_leaf_t *leaf = &view->leaves[view->leaf_count++];
  *leaf = (kafs_v7_fuse_triple_leaf_t){
      .middle_index = middle->index,
      .leaf_index = leaf_index,
      .logical_block = logical_block,
      .references = references,
      .plan_id = SIZE_MAX,
  };
  *leaf_out = leaf;
  return 0;
}

static int kafs_v7_fuse_triple_slot_prepare(kafs_context_t *ctx, kafs_v7_fuse_triple_view_t *view,
                                            uint64_t file_slot, int existing_required,
                                            uint32_t *reference_out, void **destination_out)
{
  if (!ctx || !view || !reference_out || !destination_out)
    return -EINVAL;
  kafs_v7_block_tree_path_t path;
  int rc = kafs_v7_block_tree_address(ctx->c_v7_block_size, file_slot, &path);
  if (rc != 0)
    return rc;
  if (path.level_count != 3u)
    return -EOPNOTSUPP;
  kafs_v7_fuse_triple_middle_t *middle = NULL;
  rc = kafs_v7_fuse_triple_middle_get(ctx, view, path.indices[0], existing_required, &middle);
  kafs_v7_fuse_triple_leaf_t *leaf = NULL;
  if (rc == 0)
    rc = kafs_v7_fuse_triple_leaf_get(ctx, view, middle, path.indices[1], existing_required, &leaf);
  if (rc == 0)
  {
    *reference_out = le32toh(leaf->references[path.indices[2]]);
    if (*reference_out == 0u && existing_required)
      rc = -EUCLEAN;
    else
      *destination_out = &leaf->references[path.indices[2]];
  }
  return rc;
}

static int kafs_v7_fuse_stage_write_reference(
    kafs_context_t *ctx, kafs_v7_runtime_data_cow_batch_t *operation, size_t plan_id,
    const kafs_v7_runtime_data_cow_plan_t *plan, uint64_t retained_logical_block,
    uint64_t file_slot, const void *buf, size_t size, uint64_t offset, void *reference_out)
{
  uint8_t *block = calloc(1u, plan->block_size);
  if (!block)
    return -ENOMEM;
  int rc = retained_logical_block == KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK
               ? 0
               : kafs_v7_fuse_read_logical_block(ctx, retained_logical_block, block);
  uint64_t block_start = file_slot * plan->block_size;
  uint64_t copy_start = offset > block_start ? offset : block_start;
  uint64_t request_end = offset + size;
  uint64_t block_end = block_start + plan->block_size;
  uint64_t copy_end = request_end < block_end ? request_end : block_end;
  if (rc == 0)
    memcpy(block + copy_start - block_start, (const uint8_t *)buf + copy_start - offset,
           (size_t)(copy_end - copy_start));
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_batch_stage(operation, plan_id, block, plan->block_size);
  free(block);
  if (rc == 0 && plan->logical_block >= UINT32_MAX)
    rc = -ERANGE;
  if (rc == 0 && !reference_out)
    rc = -EINVAL;
  if (rc == 0)
  {
    uint32_t reference = htole32((uint32_t)plan->logical_block + 1u);
    memcpy(reference_out, &reference, sizeof(reference));
  }
  return rc;
}

static int kafs_v7_fuse_stage_index_reference(kafs_v7_runtime_data_cow_batch_t *operation,
                                              size_t plan_id,
                                              const kafs_v7_runtime_data_cow_plan_t *plan,
                                              const void *block, void *reference_out)
{
  if (!operation || !plan || !block || !reference_out)
    return -EINVAL;
  int rc = kafs_v7_runtime_data_cow_batch_stage(operation, plan_id, block, plan->block_size);
  if (rc == 0 && plan->logical_block >= UINT32_MAX)
    rc = -ERANGE;
  if (rc == 0)
  {
    uint32_t reference = htole32((uint32_t)plan->logical_block + 1u);
    memcpy(reference_out, &reference, sizeof(reference));
  }
  return rc;
}

static int kafs_v7_fuse_stage_truncate_tail(kafs_context_t *ctx,
                                            kafs_v7_runtime_data_cow_batch_t *operation,
                                            size_t plan_id,
                                            const kafs_v7_runtime_data_cow_plan_t *plan,
                                            uint64_t retained_logical_block, uint64_t tail_bytes,
                                            void *reference_out)
{
  if (!ctx || !operation || !plan || !reference_out || tail_bytes >= plan->block_size)
    return -EINVAL;
  uint8_t *block = malloc(plan->block_size);
  if (!block)
    return -ENOMEM;
  int rc = kafs_v7_fuse_read_logical_block(ctx, retained_logical_block, block);
  if (rc == 0)
  {
    memset(block + tail_bytes, 0, plan->block_size - tail_bytes);
    rc = kafs_v7_runtime_data_cow_batch_stage(operation, plan_id, block, plan->block_size);
  }
  free(block);
  if (rc == 0 && plan->logical_block >= UINT32_MAX)
    rc = -ERANGE;
  if (rc == 0)
  {
    uint32_t reference = htole32((uint32_t)plan->logical_block + 1u);
    memcpy(reference_out, &reference, sizeof(reference));
  }
  return rc;
}

static int kafs_v7_fuse_retire_blocks(kafs_context_t *ctx, uint32_t group_id,
                                      const uint64_t *logical_blocks, size_t count)
{
  int retirement_rc = 0;
  for (size_t i = 0; i < count; ++i)
  {
    if (logical_blocks[i] == KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK)
      continue;
    kafs_v7_runtime_data_retirement_request_t retirement = {
        .group_id = group_id,
        .logical_block = logical_blocks[i],
    };
    kafs_v7_runtime_data_retirement_result_t retirement_result;
    int rc = kafs_v7_runtime_data_retire(ctx->c_v7_runtime_transactions, &retirement,
                                         &retirement_result);
    if (retirement_rc == 0 && rc != 0)
      retirement_rc = rc;
  }
  return retirement_rc;
}

static int kafs_v7_fuse_write_single_indirect(kafs_context_t *ctx, kafs_inocnt_t ino,
                                              const void *buf, size_t size, uint64_t offset,
                                              kafs_v7_fuse_write_result_t *result)
{
  const kafs_v7_inode_runtime_shard_t *shard = NULL;
  const kafs_v7_inode_t *mapped = NULL;
  kafs_v7_fuse_write_extent_t extent;
  int rc = kafs_v7_fuse_write_state_init(ctx, ino, size, offset, 0, &shard, &mapped, &extent);
  if (rc != 0)
    return rc;

  size_t block_count = (size_t)(extent.last_slot - extent.first_slot + 1u);
  if (block_count == SIZE_MAX || block_count + 1u > 64u)
    return -E2BIG;
  uint64_t old_root = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK;
  uint32_t root_reference =
      kafs_v7_fuse_inode_reference(mapped, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT);
  if (root_reference != 0u)
    old_root = (uint64_t)root_reference - 1u;

  uint32_t *root = calloc(1u, ctx->c_v7_block_size);
  uint64_t *retained = malloc(block_count * sizeof(*retained));
  kafs_v7_runtime_data_cow_request_t *requests = calloc(block_count + 1u, sizeof(*requests));
  kafs_v7_runtime_data_cow_plan_t *plans = calloc(block_count + 1u, sizeof(*plans));
  if (!root || !retained || !requests || !plans)
  {
    free(root);
    free(retained);
    free(requests);
    free(plans);
    return -ENOMEM;
  }

  rc = old_root == KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK
           ? 0
           : kafs_v7_fuse_read_logical_block(ctx, old_root, root);
  for (size_t i = 0; rc == 0 && i < block_count; ++i)
  {
    uint64_t slot = extent.first_slot + i;
    uint32_t reference = slot < KAFS_V7_INODE_DIRECT_REFERENCE_COUNT
                             ? kafs_v7_fuse_inode_reference(mapped, (uint32_t)slot)
                             : le32toh(root[slot - KAFS_V7_INODE_DIRECT_REFERENCE_COUNT]);
    if (reference == 0u && slot < extent.existing_slots)
      rc = -EOPNOTSUPP;
    retained[i] = reference == 0u ? KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK : (uint64_t)reference - 1u;
    requests[i] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = shard->group_id,
        .retained_logical_block = retained[i],
    };
  }
  requests[block_count] = (kafs_v7_runtime_data_cow_request_t){
      .group_id = shard->group_id,
      .retained_logical_block = old_root,
  };

  kafs_v7_runtime_data_cow_batch_t *operation = NULL;
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_batch_prepare(ctx->c_v7_runtime_transactions, requests,
                                                block_count + 1u, &operation, plans);
  kafs_v7_inode_t inode;
  memcpy(&inode, mapped, sizeof(inode));
  for (size_t i = 0; rc == 0 && i < block_count; ++i)
  {
    uint64_t slot = extent.first_slot + i;
    void *destination = slot < KAFS_V7_INODE_DIRECT_REFERENCE_COUNT
                            ? inode.inline_or_block_refs + slot * sizeof(uint32_t)
                            : (void *)&root[slot - KAFS_V7_INODE_DIRECT_REFERENCE_COUNT];
    rc = kafs_v7_fuse_stage_write_reference(ctx, operation, i, &plans[i], retained[i], slot, buf,
                                            size, offset, destination);
  }
  void *single_reference =
      inode.inline_or_block_refs + KAFS_V7_INODE_DIRECT_REFERENCE_COUNT * sizeof(uint32_t);
  if (rc == 0)
    rc = kafs_v7_fuse_stage_index_reference(operation, block_count, &plans[block_count], root,
                                            single_reference);
  if (rc == 0)
  {
    if (extent.request_end > extent.file_size)
      inode.size = htole64(extent.request_end);
    uint32_t added = old_root == KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK ? 1u : 0u;
    for (size_t i = 0; i < block_count; ++i)
      if (retained[i] == KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK)
        ++added;
    if (added > UINT32_MAX - le32toh(inode.blocks))
      rc = -EOVERFLOW;
    else
      inode.blocks = htole32(le32toh(inode.blocks) + added);
  }
  rc = kafs_v7_fuse_commit_data_cow_inode(&operation, ino, &inode, rc);
  int retirement_rc = 0;
  if (rc == 0)
  {
    retirement_rc = kafs_v7_fuse_retire_blocks(ctx, shard->group_id, retained, block_count);
    int root_retirement = kafs_v7_fuse_retire_blocks(ctx, shard->group_id, &old_root, 1u);
    if (retirement_rc == 0)
      retirement_rc = root_retirement;
  }
  if (rc == 0 && result)
  {
    result->new_logical_block = plans[0].logical_block;
    result->retained_logical_block = retained[0];
    result->retirement_rc = retirement_rc;
  }
  free(root);
  free(retained);
  free(requests);
  free(plans);
  return rc == 0 ? (int)size : rc;
}

static int kafs_v7_fuse_write_double_indirect(kafs_context_t *ctx, kafs_inocnt_t ino,
                                              const void *buf, size_t size, uint64_t offset,
                                              kafs_v7_fuse_write_result_t *result)
{
  const kafs_v7_inode_runtime_shard_t *shard = NULL;
  const kafs_v7_inode_t *mapped = NULL;
  kafs_v7_fuse_write_extent_t extent;
  int rc = kafs_v7_fuse_write_state_init(ctx, ino, size, offset, 1, &shard, &mapped, &extent);
  if (rc != 0)
    return rc;

  size_t block_count = (size_t)(extent.last_slot - extent.first_slot + 1u);
  uint64_t first_double_slot =
      extent.first_slot > extent.single_capacity ? extent.first_slot : extent.single_capacity;
  size_t first_leaf = (size_t)((first_double_slot - extent.single_capacity) /
                               (ctx->c_v7_block_size / sizeof(uint32_t)));
  size_t last_leaf = (size_t)((extent.last_slot - extent.single_capacity) /
                              (ctx->c_v7_block_size / sizeof(uint32_t)));
  size_t touched_leaf_count = last_leaf - first_leaf + 1u;
  int cow_single_root = extent.first_slot < extent.single_capacity;
  size_t overhead = touched_leaf_count + 1u + (size_t)cow_single_root;
  if (block_count > 64u || overhead > 64u || block_count > 64u - overhead)
    return -E2BIG;
  size_t plan_count = block_count + touched_leaf_count + 1u + (size_t)cow_single_root;

  kafs_v7_fuse_double_view_t view;
  rc = kafs_v7_fuse_double_view_init(ctx, mapped, &view);
  if (rc == 0)
    rc = kafs_v7_fuse_double_view_resize(&view, last_leaf + 1u, ctx->c_v7_block_size);
  uint64_t *retained = rc == 0 ? malloc(block_count * sizeof(*retained)) : NULL;
  kafs_v7_runtime_data_cow_request_t *requests =
      rc == 0 ? calloc(plan_count, sizeof(*requests)) : NULL;
  kafs_v7_runtime_data_cow_plan_t *plans = rc == 0 ? calloc(plan_count, sizeof(*plans)) : NULL;
  if (rc == 0 && (!retained || !requests || !plans))
    rc = -ENOMEM;
  if (rc != 0)
  {
    free(retained);
    free(requests);
    free(plans);
    kafs_v7_fuse_double_view_clear(&view);
    return rc;
  }

  for (size_t i = 0u; rc == 0 && i < block_count; ++i)
  {
    uint64_t slot = extent.first_slot + i;
    uint32_t reference = 0u;
    rc = kafs_v7_fuse_double_view_reference(&view, mapped, ctx->c_v7_block_size, slot, &reference);
    if (rc == 0 && reference == 0u && slot < extent.existing_slots)
      rc = -EUCLEAN;
    retained[i] = reference == 0u ? KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK : (uint64_t)reference - 1u;
    requests[i] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = shard->group_id,
        .retained_logical_block = retained[i],
    };
  }

  size_t next_plan = block_count;
  size_t single_plan = SIZE_MAX;
  if (cow_single_root)
  {
    single_plan = next_plan++;
    requests[single_plan] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = shard->group_id,
        .retained_logical_block = view.single_root_block,
    };
  }
  size_t leaf_plan_start = next_plan;
  for (size_t i = 0u; i < touched_leaf_count; ++i)
  {
    size_t leaf = first_leaf + i;
    requests[next_plan++] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = shard->group_id,
        .retained_logical_block = view.double_leaf_blocks[leaf],
    };
  }
  size_t double_plan = next_plan++;
  requests[double_plan] = (kafs_v7_runtime_data_cow_request_t){
      .group_id = shard->group_id,
      .retained_logical_block = view.double_root_block,
  };
  if (rc == 0 && next_plan != plan_count)
    rc = -EUCLEAN;

  kafs_v7_runtime_data_cow_batch_t *operation = NULL;
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_batch_prepare(ctx->c_v7_runtime_transactions, requests,
                                                plan_count, &operation, plans);
  kafs_v7_inode_t inode;
  memcpy(&inode, mapped, sizeof(inode));
  for (size_t i = 0u; rc == 0 && i < block_count; ++i)
  {
    uint64_t slot = extent.first_slot + i;
    void *destination = NULL;
    rc = kafs_v7_fuse_double_view_destination(&view, &inode, ctx->c_v7_block_size, slot,
                                              &destination);
    if (rc == 0)
      rc = kafs_v7_fuse_stage_write_reference(ctx, operation, i, &plans[i], retained[i], slot, buf,
                                              size, offset, destination);
  }
  if (rc == 0 && cow_single_root)
    rc = kafs_v7_fuse_stage_index_reference(
        operation, single_plan, &plans[single_plan], view.single_root,
        inode.inline_or_block_refs + KAFS_V7_INODE_DIRECT_REFERENCE_COUNT * sizeof(uint32_t));
  for (size_t i = 0u; rc == 0 && i < touched_leaf_count; ++i)
  {
    size_t leaf = first_leaf + i;
    size_t plan_id = leaf_plan_start + i;
    rc = kafs_v7_fuse_stage_index_reference(operation, plan_id, &plans[plan_id],
                                            view.double_leaves[leaf], &view.double_root[leaf]);
  }
  if (rc == 0)
    rc = kafs_v7_fuse_stage_index_reference(
        operation, double_plan, &plans[double_plan], view.double_root,
        inode.inline_or_block_refs +
            (KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 1u) * sizeof(uint32_t));
  uint64_t new_size = extent.request_end > extent.file_size ? extent.request_end : extent.file_size;
  uint64_t new_slots = new_size == 0u ? 0u : (new_size - 1u) / ctx->c_v7_block_size + 1u;
  uint32_t expected_blocks = 0u;
  if (rc == 0)
    rc = kafs_v7_fuse_expected_blocks(ctx->c_v7_block_size, new_slots, &expected_blocks);
  if (rc == 0)
  {
    inode.size = htole64(new_size);
    inode.blocks = htole32(expected_blocks);
  }
  rc = kafs_v7_fuse_commit_data_cow_inode(&operation, ino, &inode, rc);

  int retirement_rc = 0;
  if (rc == 0)
  {
    retirement_rc = kafs_v7_fuse_retire_blocks(ctx, shard->group_id, retained, block_count);
    if (cow_single_root)
    {
      int retire_rc = kafs_v7_fuse_retire_blocks(ctx, shard->group_id, &view.single_root_block, 1u);
      if (retirement_rc == 0)
        retirement_rc = retire_rc;
    }
    int retire_rc = kafs_v7_fuse_retire_blocks(
        ctx, shard->group_id, view.double_leaf_blocks + first_leaf, touched_leaf_count);
    if (retirement_rc == 0)
      retirement_rc = retire_rc;
    retire_rc = kafs_v7_fuse_retire_blocks(ctx, shard->group_id, &view.double_root_block, 1u);
    if (retirement_rc == 0)
      retirement_rc = retire_rc;
  }
  if (rc == 0 && result)
  {
    result->new_logical_block = plans[0].logical_block;
    result->retained_logical_block = retained[0];
    result->retirement_rc = retirement_rc;
  }
  if (operation)
    (void)kafs_v7_runtime_data_cow_batch_abort(&operation);
  free(retained);
  free(requests);
  free(plans);
  kafs_v7_fuse_double_view_clear(&view);
  return rc == 0 ? (int)size : rc;
}

static int kafs_v7_fuse_write_triple_indirect(kafs_context_t *ctx, kafs_inocnt_t ino,
                                              const void *buf, size_t size, uint64_t offset,
                                              kafs_v7_fuse_write_result_t *result)
{
  if (!ctx || !ctx->c_v7_runtime_transactions || !kafs_v7_fuse_policy_controlled_write_active(ctx))
    return -EROFS;
  const kafs_v7_inode_runtime_shard_t *shard = NULL;
  const kafs_v7_inode_t *mapped = NULL;
  int rc = kafs_v7_fuse_regular_inode_state(ctx, ino, &shard, &mapped);
  kafs_v7_fuse_write_extent_t extent;
  if (rc == 0)
    rc = kafs_v7_fuse_write_extent_init(ctx->c_v7_block_size, le64toh(mapped->size), size, offset,
                                        &extent);
  uint64_t triple_capacity = 0u;
  if (rc == 0)
    rc = kafs_v7_fuse_triple_capacity(ctx->c_v7_block_size, &extent.single_capacity,
                                      &extent.double_capacity, &triple_capacity);
  uint32_t triple_reference =
      rc == 0 ? kafs_v7_fuse_inode_reference(mapped, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 2u)
              : 0u;
  if (rc == 0 && (extent.last_slot < extent.double_capacity ||
                  extent.last_slot >= triple_capacity || extent.existing_slots > triple_capacity ||
                  ((extent.existing_slots > extent.double_capacity) != (triple_reference != 0u))))
    rc = -EOPNOTSUPP;
  if (rc != 0)
    return rc;

  size_t block_count = (size_t)(extent.last_slot - extent.first_slot + 1u);
  if (block_count == 0u || block_count > 64u)
    return -E2BIG;
  int touches_double = extent.first_slot < extent.double_capacity;
  if (touches_double && extent.first_slot < extent.single_capacity)
    return -E2BIG;

  kafs_v7_fuse_double_view_t double_view;
  memset(&double_view, 0, sizeof(double_view));
  if (touches_double)
    rc = kafs_v7_fuse_double_view_init(ctx, mapped, &double_view);
  kafs_v7_fuse_triple_view_t triple_view;
  memset(&triple_view, 0, sizeof(triple_view));
  if (rc == 0)
    rc = kafs_v7_fuse_triple_view_init(ctx, mapped, extent.double_capacity, &triple_view);

  uint64_t *retained = rc == 0 ? malloc(block_count * sizeof(*retained)) : NULL;
  void **destinations = rc == 0 ? calloc(block_count, sizeof(*destinations)) : NULL;
  size_t lower_leaves[64];
  size_t lower_leaf_count = 0u;
  if (rc == 0 && (!retained || !destinations))
    rc = -ENOMEM;
  for (size_t i = 0u; rc == 0 && i < block_count; ++i)
  {
    uint64_t slot = extent.first_slot + i;
    uint32_t reference = 0u;
    if (slot < extent.double_capacity)
    {
      rc = kafs_v7_fuse_double_view_reference(&double_view, mapped, ctx->c_v7_block_size, slot,
                                              &reference);
      if (rc == 0)
        rc = kafs_v7_fuse_double_view_destination(&double_view, (kafs_v7_inode_t *)mapped,
                                                  ctx->c_v7_block_size, slot, &destinations[i]);
      kafs_v7_block_tree_path_t path;
      if (rc == 0)
        rc = kafs_v7_block_tree_address(ctx->c_v7_block_size, slot, &path);
      if (rc == 0 && path.level_count != 2u)
        rc = -EUCLEAN;
      if (rc == 0)
      {
        size_t leaf = path.indices[0];
        int seen = 0;
        for (size_t j = 0u; j < lower_leaf_count; ++j)
          if (lower_leaves[j] == leaf)
            seen = 1;
        if (!seen)
          lower_leaves[lower_leaf_count++] = leaf;
      }
    }
    else
      rc = kafs_v7_fuse_triple_slot_prepare(ctx, &triple_view, slot, slot < extent.existing_slots,
                                            &reference, &destinations[i]);
    if (rc == 0 && reference == 0u && slot < extent.existing_slots)
      rc = -EUCLEAN;
    retained[i] = reference == 0u ? KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK : (uint64_t)reference - 1u;
  }

  size_t plan_count = block_count + lower_leaf_count + (size_t)touches_double +
                      triple_view.leaf_count + triple_view.middle_count + 1u;
  if (rc == 0 && plan_count > 64u)
    rc = -E2BIG;
  kafs_v7_runtime_data_cow_request_t *requests =
      rc == 0 ? calloc(plan_count, sizeof(*requests)) : NULL;
  kafs_v7_runtime_data_cow_plan_t *plans = rc == 0 ? calloc(plan_count, sizeof(*plans)) : NULL;
  if (rc == 0 && (!requests || !plans))
    rc = -ENOMEM;
  size_t next_plan = 0u;
  for (size_t i = 0u; rc == 0 && i < block_count; ++i)
    requests[next_plan++] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = shard->group_id,
        .retained_logical_block = retained[i],
    };
  size_t lower_leaf_plan_start = next_plan;
  for (size_t i = 0u; rc == 0 && i < lower_leaf_count; ++i)
    requests[next_plan++] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = shard->group_id,
        .retained_logical_block = double_view.double_leaf_blocks[lower_leaves[i]],
    };
  size_t lower_root_plan = SIZE_MAX;
  if (rc == 0 && touches_double)
  {
    lower_root_plan = next_plan++;
    requests[lower_root_plan] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = shard->group_id,
        .retained_logical_block = double_view.double_root_block,
    };
  }
  for (size_t i = 0u; rc == 0 && i < triple_view.leaf_count; ++i)
  {
    triple_view.leaves[i].plan_id = next_plan++;
    requests[triple_view.leaves[i].plan_id] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = shard->group_id,
        .retained_logical_block = triple_view.leaves[i].logical_block,
    };
  }
  for (size_t i = 0u; rc == 0 && i < triple_view.middle_count; ++i)
  {
    triple_view.middles[i].plan_id = next_plan++;
    requests[triple_view.middles[i].plan_id] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = shard->group_id,
        .retained_logical_block = triple_view.middles[i].logical_block,
    };
  }
  if (rc == 0)
  {
    triple_view.root_plan_id = next_plan++;
    requests[triple_view.root_plan_id] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = shard->group_id,
        .retained_logical_block = triple_view.root_block,
    };
  }
  if (rc == 0 && next_plan != plan_count)
    rc = -EUCLEAN;

  kafs_v7_runtime_data_cow_batch_t *operation = NULL;
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_batch_prepare(ctx->c_v7_runtime_transactions, requests,
                                                plan_count, &operation, plans);
  kafs_v7_inode_t inode;
  memcpy(&inode, mapped, sizeof(inode));
  for (size_t i = 0u; rc == 0 && i < block_count; ++i)
    rc = kafs_v7_fuse_stage_write_reference(ctx, operation, i, &plans[i], retained[i],
                                            extent.first_slot + i, buf, size, offset,
                                            destinations[i]);
  for (size_t i = 0u; rc == 0 && i < lower_leaf_count; ++i)
  {
    size_t plan_id = lower_leaf_plan_start + i;
    size_t leaf = lower_leaves[i];
    rc = kafs_v7_fuse_stage_index_reference(operation, plan_id, &plans[plan_id],
                                            double_view.double_leaves[leaf],
                                            &double_view.double_root[leaf]);
  }
  if (rc == 0 && touches_double)
    rc = kafs_v7_fuse_stage_index_reference(
        operation, lower_root_plan, &plans[lower_root_plan], double_view.double_root,
        inode.inline_or_block_refs +
            (KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 1u) * sizeof(uint32_t));
  for (size_t i = 0u; rc == 0 && i < triple_view.leaf_count; ++i)
  {
    kafs_v7_fuse_triple_leaf_t *leaf = &triple_view.leaves[i];
    kafs_v7_fuse_triple_middle_t *middle = NULL;
    for (size_t j = 0u; j < triple_view.middle_count; ++j)
      if (triple_view.middles[j].index == leaf->middle_index)
        middle = &triple_view.middles[j];
    if (!middle)
      rc = -EUCLEAN;
    else
      rc = kafs_v7_fuse_stage_index_reference(operation, leaf->plan_id, &plans[leaf->plan_id],
                                              leaf->references,
                                              &middle->references[leaf->leaf_index]);
  }
  for (size_t i = 0u; rc == 0 && i < triple_view.middle_count; ++i)
  {
    kafs_v7_fuse_triple_middle_t *middle = &triple_view.middles[i];
    rc = kafs_v7_fuse_stage_index_reference(operation, middle->plan_id, &plans[middle->plan_id],
                                            middle->references, &triple_view.root[middle->index]);
  }
  if (rc == 0)
    rc = kafs_v7_fuse_stage_index_reference(
        operation, triple_view.root_plan_id, &plans[triple_view.root_plan_id], triple_view.root,
        inode.inline_or_block_refs +
            (KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 2u) * sizeof(uint32_t));

  uint64_t new_size = extent.request_end > extent.file_size ? extent.request_end : extent.file_size;
  uint64_t new_slots = (new_size - 1u) / ctx->c_v7_block_size + 1u;
  uint32_t expected_blocks = 0u;
  if (rc == 0)
    rc = kafs_v7_fuse_expected_blocks(ctx->c_v7_block_size, new_slots, &expected_blocks);
  if (rc == 0)
  {
    inode.size = htole64(new_size);
    inode.blocks = htole32(expected_blocks);
  }
  rc = kafs_v7_fuse_commit_data_cow_inode(&operation, ino, &inode, rc);

  int retirement_rc = 0;
  if (rc == 0)
  {
    retirement_rc = kafs_v7_fuse_retire_blocks(ctx, shard->group_id, retained, block_count);
    for (size_t i = 0u; i < lower_leaf_count; ++i)
    {
      int retire_rc = kafs_v7_fuse_retire_blocks(
          ctx, shard->group_id, &double_view.double_leaf_blocks[lower_leaves[i]], 1u);
      if (retirement_rc == 0)
        retirement_rc = retire_rc;
    }
    if (touches_double)
    {
      int retire_rc =
          kafs_v7_fuse_retire_blocks(ctx, shard->group_id, &double_view.double_root_block, 1u);
      if (retirement_rc == 0)
        retirement_rc = retire_rc;
    }
    for (size_t i = 0u; i < triple_view.leaf_count; ++i)
    {
      int retire_rc = kafs_v7_fuse_retire_blocks(ctx, shard->group_id,
                                                 &triple_view.leaves[i].logical_block, 1u);
      if (retirement_rc == 0)
        retirement_rc = retire_rc;
    }
    for (size_t i = 0u; i < triple_view.middle_count; ++i)
    {
      int retire_rc = kafs_v7_fuse_retire_blocks(ctx, shard->group_id,
                                                 &triple_view.middles[i].logical_block, 1u);
      if (retirement_rc == 0)
        retirement_rc = retire_rc;
    }
    int retire_rc = kafs_v7_fuse_retire_blocks(ctx, shard->group_id, &triple_view.root_block, 1u);
    if (retirement_rc == 0)
      retirement_rc = retire_rc;
  }
  if (rc == 0 && result)
  {
    result->new_logical_block = plans[0].logical_block;
    result->retained_logical_block = retained[0];
    result->retirement_rc = retirement_rc;
  }
  if (operation)
    (void)kafs_v7_runtime_data_cow_batch_abort(&operation);
  free(requests);
  free(plans);
  free(destinations);
  free(retained);
  kafs_v7_fuse_triple_view_clear(&triple_view);
  kafs_v7_fuse_double_view_clear(&double_view);
  return rc == 0 ? (int)size : rc;
}

static int kafs_v7_fuse_promote_inline_regular(kafs_context_t *ctx, kafs_inocnt_t ino,
                                               const kafs_v7_inode_t *mapped, const void *buf,
                                               size_t size, uint64_t offset,
                                               kafs_v7_fuse_write_result_t *result)
{
  const kafs_v7_inode_runtime_shard_t *shard = kafs_ctx_v7_inode_shard_for_ino(ctx, ino);
  uint64_t old_size = le64toh(mapped->size);
  uint64_t request_end = offset + size;
  if (!shard || ctx->c_v7_block_size == 0u || request_end > ctx->c_v7_block_size ||
      old_size > ctx->c_v7_block_size)
    return -EOPNOTSUPP;

  kafs_v7_runtime_data_cow_request_t request = {
      .group_id = shard->group_id,
      .retained_logical_block = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK,
  };
  kafs_v7_runtime_data_cow_batch_t *operation = NULL;
  kafs_v7_runtime_data_cow_plan_t plan;
  int rc = kafs_v7_runtime_data_cow_batch_prepare(ctx->c_v7_runtime_transactions, &request, 1u,
                                                  &operation, &plan);
  uint8_t *block = NULL;
  if (rc == 0)
  {
    block = calloc(1u, plan.block_size);
    if (!block)
      rc = -ENOMEM;
  }
  if (rc == 0)
  {
    memcpy(block, mapped->inline_or_block_refs, (size_t)old_size);
    memcpy(block + offset, buf, size);
    rc = kafs_v7_runtime_data_cow_batch_stage(operation, 0u, block, plan.block_size);
  }
  free(block);

  kafs_v7_inode_t inode;
  memcpy(&inode, mapped, sizeof(inode));
  if (rc == 0 && plan.logical_block >= UINT32_MAX)
    rc = -ERANGE;
  if (rc == 0)
  {
    uint32_t reference = htole32((uint32_t)plan.logical_block + 1u);
    memset(inode.inline_or_block_refs, 0, sizeof(inode.inline_or_block_refs));
    memcpy(inode.inline_or_block_refs, &reference, sizeof(reference));
    inode.size = htole64(request_end);
    inode.blocks = htole32(1u);
  }
  rc = kafs_v7_fuse_commit_data_cow_inode(&operation, ino, &inode, rc);
  if (rc != 0)
    return rc;

  if (result)
  {
    result->new_logical_block = plan.logical_block;
    result->retained_logical_block = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK;
    result->retirement_rc = 0;
  }
  return (int)size;
}

int kafs_v7_fuse_write_regular(kafs_context_t *ctx, kafs_inocnt_t ino, const void *buf, size_t size,
                               uint64_t offset, kafs_v7_fuse_write_result_t *result)
{
  if (result)
    memset(result, 0, sizeof(*result));
  if (!buf)
    return -EINVAL;

  if (ctx && ctx->c_v7_block_size != 0u && size != 0u && size <= UINT64_MAX - offset)
  {
    uint64_t single_capacity = 0u;
    uint64_t double_capacity = 0u;
    uint64_t triple_capacity = 0u;
    int capacity_rc = kafs_v7_fuse_triple_capacity(ctx->c_v7_block_size, &single_capacity,
                                                   &double_capacity, &triple_capacity);
    if (capacity_rc != 0)
      return capacity_rc;
    uint64_t last_slot = (offset + size - 1u) / ctx->c_v7_block_size;
    if (last_slot >= double_capacity)
      return kafs_v7_fuse_write_triple_indirect(ctx, ino, buf, size, offset, result);
    if (last_slot >= single_capacity)
      return kafs_v7_fuse_write_double_indirect(ctx, ino, buf, size, offset, result);
    if (last_slot >= KAFS_V7_INODE_DIRECT_REFERENCE_COUNT)
      return kafs_v7_fuse_write_single_indirect(ctx, ino, buf, size, offset, result);
  }

  if (ctx && ctx->c_v7_runtime_transactions && kafs_v7_fuse_policy_controlled_write_active(ctx))
  {
    const kafs_v7_inode_t *mapped = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(ctx, ino);
    uint64_t old_size = mapped ? le64toh(mapped->size) : 0u;
    uint64_t inline_capacity = sizeof(mapped->inline_or_block_refs);
    if (mapped && S_ISREG(le16toh(mapped->mode)) && old_size <= inline_capacity &&
        le32toh(mapped->blocks) == 0u && offset <= old_size && size <= UINT64_MAX - offset &&
        offset + size <= inline_capacity)
    {
      kafs_v7_inode_t inode;
      memcpy(&inode, mapped, sizeof(inode));
      memcpy(inode.inline_or_block_refs + offset, buf, size);
      if (offset + size > old_size)
        inode.size = htole64(offset + size);
      kafs_v7_journal_patch_t inode_patch = {
          .target_type = KAFS_V7_JOURNAL_TARGET_INODE,
          .logical_index = ino,
          .patch_bytes = sizeof(inode),
          .patch = &inode,
      };
      kafs_v7_runtime_transaction_result_t transaction;
      int inline_rc = kafs_v7_runtime_transaction_commit(ctx->c_v7_runtime_transactions,
                                                         &inode_patch, 1u, &transaction);
      return inline_rc == 0 ? (int)size : inline_rc;
    }
    if (mapped && S_ISREG(le16toh(mapped->mode)) && old_size <= inline_capacity &&
        le32toh(mapped->blocks) == 0u && offset <= old_size && size <= UINT64_MAX - offset &&
        offset + size > inline_capacity)
      return kafs_v7_fuse_promote_inline_regular(ctx, ino, mapped, buf, size, offset, result);
    if (mapped && old_size <= inline_capacity && le32toh(mapped->blocks) == 0u)
      return -EOPNOTSUPP;
  }

  const kafs_v7_inode_t *mapped_inode = NULL;
  uint32_t group_id = 0u;
  uint32_t first_slot = 0u;
  size_t block_count = 0u;
  uint64_t retained[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT] = {0};
  int rc = kafs_v7_fuse_write_validate(ctx, ino, size, offset, &mapped_inode, &group_id,
                                       &first_slot, &block_count, retained);
  if (rc != 0)
    return rc;
  if (block_count == 0u)
    return -EUCLEAN;

  {
    kafs_v7_runtime_data_cow_request_t requests[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT] = {0};
    kafs_v7_runtime_data_cow_plan_t plans[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT] = {0};
    for (size_t i = 0; i < block_count; ++i)
      requests[i] = (kafs_v7_runtime_data_cow_request_t){.group_id = group_id,
                                                         .retained_logical_block = retained[i]};
    kafs_v7_runtime_data_cow_batch_t *operation = NULL;
    rc = kafs_v7_runtime_data_cow_batch_prepare(ctx->c_v7_runtime_transactions, requests,
                                                block_count, &operation, plans);
    kafs_v7_inode_t inode;
    memcpy(&inode, mapped_inode, sizeof(inode));
    for (size_t i = 0; rc == 0 && i < block_count; ++i)
    {
      uint64_t slot = (uint64_t)first_slot + i;
      void *destination = inode.inline_or_block_refs + slot * sizeof(uint32_t);
      rc = kafs_v7_fuse_stage_write_reference(ctx, operation, i, &plans[i], retained[i], slot, buf,
                                              size, offset, destination);
    }
    uint64_t request_end = offset + size;
    uint64_t old_size = le64toh(inode.size);
    if (rc == 0 && request_end > old_size)
      inode.size = htole64(request_end);
    uint32_t added_blocks = 0u;
    for (size_t i = 0; i < block_count; ++i)
      if (retained[i] == KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK)
        ++added_blocks;
    if (rc == 0 && added_blocks > UINT32_MAX - le32toh(inode.blocks))
      rc = -EOVERFLOW;
    if (rc == 0)
      inode.blocks = htole32(le32toh(inode.blocks) + added_blocks);
    rc = kafs_v7_fuse_commit_data_cow_inode(&operation, ino, &inode, rc);
    if (rc != 0)
      return rc;
    int retirement_rc = 0;
    for (size_t i = 0; i < block_count; ++i)
    {
      if (retained[i] == KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK)
        continue;
      kafs_v7_runtime_data_retirement_request_t retirement = {.group_id = group_id,
                                                              .logical_block = retained[i]};
      kafs_v7_runtime_data_retirement_result_t retirement_result;
      int retire_rc = kafs_v7_runtime_data_retire(ctx->c_v7_runtime_transactions, &retirement,
                                                  &retirement_result);
      if (retirement_rc == 0 && retire_rc != 0)
        retirement_rc = retire_rc;
    }
    if (result)
    {
      result->new_logical_block = plans[0].logical_block;
      result->retained_logical_block = retained[0];
      result->retirement_rc = retirement_rc;
    }
    return (int)size;
  }
}

static uint32_t kafs_v7_fuse_inode_reference(const kafs_v7_inode_t *inode, uint32_t slot)
{
  uint32_t reference = 0u;
  memcpy(&reference, inode->inline_or_block_refs + slot * sizeof(reference), sizeof(reference));
  return le32toh(reference);
}

static uint32_t kafs_v7_fuse_name_hash(const char *name, size_t bytes)
{
  uint32_t hash = 2166136261u;
  for (size_t i = 0u; i < bytes; ++i)
  {
    hash ^= (uint8_t)name[i];
    hash *= 16777619u;
  }
  return hash;
}

static kafs_inocnt_t kafs_v7_fuse_find_free_inode(kafs_context_t *ctx,
                                                  const kafs_v7_inode_runtime_shard_t *shard)
{
  for (uint64_t candidate = shard->logical_start;
       candidate < shard->logical_start + shard->logical_count; ++candidate)
  {
    if (candidate <= KAFS_INO_ROOTDIR)
      continue;
    const kafs_v7_inode_t *inode =
        (const kafs_v7_inode_t *)kafs_ctx_v7_inode(ctx, (kafs_inocnt_t)candidate);
    if (inode && le16toh(inode->mode) == 0u)
      return (kafs_inocnt_t)candidate;
  }
  return 0u;
}

static int kafs_v7_fuse_directory_append(uint8_t *payload, size_t capacity, const char *name,
                                         size_t name_bytes, kafs_inocnt_t ino, size_t *new_size)
{
  if (capacity < KAFS_V7_KDIR_HEADER_BYTES)
    return -EUCLEAN;
  kafs_v7_kdir_header_t *header = (kafs_v7_kdir_header_t *)payload;
  if (le32toh(header->magic) != KAFS_V7_KDIR_MAGIC ||
      le16toh(header->version) != KAFS_V7_KDIR_VERSION)
    return -EUCLEAN;
  size_t used = KAFS_V7_KDIR_HEADER_BYTES + le32toh(header->record_bytes);
  size_t record_bytes = KAFS_V7_KDIR_RECORD_PREFIX_BYTES + name_bytes;
  if (used > capacity || record_bytes > capacity - used)
    return -ENOSPC;
  for (size_t offset = KAFS_V7_KDIR_HEADER_BYTES; offset < used;)
  {
    if (used - offset < KAFS_V7_KDIR_RECORD_PREFIX_BYTES)
      return -EUCLEAN;
    const kafs_v7_kdir_record_t *existing = (const kafs_v7_kdir_record_t *)(payload + offset);
    size_t existing_bytes = le16toh(existing->record_length);
    size_t existing_name_bytes = le16toh(existing->name_bytes);
    if (existing_bytes < KAFS_V7_KDIR_RECORD_PREFIX_BYTES || existing_bytes > used - offset ||
        existing_name_bytes > existing_bytes - KAFS_V7_KDIR_RECORD_PREFIX_BYTES)
      return -EUCLEAN;
    if ((le16toh(existing->flags) & KAFS_V7_KDIR_FLAG_TOMBSTONE) == 0u &&
        existing_name_bytes == name_bytes && memcmp(existing->name, name, name_bytes) == 0)
      return -EEXIST;
    offset += existing_bytes;
  }
  kafs_v7_kdir_record_t *record = (kafs_v7_kdir_record_t *)(payload + used);
  memset(record, 0, record_bytes);
  record->record_length = htole16((uint16_t)record_bytes);
  record->inode = htole32((uint32_t)ino);
  record->name_bytes = htole16((uint16_t)name_bytes);
  record->name_hash = htole32(kafs_v7_fuse_name_hash(name, name_bytes));
  memcpy(record->name, name, name_bytes);
  header->live_count = htole32(le32toh(header->live_count) + 1u);
  header->record_bytes = htole32(le32toh(header->record_bytes) + (uint32_t)record_bytes);
  *new_size = used + record_bytes;
  return 0;
}

static int
kafs_v7_fuse_validate_direct_references(const kafs_v7_inode_t *inode, uint32_t block_count,
                                        uint64_t retained[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT])
{
  for (uint32_t slot = 0u; slot < KAFS_V7_INODE_REFERENCE_COUNT; ++slot)
  {
    uint32_t reference = kafs_v7_fuse_inode_reference(inode, slot);
    if ((slot < block_count) != (reference != 0u))
      return -EOPNOTSUPP;
    if (slot < block_count)
      retained[slot] = (uint64_t)reference - 1u;
  }
  return 0;
}

static int kafs_v7_fuse_direct_payload_bytes(uint32_t block_size, uint32_t block_count,
                                             size_t *bytes_out)
{
  if (block_size == 0u || block_count > SIZE_MAX / block_size)
    return -EOPNOTSUPP;
  *bytes_out = (size_t)block_size * block_count;
  return 0;
}

static int
kafs_v7_fuse_retire_direct_blocks(kafs_context_t *ctx, uint32_t group_id,
                                  const uint64_t retained[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT],
                                  size_t block_count)
{
  int retirement_rc = 0;
  for (size_t i = 0u; i < block_count; ++i)
  {
    kafs_v7_runtime_data_retirement_request_t retirement = {
        .group_id = group_id,
        .logical_block = retained[i],
    };
    kafs_v7_runtime_data_retirement_result_t retirement_result;
    int current_rc = kafs_v7_runtime_data_retire(ctx->c_v7_runtime_transactions, &retirement,
                                                 &retirement_result);
    if (retirement_rc == 0 && current_rc != 0)
      retirement_rc = current_rc;
  }
  return retirement_rc;
}

static int kafs_v7_fuse_load_direct_directory(
    kafs_context_t *ctx, const kafs_v7_inode_t *mapped_parent, const char *name, size_t name_bytes,
    kafs_inocnt_t ino, uint64_t retained[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT],
    uint8_t **payload_out, size_t *block_count_out, size_t *new_size_out)
{
  uint32_t parent_blocks = le32toh(mapped_parent->blocks);
  if (parent_blocks == 0u || parent_blocks > KAFS_V7_INODE_DIRECT_REFERENCE_COUNT ||
      le64toh(mapped_parent->size) > (uint64_t)ctx->c_v7_block_size * parent_blocks)
    return -EOPNOTSUPP;

  int rc = kafs_v7_fuse_validate_direct_references(mapped_parent, parent_blocks, retained);
  if (rc != 0)
    return rc;

  size_t payload_bytes = 0u;
  rc = kafs_v7_fuse_direct_payload_bytes(ctx->c_v7_block_size, parent_blocks, &payload_bytes);
  if (rc != 0)
    return rc;
  uint8_t *payload = malloc(payload_bytes);
  if (!payload)
    return -ENOMEM;
  for (size_t i = 0u; rc == 0 && i < parent_blocks; ++i)
  {
    uint64_t physical_off = 0u;
    rc = kafs_ctx_v7_data_ref_physical_offset(ctx, (kafs_blkcnt_t)retained[i] + 1u, &physical_off);
    if (rc == 0)
      rc = kafs_pread_all(ctx->c_fd, payload + i * ctx->c_v7_block_size, ctx->c_v7_block_size,
                          (off_t)physical_off);
  }

  size_t new_size = 0u;
  if (rc == 0)
    rc = kafs_v7_fuse_directory_append(payload, payload_bytes, name, name_bytes, ino, &new_size);
  size_t plan_count = parent_blocks;
  if (rc == -ENOSPC && parent_blocks < KAFS_V7_INODE_DIRECT_REFERENCE_COUNT)
  {
    size_t grown_bytes = payload_bytes + ctx->c_v7_block_size;
    uint8_t *grown = realloc(payload, grown_bytes);
    if (!grown)
      rc = -ENOMEM;
    else
    {
      payload = grown;
      memset(payload + payload_bytes, 0, ctx->c_v7_block_size);
      payload_bytes = grown_bytes;
      plan_count++;
      rc = kafs_v7_fuse_directory_append(payload, payload_bytes, name, name_bytes, ino, &new_size);
    }
  }
  if (rc != 0)
  {
    free(payload);
    return rc;
  }

  *payload_out = payload;
  *block_count_out = plan_count;
  *new_size_out = new_size;
  return 0;
}

static int kafs_v7_fuse_create_in_direct_blocks(kafs_context_t *ctx,
                                                const kafs_v7_inode_runtime_shard_t *parent_shard,
                                                const kafs_v7_inode_t *mapped_parent,
                                                kafs_inocnt_t parent_ino,
                                                const kafs_v7_inode_t *child, kafs_inocnt_t ino,
                                                const char *name, size_t name_bytes,
                                                kafs_inocnt_t *ino_out, int *retirement_rc_out)
{
  uint32_t parent_blocks = le32toh(mapped_parent->blocks);
  uint64_t retained[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT];
  uint8_t *payload = NULL;
  size_t plan_count = 0u;
  size_t new_size = 0u;
  int rc = kafs_v7_fuse_load_direct_directory(ctx, mapped_parent, name, name_bytes, ino, retained,
                                              &payload, &plan_count, &new_size);
  if (rc != 0)
    return rc;

  kafs_v7_runtime_data_cow_request_t requests[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT];
  for (size_t i = 0u; i < plan_count; ++i)
    requests[i] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = parent_shard->group_id,
        .retained_logical_block =
            i < parent_blocks ? retained[i] : KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK,
    };
  kafs_v7_runtime_data_cow_batch_t *batch = NULL;
  kafs_v7_runtime_data_cow_plan_t plans[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT];
  rc = kafs_v7_runtime_data_cow_batch_prepare(ctx->c_v7_runtime_transactions, requests, plan_count,
                                              &batch, plans);
  for (size_t i = 0u; rc == 0 && i < plan_count; ++i)
    rc = kafs_v7_runtime_data_cow_batch_stage(batch, i, payload + i * ctx->c_v7_block_size,
                                              ctx->c_v7_block_size);
  free(payload);

  kafs_v7_inode_t parent;
  memcpy(&parent, mapped_parent, sizeof(parent));
  for (size_t i = 0u; rc == 0 && i < plan_count; ++i)
    if (plans[i].logical_block >= UINT32_MAX)
      rc = -ERANGE;
  if (rc == 0)
  {
    memset(parent.inline_or_block_refs, 0, sizeof(parent.inline_or_block_refs));
    for (size_t i = 0u; i < plan_count; ++i)
    {
      uint32_t reference = htole32((uint32_t)plans[i].logical_block + 1u);
      memcpy(parent.inline_or_block_refs + i * sizeof(reference), &reference, sizeof(reference));
    }
    parent.size = htole64(new_size);
    parent.blocks = htole32((uint32_t)plan_count);
  }
  kafs_v7_journal_patch_t patches[2] = {
      {.target_type = KAFS_V7_JOURNAL_TARGET_INODE,
       .logical_index = parent_ino,
       .patch_bytes = sizeof(parent),
       .patch = &parent},
      {.target_type = KAFS_V7_JOURNAL_TARGET_INODE,
       .logical_index = ino,
       .patch_bytes = sizeof(*child),
       .patch = child,
       .free_inodes_delta = -1},
  };
  kafs_v7_runtime_transaction_result_t transaction;
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_batch_commit(&batch, patches, 2u, &transaction);
  if (batch)
    (void)kafs_v7_runtime_data_cow_batch_abort(&batch);
  if (rc != 0)
    return rc;

  int retirement_rc =
      kafs_v7_fuse_retire_direct_blocks(ctx, parent_shard->group_id, retained, parent_blocks);
  if (retirement_rc_out)
    *retirement_rc_out = retirement_rc;
  *ino_out = ino;
  return 0;
}

int kafs_v7_fuse_create_in_direct_directory(kafs_context_t *ctx, kafs_inocnt_t parent_ino,
                                            const char *name, uint16_t mode, uint16_t uid,
                                            uint16_t gid, kafs_inocnt_t *ino_out,
                                            int *retirement_rc_out)
{
  if (retirement_rc_out)
    *retirement_rc_out = 0;
  if (!ctx || !name || !ino_out || !ctx->c_v7_runtime_transactions)
    return -EINVAL;
  size_t name_bytes = strlen(name);
  if (name_bytes == 0u || name_bytes > 255u)
    return -ENAMETOOLONG;
  const kafs_v7_inode_runtime_shard_t *parent_shard =
      kafs_ctx_v7_inode_shard_for_ino(ctx, parent_ino);
  const kafs_v7_inode_t *mapped_parent =
      (const kafs_v7_inode_t *)kafs_ctx_v7_inode(ctx, parent_ino);
  if (!parent_shard || !mapped_parent || !S_ISDIR(le16toh(mapped_parent->mode)))
    return -EOPNOTSUPP;
  kafs_inocnt_t ino = kafs_v7_fuse_find_free_inode(ctx, parent_shard);
  if (ino == 0u)
    return -ENOSPC;
  kafs_v7_inode_t child;
  memset(&child, 0, sizeof(child));
  child.mode = htole16((uint16_t)(S_IFREG | (mode & 07777u)));
  child.uid = htole16(uid);
  child.gid = htole16(gid);
  child.link_count = htole16(1u);

  if (le32toh(mapped_parent->blocks) == 0u &&
      le64toh(mapped_parent->size) <= sizeof(mapped_parent->inline_or_block_refs))
  {
    kafs_v7_inode_t parent;
    memcpy(&parent, mapped_parent, sizeof(parent));
    size_t new_size = 0u;
    int rc = kafs_v7_fuse_directory_append(parent.inline_or_block_refs,
                                           sizeof(parent.inline_or_block_refs), name, name_bytes,
                                           ino, &new_size);
    if (rc != 0 && rc != -ENOSPC)
      return rc;
    if (rc == 0)
    {
      parent.size = htole64(new_size);
      kafs_v7_journal_patch_t patches[2] = {
          {.target_type = KAFS_V7_JOURNAL_TARGET_INODE,
           .logical_index = parent_ino,
           .patch_bytes = sizeof(parent),
           .patch = &parent},
          {.target_type = KAFS_V7_JOURNAL_TARGET_INODE,
           .logical_index = ino,
           .patch_bytes = sizeof(child),
           .patch = &child,
           .free_inodes_delta = -1},
      };
      kafs_v7_runtime_transaction_result_t transaction;
      rc = kafs_v7_runtime_transaction_commit(ctx->c_v7_runtime_transactions, patches, 2u,
                                              &transaction);
      if (rc == 0)
        *ino_out = ino;
      return rc;
    }

    kafs_v7_runtime_data_cow_request_t request = {
        .group_id = parent_shard->group_id,
        .retained_logical_block = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK,
    };
    kafs_v7_runtime_data_cow_t *operation = NULL;
    kafs_v7_runtime_data_cow_plan_t plan;
    rc = kafs_v7_runtime_data_cow_prepare(ctx->c_v7_runtime_transactions, &request, &operation,
                                          &plan);
    uint8_t *block = rc == 0 ? calloc(1u, plan.block_size) : NULL;
    if (rc == 0 && !block)
      rc = -ENOMEM;
    if (rc == 0)
    {
      memcpy(block, mapped_parent->inline_or_block_refs,
             sizeof(mapped_parent->inline_or_block_refs));
      rc = kafs_v7_fuse_directory_append(block, plan.block_size, name, name_bytes, ino, &new_size);
    }
    if (rc == 0)
      rc = kafs_v7_runtime_data_cow_stage(operation, block, plan.block_size);
    free(block);
    if (rc == 0 && plan.logical_block >= UINT32_MAX)
      rc = -ERANGE;
    if (rc == 0)
    {
      memset(parent.inline_or_block_refs, 0, sizeof(parent.inline_or_block_refs));
      uint32_t reference = htole32((uint32_t)plan.logical_block + 1u);
      memcpy(parent.inline_or_block_refs, &reference, sizeof(reference));
      parent.size = htole64(new_size);
      parent.blocks = htole32(1u);
    }
    kafs_v7_journal_patch_t patches[2] = {
        {.target_type = KAFS_V7_JOURNAL_TARGET_INODE,
         .logical_index = parent_ino,
         .patch_bytes = sizeof(parent),
         .patch = &parent},
        {.target_type = KAFS_V7_JOURNAL_TARGET_INODE,
         .logical_index = ino,
         .patch_bytes = sizeof(child),
         .patch = &child,
         .free_inodes_delta = -1},
    };
    kafs_v7_runtime_data_cow_result_t cow_result;
    if (rc == 0)
      rc = kafs_v7_runtime_data_cow_commit(&operation, patches, 2u, &cow_result);
    if (operation)
      (void)kafs_v7_runtime_data_cow_abort(&operation);
    if (rc == 0)
      *ino_out = ino;
    return rc;
  }
  return kafs_v7_fuse_create_in_direct_blocks(ctx, parent_shard, mapped_parent, parent_ino, &child,
                                              ino, name, name_bytes, ino_out, retirement_rc_out);
}

static int kafs_v7_fuse_truncate_single_indirect(kafs_context_t *ctx, kafs_inocnt_t ino,
                                                 uint64_t size,
                                                 kafs_v7_fuse_truncate_result_t *result)
{
  kafs_v7_fuse_truncate_state_t state;
  int state_rc = kafs_v7_fuse_truncate_state_init(ctx, ino, size, &state);
  if (state_rc != 0)
    return state_rc;
  const kafs_v7_inode_runtime_shard_t *shard = state.shard;
  const kafs_v7_inode_t *mapped = state.inode;
  uint64_t old_size = state.old_size;
  uint64_t old_slots = state.old_slots;
  uint64_t keep_slots = state.keep_slots;
  uint64_t references_per_block = ctx->c_v7_block_size / sizeof(uint32_t);
  uint64_t single_capacity = KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + references_per_block;
  if (size > old_size || old_slots <= KAFS_V7_INODE_DIRECT_REFERENCE_COUNT ||
      old_slots > single_capacity || (size != 0u && size <= sizeof(mapped->inline_or_block_refs)) ||
      kafs_v7_fuse_inode_reference(mapped, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 1u) != 0u ||
      kafs_v7_fuse_inode_reference(mapped, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 2u) != 0u)
    return -EOPNOTSUPP;
  uint32_t root_reference =
      kafs_v7_fuse_inode_reference(mapped, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT);
  if (root_reference == 0u)
    return -EUCLEAN;
  uint64_t old_root = (uint64_t)root_reference - 1u;
  uint32_t *root = malloc(ctx->c_v7_block_size);
  uint64_t *retired = malloc((size_t)(old_slots + 1u) * sizeof(*retired));
  if (!root || !retired)
  {
    free(root);
    free(retired);
    return -ENOMEM;
  }
  int rc = kafs_v7_fuse_read_logical_block(ctx, old_root, root);
  size_t retired_count = 0u;
  for (uint64_t slot = keep_slots; rc == 0 && slot < old_slots; ++slot)
  {
    uint32_t reference = slot < KAFS_V7_INODE_DIRECT_REFERENCE_COUNT
                             ? kafs_v7_fuse_inode_reference(mapped, (uint32_t)slot)
                             : le32toh(root[slot - KAFS_V7_INODE_DIRECT_REFERENCE_COUNT]);
    if (reference == 0u)
      rc = -EUCLEAN;
    else
      retired[retired_count++] = (uint64_t)reference - 1u;
  }

  kafs_v7_inode_t inode;
  memcpy(&inode, mapped, sizeof(inode));
  kafs_v7_fuse_clear_direct_references(&inode, keep_slots);
  uint64_t indirect_keep = keep_slots > KAFS_V7_INODE_DIRECT_REFERENCE_COUNT
                               ? keep_slots - KAFS_V7_INODE_DIRECT_REFERENCE_COUNT
                               : 0u;
  for (uint64_t index = indirect_keep; index < references_per_block; ++index)
    root[index] = 0u;
  inode.size = htole64(size);
  inode.blocks = htole32((uint32_t)(keep_slots + (indirect_keep != 0u)));

  uint32_t tail_bytes = (uint32_t)(size % ctx->c_v7_block_size);
  int cow_tail = tail_bytes != 0u;
  int cow_root = indirect_keep != 0u;
  size_t plan_count = (size_t)cow_tail + (size_t)cow_root;
  uint64_t tail_retained = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK;
  kafs_v7_runtime_data_cow_batch_t *operation = NULL;
  kafs_v7_runtime_data_cow_plan_t plans[2];
  if (rc == 0 && plan_count != 0u)
  {
    kafs_v7_runtime_data_cow_request_t requests[2];
    size_t request_id = 0u;
    if (cow_tail)
    {
      uint64_t tail_slot = keep_slots - 1u;
      uint32_t reference = tail_slot < KAFS_V7_INODE_DIRECT_REFERENCE_COUNT
                               ? kafs_v7_fuse_inode_reference(mapped, (uint32_t)tail_slot)
                               : le32toh(root[tail_slot - KAFS_V7_INODE_DIRECT_REFERENCE_COUNT]);
      if (reference == 0u)
        rc = -EUCLEAN;
      else
        tail_retained = (uint64_t)reference - 1u;
      requests[request_id++] = (kafs_v7_runtime_data_cow_request_t){
          .group_id = shard->group_id,
          .retained_logical_block = tail_retained,
      };
    }
    if (cow_root)
      requests[request_id++] = (kafs_v7_runtime_data_cow_request_t){
          .group_id = shard->group_id,
          .retained_logical_block = old_root,
      };
    if (rc == 0)
      rc = kafs_v7_runtime_data_cow_batch_prepare(ctx->c_v7_runtime_transactions, requests,
                                                  plan_count, &operation, plans);
  }
  size_t plan_id = 0u;
  if (rc == 0 && cow_tail)
  {
    uint8_t *block = malloc(plans[plan_id].block_size);
    if (!block)
      rc = -ENOMEM;
    if (rc == 0)
      rc = kafs_v7_fuse_read_logical_block(ctx, tail_retained, block);
    if (rc == 0)
    {
      memset(block + tail_bytes, 0, plans[plan_id].block_size - tail_bytes);
      rc = kafs_v7_runtime_data_cow_batch_stage(operation, plan_id, block,
                                                plans[plan_id].block_size);
    }
    free(block);
    if (rc == 0 && plans[plan_id].logical_block >= UINT32_MAX)
      rc = -ERANGE;
    if (rc == 0)
    {
      uint32_t reference = htole32((uint32_t)plans[plan_id].logical_block + 1u);
      uint64_t tail_slot = keep_slots - 1u;
      if (tail_slot < KAFS_V7_INODE_DIRECT_REFERENCE_COUNT)
        memcpy(inode.inline_or_block_refs + tail_slot * sizeof(reference), &reference,
               sizeof(reference));
      else
        root[tail_slot - KAFS_V7_INODE_DIRECT_REFERENCE_COUNT] = reference;
    }
    ++plan_id;
  }
  if (rc == 0 && cow_root)
    rc = kafs_v7_fuse_stage_index_reference(
        operation, plan_id, &plans[plan_id], root,
        inode.inline_or_block_refs + KAFS_V7_INODE_DIRECT_REFERENCE_COUNT * sizeof(uint32_t));
  if (!cow_root)
    memset(inode.inline_or_block_refs + KAFS_V7_INODE_DIRECT_REFERENCE_COUNT * sizeof(uint32_t), 0,
           sizeof(uint32_t));

  if (rc == 0)
    rc = kafs_v7_fuse_commit_truncate_inode(ctx, &operation, plan_count, ino, &inode);
  else if (operation)
    (void)kafs_v7_runtime_data_cow_batch_abort(&operation);
  int retirement_rc = 0;
  if (rc == 0)
  {
    if (tail_retained != KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK)
      retired[retired_count++] = tail_retained;
    retired[retired_count++] = old_root;
    retirement_rc = kafs_v7_fuse_retire_blocks(ctx, shard->group_id, retired, retired_count);
  }
  if (rc == 0 && result)
  {
    result->retained_logical_block = tail_retained;
    result->retirement_rc = retirement_rc;
  }
  free(root);
  free(retired);
  return rc;
}

static int kafs_v7_fuse_truncate_double_indirect(kafs_context_t *ctx, kafs_inocnt_t ino,
                                                 uint64_t size,
                                                 kafs_v7_fuse_truncate_result_t *result)
{
  kafs_v7_fuse_truncate_state_t state;
  int state_rc = kafs_v7_fuse_truncate_state_init(ctx, ino, size, &state);
  if (state_rc != 0)
    return state_rc;
  const kafs_v7_inode_runtime_shard_t *shard = state.shard;
  const kafs_v7_inode_t *mapped = state.inode;
  uint64_t old_size = state.old_size;
  uint64_t old_slots = state.old_slots;
  uint64_t keep_slots = state.keep_slots;
  uint64_t single_capacity = 0u;
  uint64_t double_capacity = 0u;
  int rc = kafs_v7_fuse_capacities(ctx->c_v7_block_size, &single_capacity, &double_capacity);
  if (rc != 0)
    return rc;
  if (size > old_size || old_slots <= single_capacity || old_slots > double_capacity ||
      kafs_v7_fuse_inode_reference(mapped, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 2u) != 0u)
    return -EOPNOTSUPP;

  kafs_v7_fuse_double_view_t view;
  rc = kafs_v7_fuse_double_view_init(ctx, mapped, &view);
  uint64_t removed_data = old_slots - keep_slots;
  if (removed_data > SIZE_MAX - view.double_leaf_count - 4u ||
      removed_data + view.double_leaf_count + 4u > SIZE_MAX / sizeof(uint64_t))
    rc = -EOVERFLOW;
  size_t retired_capacity = rc == 0 ? (size_t)removed_data + view.double_leaf_count + 4u : 0u;
  uint64_t *retired = rc == 0 ? malloc(retired_capacity * sizeof(*retired)) : NULL;
  if (rc == 0 && !retired)
    rc = -ENOMEM;
  if (rc != 0)
  {
    free(retired);
    kafs_v7_fuse_double_view_clear(&view);
    return rc;
  }

  size_t retired_count = 0u;
  for (uint64_t slot = keep_slots; rc == 0 && slot < old_slots; ++slot)
  {
    uint32_t reference = 0u;
    rc = kafs_v7_fuse_double_view_reference(&view, mapped, ctx->c_v7_block_size, slot, &reference);
    if (rc == 0 && reference == 0u)
      rc = -EUCLEAN;
    else if (rc == 0)
      retired[retired_count++] = (uint64_t)reference - 1u;
  }

  kafs_v7_inode_t inode;
  memcpy(&inode, mapped, sizeof(inode));
  kafs_v7_fuse_clear_direct_references(&inode, keep_slots);

  uint32_t tail_bytes = (uint32_t)(size % ctx->c_v7_block_size);
  int cow_tail = tail_bytes != 0u;
  int keep_single_root = keep_slots > KAFS_V7_INODE_DIRECT_REFERENCE_COUNT;
  int keep_double_root = keep_slots > single_capacity;
  int cow_single_root =
      keep_single_root && !keep_double_root && (keep_slots < single_capacity || cow_tail);
  uint64_t keep_double_data = keep_double_root ? keep_slots - single_capacity : 0u;
  size_t keep_leaf_count =
      keep_double_root ? (size_t)((keep_double_data - 1u) / view.references_per_block + 1u) : 0u;
  int cow_double_leaf =
      keep_double_root && (cow_tail || keep_double_data % view.references_per_block != 0u);
  int cow_double_root = keep_double_root;

  if (!keep_single_root)
  {
    memset(inode.inline_or_block_refs + KAFS_V7_INODE_DIRECT_REFERENCE_COUNT * sizeof(uint32_t), 0,
           sizeof(uint32_t));
    retired[retired_count++] = view.single_root_block;
  }
  else if (cow_single_root)
  {
    uint64_t single_keep = keep_slots - KAFS_V7_INODE_DIRECT_REFERENCE_COUNT;
    for (uint64_t index = single_keep; index < view.references_per_block; ++index)
      view.single_root[index] = 0u;
    retired[retired_count++] = view.single_root_block;
  }

  size_t cow_leaf = SIZE_MAX;
  if (!keep_double_root)
  {
    memset(inode.inline_or_block_refs +
               (KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 1u) * sizeof(uint32_t),
           0, sizeof(uint32_t));
    for (size_t leaf = 0u; leaf < view.double_leaf_count; ++leaf)
      retired[retired_count++] = view.double_leaf_blocks[leaf];
    retired[retired_count++] = view.double_root_block;
  }
  else
  {
    if (keep_leaf_count == 0u || keep_leaf_count > view.double_leaf_count)
      rc = -EUCLEAN;
    if (rc == 0 && cow_double_leaf)
    {
      cow_leaf = keep_leaf_count - 1u;
      uint64_t leaf_keep = keep_double_data - cow_leaf * view.references_per_block;
      for (uint64_t index = leaf_keep; index < view.references_per_block; ++index)
        view.double_leaves[cow_leaf][index] = 0u;
      retired[retired_count++] = view.double_leaf_blocks[cow_leaf];
    }
    for (size_t leaf = keep_leaf_count; leaf < view.double_leaf_count; ++leaf)
      retired[retired_count++] = view.double_leaf_blocks[leaf];
    for (size_t leaf = keep_leaf_count; leaf < view.references_per_block; ++leaf)
      view.double_root[leaf] = 0u;
    retired[retired_count++] = view.double_root_block;
  }

  uint64_t tail_retained = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK;
  size_t plan_count = (size_t)cow_tail + (size_t)cow_single_root + (size_t)cow_double_leaf +
                      (size_t)cow_double_root;
  kafs_v7_runtime_data_cow_request_t requests[4];
  kafs_v7_runtime_data_cow_plan_t plans[4];
  size_t next_plan = 0u;
  size_t tail_plan = SIZE_MAX;
  size_t single_plan = SIZE_MAX;
  size_t leaf_plan = SIZE_MAX;
  size_t double_plan = SIZE_MAX;
  if (rc == 0 && cow_tail)
  {
    uint32_t reference = 0u;
    rc = kafs_v7_fuse_double_view_reference(&view, mapped, ctx->c_v7_block_size, keep_slots - 1u,
                                            &reference);
    if (rc == 0 && reference == 0u)
      rc = -EUCLEAN;
    if (rc == 0)
      tail_retained = (uint64_t)reference - 1u;
    tail_plan = next_plan++;
    requests[tail_plan] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = shard->group_id,
        .retained_logical_block = tail_retained,
    };
  }
  if (cow_single_root)
  {
    single_plan = next_plan++;
    requests[single_plan] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = shard->group_id,
        .retained_logical_block = view.single_root_block,
    };
  }
  if (cow_double_leaf)
  {
    leaf_plan = next_plan++;
    requests[leaf_plan] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = shard->group_id,
        .retained_logical_block = view.double_leaf_blocks[cow_leaf],
    };
  }
  if (cow_double_root)
  {
    double_plan = next_plan++;
    requests[double_plan] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = shard->group_id,
        .retained_logical_block = view.double_root_block,
    };
  }
  if (rc == 0 && next_plan != plan_count)
    rc = -EUCLEAN;

  kafs_v7_runtime_data_cow_batch_t *operation = NULL;
  if (rc == 0 && plan_count != 0u)
    rc = kafs_v7_runtime_data_cow_batch_prepare(ctx->c_v7_runtime_transactions, requests,
                                                plan_count, &operation, plans);
  if (rc == 0 && cow_tail)
  {
    void *destination = NULL;
    if (rc == 0)
      rc = kafs_v7_fuse_double_view_destination(&view, &inode, ctx->c_v7_block_size,
                                                keep_slots - 1u, &destination);
    if (rc == 0)
      rc = kafs_v7_fuse_stage_truncate_tail(ctx, operation, tail_plan, &plans[tail_plan],
                                            tail_retained, tail_bytes, destination);
  }
  if (rc == 0 && cow_single_root)
    rc = kafs_v7_fuse_stage_index_reference(
        operation, single_plan, &plans[single_plan], view.single_root,
        inode.inline_or_block_refs + KAFS_V7_INODE_DIRECT_REFERENCE_COUNT * sizeof(uint32_t));
  if (rc == 0 && cow_double_leaf)
    rc = kafs_v7_fuse_stage_index_reference(operation, leaf_plan, &plans[leaf_plan],
                                            view.double_leaves[cow_leaf],
                                            &view.double_root[cow_leaf]);
  if (rc == 0 && cow_double_root)
    rc = kafs_v7_fuse_stage_index_reference(
        operation, double_plan, &plans[double_plan], view.double_root,
        inode.inline_or_block_refs +
            (KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 1u) * sizeof(uint32_t));

  uint32_t expected_blocks = 0u;
  if (rc == 0)
    rc = kafs_v7_fuse_expected_blocks(ctx->c_v7_block_size, keep_slots, &expected_blocks);
  if (rc == 0)
  {
    inode.size = htole64(size);
    inode.blocks = htole32(expected_blocks);
  }
  if (rc == 0)
    rc = kafs_v7_fuse_commit_truncate_inode(ctx, &operation, plan_count, ino, &inode);
  else if (operation)
    (void)kafs_v7_runtime_data_cow_batch_abort(&operation);
  if (operation)
    (void)kafs_v7_runtime_data_cow_batch_abort(&operation);
  if (rc == 0 && tail_retained != KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK)
    retired[retired_count++] = tail_retained;
  int retirement_rc =
      rc == 0 ? kafs_v7_fuse_retire_blocks(ctx, shard->group_id, retired, retired_count) : 0;
  if (rc == 0 && result)
  {
    result->retained_logical_block = tail_retained;
    result->retirement_rc = retirement_rc;
  }
  free(retired);
  kafs_v7_fuse_double_view_clear(&view);
  return rc;
}

static int kafs_v7_fuse_retire_indirect_suffix(kafs_context_t *ctx, uint32_t group_id,
                                               uint32_t reference, uint32_t levels,
                                               uint64_t base_slot, uint64_t keep_slot,
                                               uint64_t old_end)
{
  if (!ctx || reference == 0u || levels == 0u || levels > KAFS_V7_INODE_INDIRECT_REFERENCE_COUNT)
    return -EUCLEAN;
  uint64_t references_per_block = ctx->c_v7_block_size / sizeof(uint32_t);
  uint64_t child_span = 1u;
  for (uint32_t level = 1u; level < levels; ++level)
  {
    if (child_span > UINT64_MAX / references_per_block)
      return -EOVERFLOW;
    child_span *= references_per_block;
  }
  uint32_t *references = malloc(ctx->c_v7_block_size);
  if (!references)
    return -ENOMEM;
  int rc = kafs_v7_fuse_read_logical_block(ctx, (uint64_t)reference - 1u, references);
  int retirement_rc = 0;
  for (uint64_t index = 0u; rc == 0 && index < references_per_block; ++index)
  {
    if (index > (UINT64_MAX - base_slot) / child_span)
    {
      rc = -EOVERFLOW;
      break;
    }
    uint64_t child_start = base_slot + index * child_span;
    if (child_start >= old_end)
      break;
    uint64_t child_end =
        child_start > UINT64_MAX - child_span ? UINT64_MAX : child_start + child_span;
    if (child_end <= keep_slot)
      continue;
    uint32_t child_reference = le32toh(references[index]);
    if (child_reference == 0u)
    {
      rc = -EUCLEAN;
      break;
    }
    if (levels == 1u)
    {
      uint64_t logical_block = (uint64_t)child_reference - 1u;
      int retire_rc = kafs_v7_fuse_retire_blocks(ctx, group_id, &logical_block, 1u);
      if (retirement_rc == 0)
        retirement_rc = retire_rc;
      continue;
    }
    int child_rc = kafs_v7_fuse_retire_indirect_suffix(ctx, group_id, child_reference, levels - 1u,
                                                       child_start, keep_slot, old_end);
    if (retirement_rc == 0)
      retirement_rc = child_rc;
    if (child_start >= keep_slot)
    {
      uint64_t logical_block = (uint64_t)child_reference - 1u;
      int retire_rc = kafs_v7_fuse_retire_blocks(ctx, group_id, &logical_block, 1u);
      if (retirement_rc == 0)
        retirement_rc = retire_rc;
    }
  }
  free(references);
  return rc != 0 ? rc : retirement_rc;
}

static int kafs_v7_fuse_retire_triple_suffix(kafs_context_t *ctx, uint32_t group_id,
                                             const kafs_v7_inode_t *inode, uint64_t keep_slots,
                                             uint64_t old_slots)
{
  uint64_t references_per_block = ctx->c_v7_block_size / sizeof(uint32_t);
  uint64_t region_start = 0u;
  int retirement_rc = 0;
  uint64_t direct_end = KAFS_V7_INODE_DIRECT_REFERENCE_COUNT;
  uint64_t direct_limit = old_slots < direct_end ? old_slots : direct_end;
  for (uint64_t slot = keep_slots; slot < direct_limit; ++slot)
  {
    uint32_t reference = kafs_v7_fuse_inode_reference(inode, (uint32_t)slot);
    if (reference == 0u)
      return -EUCLEAN;
    uint64_t logical_block = (uint64_t)reference - 1u;
    int rc = kafs_v7_fuse_retire_blocks(ctx, group_id, &logical_block, 1u);
    if (retirement_rc == 0)
      retirement_rc = rc;
  }
  region_start = direct_end;
  uint64_t region_span = references_per_block;
  for (uint32_t levels = 1u; levels <= KAFS_V7_INODE_INDIRECT_REFERENCE_COUNT; ++levels)
  {
    uint64_t region_end =
        region_start > UINT64_MAX - region_span ? UINT64_MAX : region_start + region_span;
    uint64_t covered_end = old_slots < region_end ? old_slots : region_end;
    if (covered_end > keep_slots && covered_end > region_start)
    {
      uint32_t reference =
          kafs_v7_fuse_inode_reference(inode, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + levels - 1u);
      if (reference == 0u)
        return -EUCLEAN;
      int rc = kafs_v7_fuse_retire_indirect_suffix(ctx, group_id, reference, levels, region_start,
                                                   keep_slots, covered_end);
      if (retirement_rc == 0)
        retirement_rc = rc;
      if (keep_slots <= region_start)
      {
        uint64_t logical_block = (uint64_t)reference - 1u;
        rc = kafs_v7_fuse_retire_blocks(ctx, group_id, &logical_block, 1u);
        if (retirement_rc == 0)
          retirement_rc = rc;
      }
    }
    region_start = region_end;
    if (levels < KAFS_V7_INODE_INDIRECT_REFERENCE_COUNT)
    {
      if (region_span > UINT64_MAX / references_per_block)
        return -EOVERFLOW;
      region_span *= references_per_block;
    }
  }
  return retirement_rc;
}

static int kafs_v7_fuse_truncate_triple_indirect(kafs_context_t *ctx, kafs_inocnt_t ino,
                                                 uint64_t size,
                                                 kafs_v7_fuse_truncate_result_t *result)
{
  kafs_v7_fuse_truncate_state_t state;
  int rc = kafs_v7_fuse_truncate_state_init(ctx, ino, size, &state);
  if (rc != 0)
    return rc;
  uint64_t single_capacity = 0u;
  uint64_t double_capacity = 0u;
  uint64_t triple_capacity = 0u;
  rc = kafs_v7_fuse_triple_capacity(ctx->c_v7_block_size, &single_capacity, &double_capacity,
                                    &triple_capacity);
  uint32_t old_triple_reference =
      kafs_v7_fuse_inode_reference(state.inode, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 2u);
  if (rc == 0 && (state.old_slots <= double_capacity || state.old_slots > triple_capacity ||
                  old_triple_reference == 0u))
    rc = -EOPNOTSUPP;
  if (rc != 0)
    return rc;

  kafs_v7_inode_t inode;
  memcpy(&inode, state.inode, sizeof(inode));
  kafs_v7_fuse_clear_direct_references(&inode, state.keep_slots);
  kafs_v7_block_tree_path_t path;
  memset(&path, 0, sizeof(path));
  if (state.keep_slots != 0u)
    rc = kafs_v7_block_tree_address(ctx->c_v7_block_size, state.keep_slots - 1u, &path);
  if (rc != 0)
    return rc;
  for (uint32_t level = path.level_count + 1u; level <= KAFS_V7_INODE_INDIRECT_REFERENCE_COUNT;
       ++level)
    memset(inode.inline_or_block_refs +
               (KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + level - 1u) * sizeof(uint32_t),
           0, sizeof(uint32_t));

  kafs_v7_fuse_double_view_t double_view;
  memset(&double_view, 0, sizeof(double_view));
  if (path.level_count == 1u || path.level_count == 2u)
    rc = kafs_v7_fuse_double_view_init(ctx, state.inode, &double_view);
  kafs_v7_fuse_triple_view_t triple_view;
  memset(&triple_view, 0, sizeof(triple_view));
  if (rc == 0 && path.level_count == 3u)
    rc = kafs_v7_fuse_triple_view_init(ctx, state.inode, double_capacity, &triple_view);
  if (rc != 0)
  {
    kafs_v7_fuse_triple_view_clear(&triple_view);
    kafs_v7_fuse_double_view_clear(&double_view);
    return rc;
  }

  uint64_t dirty_blocks[3] = {0u, 0u, 0u};
  const void *dirty_buffers[3] = {NULL, NULL, NULL};
  void *dirty_destinations[3] = {NULL, NULL, NULL};
  size_t dirty_count = 0u;
  uint32_t references_per_block = ctx->c_v7_block_size / sizeof(uint32_t);
  uint32_t tail_bytes = (uint32_t)(size % ctx->c_v7_block_size);
  int cow_tail = tail_bytes != 0u;
  void *tail_destination = NULL;
  uint32_t tail_reference = 0u;

  if (state.keep_slots != 0u && path.level_count == 0u)
  {
    tail_reference = kafs_v7_fuse_inode_reference(state.inode, path.inode_slot);
    tail_destination = inode.inline_or_block_refs + path.inode_slot * sizeof(uint32_t);
  }
  else if (path.level_count == 1u)
  {
    for (uint32_t index = path.indices[0] + 1u; index < references_per_block; ++index)
      double_view.single_root[index] = 0u;
    tail_reference = le32toh(double_view.single_root[path.indices[0]]);
    tail_destination = &double_view.single_root[path.indices[0]];
    int dirty = cow_tail || path.indices[0] + 1u < references_per_block;
    if (dirty)
    {
      dirty_blocks[dirty_count] = double_view.single_root_block;
      dirty_buffers[dirty_count] = double_view.single_root;
      dirty_destinations[dirty_count++] =
          inode.inline_or_block_refs + KAFS_V7_INODE_DIRECT_REFERENCE_COUNT * sizeof(uint32_t);
    }
  }
  else if (path.level_count == 2u)
  {
    uint32_t *leaf = double_view.double_leaves[path.indices[0]];
    for (uint32_t index = path.indices[1] + 1u; index < references_per_block; ++index)
      leaf[index] = 0u;
    for (uint32_t index = path.indices[0] + 1u; index < references_per_block; ++index)
      double_view.double_root[index] = 0u;
    tail_reference = le32toh(leaf[path.indices[1]]);
    tail_destination = &leaf[path.indices[1]];
    int leaf_dirty = cow_tail || path.indices[1] + 1u < references_per_block;
    int root_dirty = leaf_dirty || path.indices[0] + 1u < references_per_block;
    if (leaf_dirty)
    {
      dirty_blocks[dirty_count] = double_view.double_leaf_blocks[path.indices[0]];
      dirty_buffers[dirty_count] = leaf;
      dirty_destinations[dirty_count++] = &double_view.double_root[path.indices[0]];
    }
    if (root_dirty)
    {
      dirty_blocks[dirty_count] = double_view.double_root_block;
      dirty_buffers[dirty_count] = double_view.double_root;
      dirty_destinations[dirty_count++] =
          inode.inline_or_block_refs +
          (KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 1u) * sizeof(uint32_t);
    }
  }
  else if (path.level_count == 3u)
  {
    void *destination = NULL;
    rc = kafs_v7_fuse_triple_slot_prepare(ctx, &triple_view, state.keep_slots - 1u, 1,
                                          &tail_reference, &destination);
    kafs_v7_fuse_triple_middle_t *middle = NULL;
    kafs_v7_fuse_triple_leaf_t *leaf = NULL;
    for (size_t i = 0u; i < triple_view.middle_count; ++i)
      if (triple_view.middles[i].index == path.indices[0])
        middle = &triple_view.middles[i];
    for (size_t i = 0u; i < triple_view.leaf_count; ++i)
      if (triple_view.leaves[i].middle_index == path.indices[0] &&
          triple_view.leaves[i].leaf_index == path.indices[1])
        leaf = &triple_view.leaves[i];
    if (rc == 0 && (!middle || !leaf))
      rc = -EUCLEAN;
    if (rc == 0)
    {
      tail_destination = destination;
      for (uint32_t index = path.indices[2] + 1u; index < references_per_block; ++index)
        leaf->references[index] = 0u;
      for (uint32_t index = path.indices[1] + 1u; index < references_per_block; ++index)
        middle->references[index] = 0u;
      for (uint32_t index = path.indices[0] + 1u; index < references_per_block; ++index)
        triple_view.root[index] = 0u;
      int leaf_dirty = cow_tail || path.indices[2] + 1u < references_per_block;
      int middle_dirty = leaf_dirty || path.indices[1] + 1u < references_per_block;
      int root_dirty = middle_dirty || path.indices[0] + 1u < references_per_block;
      if (leaf_dirty)
      {
        dirty_blocks[dirty_count] = leaf->logical_block;
        dirty_buffers[dirty_count] = leaf->references;
        dirty_destinations[dirty_count++] = &middle->references[path.indices[1]];
      }
      if (middle_dirty)
      {
        dirty_blocks[dirty_count] = middle->logical_block;
        dirty_buffers[dirty_count] = middle->references;
        dirty_destinations[dirty_count++] = &triple_view.root[path.indices[0]];
      }
      if (root_dirty)
      {
        dirty_blocks[dirty_count] = triple_view.root_block;
        dirty_buffers[dirty_count] = triple_view.root;
        dirty_destinations[dirty_count++] =
            inode.inline_or_block_refs +
            (KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 2u) * sizeof(uint32_t);
      }
    }
  }
  if (rc == 0 && cow_tail && (tail_reference == 0u || !tail_destination))
    rc = -EUCLEAN;

  uint64_t tail_retained =
      cow_tail ? (uint64_t)tail_reference - 1u : KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK;
  size_t plan_count = (size_t)cow_tail + dirty_count;
  kafs_v7_runtime_data_cow_request_t requests[4];
  kafs_v7_runtime_data_cow_plan_t plans[4];
  size_t next_plan = 0u;
  size_t tail_plan = SIZE_MAX;
  if (rc == 0 && cow_tail)
  {
    tail_plan = next_plan++;
    requests[tail_plan] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = state.shard->group_id,
        .retained_logical_block = tail_retained,
    };
  }
  for (size_t i = 0u; rc == 0 && i < dirty_count; ++i)
    requests[next_plan++] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = state.shard->group_id,
        .retained_logical_block = dirty_blocks[i],
    };
  if (rc == 0 && next_plan != plan_count)
    rc = -EUCLEAN;

  kafs_v7_runtime_data_cow_batch_t *operation = NULL;
  if (rc == 0 && plan_count != 0u)
    rc = kafs_v7_runtime_data_cow_batch_prepare(ctx->c_v7_runtime_transactions, requests,
                                                plan_count, &operation, plans);
  if (rc == 0 && cow_tail)
    rc = kafs_v7_fuse_stage_truncate_tail(ctx, operation, tail_plan, &plans[tail_plan],
                                          tail_retained, tail_bytes, tail_destination);
  for (size_t i = 0u; rc == 0 && i < dirty_count; ++i)
  {
    size_t plan_id = (size_t)cow_tail + i;
    rc = kafs_v7_fuse_stage_index_reference(operation, plan_id, &plans[plan_id], dirty_buffers[i],
                                            dirty_destinations[i]);
  }
  uint32_t expected_blocks = 0u;
  if (rc == 0)
    rc = kafs_v7_fuse_expected_blocks(ctx->c_v7_block_size, state.keep_slots, &expected_blocks);
  if (rc == 0)
  {
    inode.size = htole64(size);
    inode.blocks = htole32(expected_blocks);
    rc = kafs_v7_fuse_commit_truncate_inode(ctx, &operation, plan_count, ino, &inode);
  }
  else if (operation)
    (void)kafs_v7_runtime_data_cow_batch_abort(&operation);

  int retirement_rc = 0;
  if (rc == 0 && cow_tail)
    retirement_rc = kafs_v7_fuse_retire_blocks(ctx, state.shard->group_id, &tail_retained, 1u);
  if (rc == 0 && state.keep_slots < state.old_slots)
  {
    int retire_rc = kafs_v7_fuse_retire_triple_suffix(ctx, state.shard->group_id, state.inode,
                                                      state.keep_slots, state.old_slots);
    if (retirement_rc == 0)
      retirement_rc = retire_rc;
  }
  if (rc == 0)
  {
    int retire_rc =
        kafs_v7_fuse_retire_blocks(ctx, state.shard->group_id, dirty_blocks, dirty_count);
    if (retirement_rc == 0)
      retirement_rc = retire_rc;
  }
  if (operation)
    (void)kafs_v7_runtime_data_cow_batch_abort(&operation);
  if (rc == 0 && result)
  {
    result->retained_logical_block = tail_retained;
    result->retirement_rc = retirement_rc;
  }
  kafs_v7_fuse_triple_view_clear(&triple_view);
  kafs_v7_fuse_double_view_clear(&double_view);
  return rc;
}

int kafs_v7_fuse_truncate_regular(kafs_context_t *ctx, kafs_inocnt_t ino, uint64_t size,
                                  kafs_v7_fuse_truncate_result_t *result)
{
  if (result)
    memset(result, 0, sizeof(*result));
  if (!ctx || !ctx->c_v7_runtime_transactions ||
      !kafs_v7_fuse_policy_controlled_write_active(ctx) || ctx->c_v7_block_size == 0u)
    return -EROFS;

  const kafs_v7_inode_runtime_shard_t *shard = kafs_ctx_v7_inode_shard_for_ino(ctx, ino);
  const kafs_v7_inode_t *mapped = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(ctx, ino);
  if (!shard || !mapped || !S_ISREG(le16toh(mapped->mode)))
    return -EOPNOTSUPP;

  uint64_t old_size = le64toh(mapped->size);
  if (size > old_size)
    return -EOPNOTSUPP;
  if (le32toh(mapped->blocks) != 0u && size != 0u && size <= sizeof(mapped->inline_or_block_refs))
    return -EOPNOTSUPP;
  if (size == old_size)
    return 0;
  uint64_t single_capacity = 0u;
  uint64_t double_capacity = 0u;
  uint64_t triple_capacity = 0u;
  int capacity_rc = kafs_v7_fuse_triple_capacity(ctx->c_v7_block_size, &single_capacity,
                                                 &double_capacity, &triple_capacity);
  if (capacity_rc != 0)
    return capacity_rc;
  uint64_t old_block_count = old_size == 0u ? 0u : (old_size - 1u) / ctx->c_v7_block_size + 1u;
  if (old_block_count > double_capacity ||
      kafs_v7_fuse_inode_reference(mapped, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 2u) != 0u)
    return kafs_v7_fuse_truncate_triple_indirect(ctx, ino, size, result);
  if (old_block_count > single_capacity ||
      kafs_v7_fuse_inode_reference(mapped, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT + 1u) != 0u)
    return kafs_v7_fuse_truncate_double_indirect(ctx, ino, size, result);
  if (old_size > KAFS_V7_INODE_DIRECT_REFERENCE_COUNT * (uint64_t)ctx->c_v7_block_size ||
      kafs_v7_fuse_inode_reference(mapped, KAFS_V7_INODE_DIRECT_REFERENCE_COUNT) != 0u)
    return kafs_v7_fuse_truncate_single_indirect(ctx, ino, size, result);
  for (uint32_t slot = KAFS_V7_INODE_DIRECT_REFERENCE_COUNT; slot < KAFS_V7_INODE_REFERENCE_COUNT;
       ++slot)
    if (kafs_v7_fuse_inode_reference(mapped, slot) != 0u)
      return -EOPNOTSUPP;

  uint32_t old_slots =
      old_size == 0u ? 0u : (uint32_t)((old_size - 1u) / ctx->c_v7_block_size + 1u);
  uint32_t keep_slots = size == 0u ? 0u : (uint32_t)((size - 1u) / ctx->c_v7_block_size + 1u);
  uint64_t retired[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT] = {0};
  size_t retired_count = 0u;
  for (uint32_t slot = 0u; slot < old_slots; ++slot)
  {
    uint32_t reference = kafs_v7_fuse_inode_reference(mapped, slot);
    if (reference == 0u)
      return -EOPNOTSUPP;
    if (slot >= keep_slots)
      retired[retired_count++] = (uint64_t)reference - 1u;
  }

  kafs_v7_inode_t inode;
  memcpy(&inode, mapped, sizeof(inode));
  for (uint32_t slot = keep_slots; slot < old_slots; ++slot)
    memset(inode.inline_or_block_refs + slot * sizeof(uint32_t), 0, sizeof(uint32_t));
  inode.size = htole64(size);
  inode.blocks = htole32(keep_slots);
  kafs_v7_journal_patch_t inode_patch = {
      .target_type = KAFS_V7_JOURNAL_TARGET_INODE,
      .logical_index = ino,
      .patch_bytes = sizeof(inode),
      .patch = &inode,
  };

  int rc = 0;
  uint64_t tail_retained = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK;
  uint32_t tail_bytes = (uint32_t)(size % ctx->c_v7_block_size);
  if (tail_bytes != 0u)
  {
    uint32_t reference = kafs_v7_fuse_inode_reference(mapped, keep_slots - 1u);
    tail_retained = (uint64_t)reference - 1u;
    kafs_v7_runtime_data_cow_request_t request = {
        .group_id = shard->group_id,
        .retained_logical_block = tail_retained,
    };
    kafs_v7_runtime_data_cow_t *operation = NULL;
    kafs_v7_runtime_data_cow_plan_t plan;
    rc = kafs_v7_runtime_data_cow_prepare(ctx->c_v7_runtime_transactions, &request, &operation,
                                          &plan);
    uint8_t *block = NULL;
    if (rc == 0)
    {
      block = malloc(plan.block_size);
      if (!block)
        rc = -ENOMEM;
    }
    uint64_t physical_off = 0u;
    if (rc == 0)
      rc = kafs_ctx_v7_data_ref_physical_offset(ctx, (kafs_blkcnt_t)tail_retained + 1u,
                                                &physical_off);
    if (rc == 0)
      rc = kafs_pread_all(ctx->c_fd, block, plan.block_size, (off_t)physical_off);
    if (rc == 0)
    {
      memset(block + tail_bytes, 0, plan.block_size - tail_bytes);
      rc = kafs_v7_runtime_data_cow_stage(operation, block, plan.block_size);
    }
    free(block);
    if (rc == 0 && plan.logical_block >= UINT32_MAX)
      rc = -ERANGE;
    if (rc == 0)
    {
      uint32_t new_reference = htole32((uint32_t)plan.logical_block + 1u);
      memcpy(inode.inline_or_block_refs + (keep_slots - 1u) * sizeof(new_reference), &new_reference,
             sizeof(new_reference));
      kafs_v7_runtime_data_cow_result_t cow_result;
      rc = kafs_v7_runtime_data_cow_commit(&operation, &inode_patch, 1u, &cow_result);
    }
    if (operation)
      (void)kafs_v7_runtime_data_cow_abort(&operation);
  }
  else
  {
    kafs_v7_runtime_transaction_result_t transaction;
    rc = kafs_v7_runtime_transaction_commit(ctx->c_v7_runtime_transactions, &inode_patch, 1u,
                                            &transaction);
  }
  if (rc != 0)
    return rc;

  int retirement_rc = 0;
  if (tail_retained != KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK)
    retired[retired_count++] = tail_retained;
  for (size_t i = 0u; i < retired_count; ++i)
  {
    kafs_v7_runtime_data_retirement_request_t retirement = {
        .group_id = shard->group_id,
        .logical_block = retired[i],
    };
    kafs_v7_runtime_data_retirement_result_t retirement_result;
    int retire_rc = kafs_v7_runtime_data_retire(ctx->c_v7_runtime_transactions, &retirement,
                                                &retirement_result);
    if (retirement_rc == 0 && retire_rc != 0)
      retirement_rc = retire_rc;
  }
  if (result)
  {
    result->retained_logical_block = tail_retained;
    result->retirement_rc = retirement_rc;
  }
  return 0;
}
