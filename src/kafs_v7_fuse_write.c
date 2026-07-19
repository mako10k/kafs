#include "kafs_v7_fuse_write.h"

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

static int kafs_v7_fuse_write_validate(const kafs_context_t *ctx, kafs_inocnt_t ino, size_t size,
                                       uint64_t offset, const kafs_v7_inode_t **inode_out,
                                       uint32_t *group_id_out, uint32_t *first_slot_out,
                                       size_t *block_count_out, uint64_t retained[12])
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

  uint64_t file_size = le64toh(inode->size);
  if (offset > file_size)
    return -EOPNOTSUPP;
  if (size > UINT64_MAX - offset)
    return -EFBIG;
  uint64_t request_end = offset + size;
  uint64_t first_slot = offset / ctx->c_v7_block_size;
  uint64_t last_slot = (request_end - 1u) / ctx->c_v7_block_size;
  if (first_slot >= 12u || last_slot >= 12u)
    return -EOPNOTSUPP;

  size_t block_count = (size_t)(last_slot - first_slot + 1u);
  for (size_t i = 0; i < block_count; ++i)
  {
    uint32_t reference = 0u;
    memcpy(&reference, inode->inline_or_block_refs + (first_slot + i) * sizeof(reference),
           sizeof(reference));
    reference = le32toh(reference);
    if (reference == 0u)
    {
      uint64_t slot = first_slot + i;
      uint64_t existing_slots = file_size == 0u ? 0u : (file_size - 1u) / ctx->c_v7_block_size + 1u;
      if (slot < existing_slots)
        return -EOPNOTSUPP;
      retained[i] = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK;
    }
    else
      retained[i] = (uint64_t)reference - 1u;
  }

  *inode_out = inode;
  *group_id_out = shard->group_id;
  *first_slot_out = (uint32_t)first_slot;
  *block_count_out = block_count;
  return 0;
}

int kafs_v7_fuse_write_direct(kafs_context_t *ctx, kafs_inocnt_t ino, const void *buf, size_t size,
                              uint64_t offset, kafs_v7_fuse_write_result_t *result)
{
  if (result)
    memset(result, 0, sizeof(*result));
  if (!buf)
    return -EINVAL;

  const kafs_v7_inode_t *mapped_inode = NULL;
  uint32_t group_id = 0u;
  uint32_t first_slot = 0u;
  size_t block_count = 0u;
  uint64_t retained[12] = {0};
  int rc = kafs_v7_fuse_write_validate(ctx, ino, size, offset, &mapped_inode, &group_id,
                                       &first_slot, &block_count, retained);
  if (rc != 0)
    return rc;

  if (block_count > 1u)
  {
    kafs_v7_runtime_data_cow_request_t requests[12];
    kafs_v7_runtime_data_cow_plan_t plans[12];
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
      uint8_t *block = malloc(plans[i].block_size);
      if (!block)
      {
        rc = -ENOMEM;
        break;
      }
      if (retained[i] == KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK)
        memset(block, 0, plans[i].block_size);
      else
      {
        uint64_t retained_physical_off = 0u;
        rc = kafs_ctx_v7_data_ref_physical_offset(ctx, (kafs_blkcnt_t)retained[i] + 1u,
                                                  &retained_physical_off);
        if (rc == 0)
          rc = kafs_pread_all(ctx->c_fd, block, plans[i].block_size, (off_t)retained_physical_off);
      }
      uint64_t block_start = ((uint64_t)first_slot + i) * plans[i].block_size;
      uint64_t copy_start = offset > block_start ? offset : block_start;
      uint64_t request_end = offset + size;
      uint64_t block_end = block_start + plans[i].block_size;
      uint64_t copy_end = request_end < block_end ? request_end : block_end;
      if (rc == 0)
        memcpy(block + copy_start - block_start, (const uint8_t *)buf + copy_start - offset,
               (size_t)(copy_end - copy_start));
      if (rc == 0)
        rc = kafs_v7_runtime_data_cow_batch_stage(operation, i, block, plans[i].block_size);
      free(block);
      if (rc == 0 && plans[i].logical_block >= UINT32_MAX)
        rc = -ERANGE;
      if (rc == 0)
      {
        uint32_t reference = htole32((uint32_t)plans[i].logical_block + 1u);
        memcpy(inode.inline_or_block_refs + (first_slot + i) * sizeof(reference), &reference,
               sizeof(reference));
      }
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
    kafs_v7_journal_patch_t inode_patch = {
        .target_type = KAFS_V7_JOURNAL_TARGET_INODE,
        .logical_index = ino,
        .patch_bytes = sizeof(inode),
        .patch = &inode,
    };
    kafs_v7_runtime_transaction_result_t transaction;
    if (rc == 0)
      rc = kafs_v7_runtime_data_cow_batch_commit(&operation, &inode_patch, 1u, &transaction);
    if (operation)
      (void)kafs_v7_runtime_data_cow_batch_abort(&operation);
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

  uint32_t slot = first_slot;
  uint64_t retained_block = retained[0];

  kafs_v7_runtime_data_cow_request_t request = {
      .group_id = group_id,
      .retained_logical_block = retained_block,
  };
  kafs_v7_runtime_data_cow_t *operation = NULL;
  kafs_v7_runtime_data_cow_plan_t plan;
  memset(&plan, 0, sizeof(plan));
  rc =
      kafs_v7_runtime_data_cow_prepare(ctx->c_v7_runtime_transactions, &request, &operation, &plan);
  uint8_t *merged = NULL;
  const void *staged = buf;
  if (rc == 0 && size != plan.block_size)
  {
    merged = malloc(plan.block_size);
    if (!merged)
      rc = -ENOMEM;
    if (rc == 0 && retained_block == KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK)
      memset(merged, 0, plan.block_size);
    else
    {
      uint64_t retained_physical_off = 0u;
      if (rc == 0)
        rc = kafs_ctx_v7_data_ref_physical_offset(ctx, (kafs_blkcnt_t)retained_block + 1u,
                                                  &retained_physical_off);
      if (rc == 0)
        rc = kafs_pread_all(ctx->c_fd, merged, plan.block_size, (off_t)retained_physical_off);
    }
    if (rc == 0)
    {
      memcpy(merged + offset % plan.block_size, buf, size);
      staged = merged;
    }
  }
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_stage(operation, staged, plan.block_size);
  free(merged);

  kafs_v7_inode_t inode;
  memcpy(&inode, mapped_inode, sizeof(inode));
  if (rc == 0)
  {
    if (plan.logical_block >= UINT32_MAX)
      rc = -ERANGE;
  }
  if (rc == 0)
  {
    uint32_t reference = htole32((uint32_t)plan.logical_block + 1u);
    memcpy(inode.inline_or_block_refs + slot * sizeof(reference), &reference, sizeof(reference));
    uint64_t request_end = offset + size;
    if (request_end > le64toh(inode.size))
      inode.size = htole64(request_end);
    if (retained_block == KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK)
    {
      if (le32toh(inode.blocks) == UINT32_MAX)
        rc = -EOVERFLOW;
      else
        inode.blocks = htole32(le32toh(inode.blocks) + 1u);
    }
  }
  kafs_v7_journal_patch_t inode_patch = {
      .target_type = KAFS_V7_JOURNAL_TARGET_INODE,
      .logical_index = ino,
      .patch_bytes = sizeof(inode),
      .patch = &inode,
  };
  kafs_v7_runtime_data_cow_result_t cow_result;
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_commit(&operation, &inode_patch, 1u, &cow_result);
  if (operation)
    (void)kafs_v7_runtime_data_cow_abort(&operation);
  if (rc != 0)
    return rc;

  int retirement_rc = 0;
  if (retained_block != KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK)
  {
    kafs_v7_runtime_data_retirement_request_t retirement = {
        .group_id = group_id,
        .logical_block = retained_block,
    };
    kafs_v7_runtime_data_retirement_result_t retirement_result;
    retirement_rc = kafs_v7_runtime_data_retire(ctx->c_v7_runtime_transactions, &retirement,
                                                &retirement_result);
  }
  if (result)
  {
    result->new_logical_block = cow_result.data.logical_block;
    result->retained_logical_block = retained_block;
    result->retirement_rc = retirement_rc;
  }
  return (int)size;
}
