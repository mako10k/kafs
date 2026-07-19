#include "kafs_v7_fuse_write.h"

#include "kafs_v7_fuse_policy.h"
#include "kafs_v7_layout.h"
#include "kafs_v7_runtime_transaction.h"

#include <endian.h>
#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>

static int kafs_v7_fuse_write_validate(const kafs_context_t *ctx, kafs_inocnt_t ino, size_t size,
                                       uint64_t offset, const kafs_v7_inode_t **inode_out,
                                       uint32_t *group_id_out, uint32_t *slot_out,
                                       uint64_t *retained_out)
{
  if (!ctx || !inode_out || !group_id_out || !slot_out || !retained_out ||
      !ctx->c_v7_runtime_transactions || !kafs_v7_fuse_policy_controlled_write_active(ctx))
    return -EROFS;
  if (ctx->c_v7_block_size == 0u || size != ctx->c_v7_block_size ||
      (offset % ctx->c_v7_block_size) != 0u)
    return -EOPNOTSUPP;

  const kafs_v7_inode_runtime_shard_t *shard = kafs_ctx_v7_inode_shard_for_ino(ctx, ino);
  const kafs_v7_inode_t *inode =
      (const kafs_v7_inode_t *)kafs_ctx_v7_inode((kafs_context_t *)ctx, ino);
  if (!shard || !inode || !S_ISREG(le16toh(inode->mode)))
    return -EOPNOTSUPP;

  uint64_t file_size = le64toh(inode->size);
  if (offset > file_size || size > file_size - offset)
    return -EFBIG;
  uint64_t slot64 = offset / ctx->c_v7_block_size;
  if (slot64 >= 12u)
    return -EOPNOTSUPP;

  uint32_t reference = 0u;
  memcpy(&reference, inode->inline_or_block_refs + slot64 * sizeof(reference), sizeof(reference));
  reference = le32toh(reference);
  if (reference == 0u)
    return -EOPNOTSUPP;

  *inode_out = inode;
  *group_id_out = shard->group_id;
  *slot_out = (uint32_t)slot64;
  *retained_out = (uint64_t)reference - 1u;
  return 0;
}

int kafs_v7_fuse_write_aligned_direct(kafs_context_t *ctx, kafs_inocnt_t ino, const void *buf,
                                      size_t size, uint64_t offset,
                                      kafs_v7_fuse_write_result_t *result)
{
  if (result)
    memset(result, 0, sizeof(*result));
  if (!buf)
    return -EINVAL;

  const kafs_v7_inode_t *mapped_inode = NULL;
  uint32_t group_id = 0u;
  uint32_t slot = 0u;
  uint64_t retained = 0u;
  int rc = kafs_v7_fuse_write_validate(ctx, ino, size, offset, &mapped_inode, &group_id, &slot,
                                       &retained);
  if (rc != 0)
    return rc;

  kafs_v7_runtime_data_cow_request_t request = {
      .group_id = group_id,
      .retained_logical_block = retained,
  };
  kafs_v7_runtime_data_cow_t *operation = NULL;
  kafs_v7_runtime_data_cow_plan_t plan;
  memset(&plan, 0, sizeof(plan));
  rc =
      kafs_v7_runtime_data_cow_prepare(ctx->c_v7_runtime_transactions, &request, &operation, &plan);
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_stage(operation, buf, size);

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

  kafs_v7_runtime_data_retirement_request_t retirement = {
      .group_id = group_id,
      .logical_block = retained,
  };
  kafs_v7_runtime_data_retirement_result_t retirement_result;
  int retirement_rc =
      kafs_v7_runtime_data_retire(ctx->c_v7_runtime_transactions, &retirement, &retirement_result);
  if (result)
  {
    result->new_logical_block = cow_result.data.logical_block;
    result->retained_logical_block = retained;
    result->retirement_rc = retirement_rc;
  }
  return (int)size;
}
