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

  uint64_t file_size = le64toh(inode->size);
  if (offset > file_size)
    return -EOPNOTSUPP;
  if (size > UINT64_MAX - offset)
    return -EFBIG;
  uint64_t request_end = offset + size;
  uint64_t first_slot = offset / ctx->c_v7_block_size;
  uint64_t last_slot = (request_end - 1u) / ctx->c_v7_block_size;
  if (first_slot >= KAFS_V7_INODE_DIRECT_REFERENCE_COUNT ||
      last_slot >= KAFS_V7_INODE_DIRECT_REFERENCE_COUNT)
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
  uint32_t parent_blocks = le32toh(mapped_parent->blocks);
  if (parent_blocks >= 2u && parent_blocks <= 3u &&
      le64toh(mapped_parent->size) <= (uint64_t)ctx->c_v7_block_size * parent_blocks)
  {
    for (uint32_t slot = parent_blocks; slot < KAFS_V7_INODE_REFERENCE_COUNT; ++slot)
      if (kafs_v7_fuse_inode_reference(mapped_parent, slot) != 0u)
        return -EOPNOTSUPP;
    uint64_t retained[3];
    for (uint32_t i = 0u; i < parent_blocks; ++i)
    {
      uint32_t reference = kafs_v7_fuse_inode_reference(mapped_parent, i);
      if (reference == 0u)
        return -EOPNOTSUPP;
      retained[i] = (uint64_t)reference - 1u;
    }
    kafs_v7_runtime_data_cow_request_t requests[3];
    for (uint32_t i = 0u; i < parent_blocks; ++i)
      requests[i] = (kafs_v7_runtime_data_cow_request_t){
          .group_id = parent_shard->group_id,
          .retained_logical_block = retained[i],
      };
    if (parent_blocks == 2u)
      requests[2] = (kafs_v7_runtime_data_cow_request_t){
          .group_id = parent_shard->group_id,
          .retained_logical_block = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK,
      };
    kafs_v7_runtime_data_cow_batch_t *batch = NULL;
    kafs_v7_runtime_data_cow_plan_t plans[3];
    size_t plan_count = parent_blocks;
    int rc = kafs_v7_runtime_data_cow_batch_prepare(ctx->c_v7_runtime_transactions, requests,
                                                    plan_count, &batch, plans);
    size_t payload_bytes = (size_t)ctx->c_v7_block_size * parent_blocks;
    uint8_t *payload = rc == 0 ? malloc(payload_bytes) : NULL;
    if (rc == 0 && !payload)
      rc = -ENOMEM;
    for (size_t i = 0u; rc == 0 && i < parent_blocks; ++i)
    {
      uint64_t physical_off = 0u;
      rc =
          kafs_ctx_v7_data_ref_physical_offset(ctx, (kafs_blkcnt_t)retained[i] + 1u, &physical_off);
      if (rc == 0)
        rc = kafs_pread_all(ctx->c_fd, payload + i * ctx->c_v7_block_size, ctx->c_v7_block_size,
                            (off_t)physical_off);
    }
    size_t new_size = 0u;
    if (rc == 0)
      rc = kafs_v7_fuse_directory_append(payload, payload_bytes, name, name_bytes, ino, &new_size);
    if (rc == -ENOSPC && parent_blocks == 2u)
    {
      rc = kafs_v7_runtime_data_cow_batch_abort(&batch);
      size_t grown_bytes = (size_t)ctx->c_v7_block_size * 3u;
      uint8_t *grown = rc == 0 ? realloc(payload, grown_bytes) : NULL;
      if (rc == 0 && !grown)
        rc = -ENOMEM;
      if (grown)
        payload = grown;
      if (rc == 0)
      {
        memset(payload + payload_bytes, 0, ctx->c_v7_block_size);
        payload_bytes = grown_bytes;
        rc =
            kafs_v7_fuse_directory_append(payload, payload_bytes, name, name_bytes, ino, &new_size);
      }
      plan_count = 3u;
      if (rc == 0)
        rc = kafs_v7_runtime_data_cow_batch_prepare(ctx->c_v7_runtime_transactions, requests,
                                                    plan_count, &batch, plans);
    }
    for (size_t i = 0u; rc == 0 && i < plan_count; ++i)
      rc = kafs_v7_runtime_data_cow_batch_stage(batch, i, payload + i * ctx->c_v7_block_size,
                                                ctx->c_v7_block_size);

    kafs_v7_inode_t parent;
    memcpy(&parent, mapped_parent, sizeof(parent));
    for (size_t i = 0u; rc == 0 && i < plan_count; ++i)
      if (plans[i].logical_block >= UINT32_MAX)
        rc = -ERANGE;
    if (rc == 0)
    {
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
         .patch_bytes = sizeof(child),
         .patch = &child,
         .free_inodes_delta = -1},
    };
    kafs_v7_runtime_transaction_result_t transaction;
    if (rc == 0)
      rc = kafs_v7_runtime_data_cow_batch_commit(&batch, patches, 2u, &transaction);
    if (batch)
      (void)kafs_v7_runtime_data_cow_batch_abort(&batch);
    free(payload);
    if (rc != 0)
      return rc;

    int retirement_rc = 0;
    for (size_t i = 0u; i < parent_blocks; ++i)
    {
      kafs_v7_runtime_data_retirement_request_t retirement = {
          .group_id = parent_shard->group_id,
          .logical_block = retained[i],
      };
      kafs_v7_runtime_data_retirement_result_t retirement_result;
      int current_rc = kafs_v7_runtime_data_retire(ctx->c_v7_runtime_transactions, &retirement,
                                                   &retirement_result);
      if (retirement_rc == 0 && current_rc != 0)
        retirement_rc = current_rc;
    }
    if (retirement_rc_out)
      *retirement_rc_out = retirement_rc;
    *ino_out = ino;
    return 0;
  }
  if (parent_blocks != 1u || le64toh(mapped_parent->size) > ctx->c_v7_block_size)
    return -EOPNOTSUPP;
  for (uint32_t slot = 1u; slot < KAFS_V7_INODE_REFERENCE_COUNT; ++slot)
    if (kafs_v7_fuse_inode_reference(mapped_parent, slot) != 0u)
      return -EOPNOTSUPP;
  uint32_t parent_reference = kafs_v7_fuse_inode_reference(mapped_parent, 0u);
  if (parent_reference == 0u)
    return -EOPNOTSUPP;

  uint64_t retained = (uint64_t)parent_reference - 1u;
  kafs_v7_runtime_data_cow_request_t request = {
      .group_id = parent_shard->group_id,
      .retained_logical_block = retained,
  };
  kafs_v7_runtime_data_cow_t *operation = NULL;
  kafs_v7_runtime_data_cow_plan_t plan;
  int rc =
      kafs_v7_runtime_data_cow_prepare(ctx->c_v7_runtime_transactions, &request, &operation, &plan);
  uint8_t *block = rc == 0 ? malloc(plan.block_size) : NULL;
  if (rc == 0 && !block)
    rc = -ENOMEM;
  uint64_t physical_off = 0u;
  if (rc == 0)
    rc = kafs_ctx_v7_data_ref_physical_offset(ctx, (kafs_blkcnt_t)retained + 1u, &physical_off);
  if (rc == 0)
    rc = kafs_pread_all(ctx->c_fd, block, plan.block_size, (off_t)physical_off);

  size_t new_size = 0u;
  if (rc == 0)
    rc = kafs_v7_fuse_directory_append(block, plan.block_size, name, name_bytes, ino, &new_size);
  if (rc == -ENOSPC)
  {
    rc = kafs_v7_runtime_data_cow_abort(&operation);
    size_t grown_bytes = (size_t)plan.block_size * 2u;
    uint8_t *grown = rc == 0 ? realloc(block, grown_bytes) : NULL;
    if (rc == 0 && !grown)
      rc = -ENOMEM;
    if (grown)
      block = grown;
    if (rc == 0)
    {
      memset(block + plan.block_size, 0, plan.block_size);
      rc = kafs_v7_fuse_directory_append(block, grown_bytes, name, name_bytes, ino, &new_size);
    }

    kafs_v7_runtime_data_cow_request_t requests[2] = {
        {.group_id = parent_shard->group_id, .retained_logical_block = retained},
        {.group_id = parent_shard->group_id,
         .retained_logical_block = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK},
    };
    kafs_v7_runtime_data_cow_batch_t *batch = NULL;
    kafs_v7_runtime_data_cow_plan_t plans[2];
    if (rc == 0)
      rc = kafs_v7_runtime_data_cow_batch_prepare(ctx->c_v7_runtime_transactions, requests, 2u,
                                                  &batch, plans);
    for (size_t i = 0u; rc == 0 && i < 2u; ++i)
      rc = kafs_v7_runtime_data_cow_batch_stage(batch, i, block + i * plan.block_size,
                                                plan.block_size);

    kafs_v7_inode_t grown_parent;
    memcpy(&grown_parent, mapped_parent, sizeof(grown_parent));
    if (rc == 0 && (plans[0].logical_block >= UINT32_MAX || plans[1].logical_block >= UINT32_MAX))
      rc = -ERANGE;
    if (rc == 0)
    {
      uint32_t first_reference = htole32((uint32_t)plans[0].logical_block + 1u);
      uint32_t second_reference = htole32((uint32_t)plans[1].logical_block + 1u);
      memcpy(grown_parent.inline_or_block_refs, &first_reference, sizeof(first_reference));
      memcpy(grown_parent.inline_or_block_refs + sizeof(first_reference), &second_reference,
             sizeof(second_reference));
      grown_parent.size = htole64(new_size);
      grown_parent.blocks = htole32(2u);
    }
    kafs_v7_journal_patch_t growth_patches[2] = {
        {.target_type = KAFS_V7_JOURNAL_TARGET_INODE,
         .logical_index = parent_ino,
         .patch_bytes = sizeof(grown_parent),
         .patch = &grown_parent},
        {.target_type = KAFS_V7_JOURNAL_TARGET_INODE,
         .logical_index = ino,
         .patch_bytes = sizeof(child),
         .patch = &child,
         .free_inodes_delta = -1},
    };
    kafs_v7_runtime_transaction_result_t transaction;
    if (rc == 0)
      rc = kafs_v7_runtime_data_cow_batch_commit(&batch, growth_patches, 2u, &transaction);
    if (batch)
      (void)kafs_v7_runtime_data_cow_batch_abort(&batch);
    free(block);
    if (rc != 0)
      return rc;

    kafs_v7_runtime_data_retirement_request_t retirement = {
        .group_id = parent_shard->group_id,
        .logical_block = retained,
    };
    kafs_v7_runtime_data_retirement_result_t retirement_result;
    int retirement_rc = kafs_v7_runtime_data_retire(ctx->c_v7_runtime_transactions, &retirement,
                                                    &retirement_result);
    if (retirement_rc_out)
      *retirement_rc_out = retirement_rc;
    *ino_out = ino;
    return 0;
  }
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_stage(operation, block, plan.block_size);
  free(block);

  kafs_v7_inode_t parent;
  memcpy(&parent, mapped_parent, sizeof(parent));
  if (rc == 0 && plan.logical_block >= UINT32_MAX)
    rc = -ERANGE;
  if (rc == 0)
  {
    uint32_t reference = htole32((uint32_t)plan.logical_block + 1u);
    memcpy(parent.inline_or_block_refs, &reference, sizeof(reference));
    parent.size = htole64(new_size);
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
  if (rc != 0)
    return rc;

  kafs_v7_runtime_data_retirement_request_t retirement = {
      .group_id = parent_shard->group_id,
      .logical_block = retained,
  };
  kafs_v7_runtime_data_retirement_result_t retirement_result;
  int retirement_rc =
      kafs_v7_runtime_data_retire(ctx->c_v7_runtime_transactions, &retirement, &retirement_result);
  if (retirement_rc_out)
    *retirement_rc_out = retirement_rc;
  *ino_out = ino;
  return 0;
}

int kafs_v7_fuse_truncate_direct(kafs_context_t *ctx, kafs_inocnt_t ino, uint64_t size,
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
  if (size > old_size ||
      old_size > KAFS_V7_INODE_DIRECT_REFERENCE_COUNT * (uint64_t)ctx->c_v7_block_size)
    return -EOPNOTSUPP;
  if (size == old_size)
    return 0;
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
