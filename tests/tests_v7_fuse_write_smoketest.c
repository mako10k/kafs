#include "test_utils.h"

#include "kafs_offline_summary.h"
#include "kafs_tool_util.h"
#include "kafs_v7_fuse_policy.h"
#include "kafs_v7_fuse_write.h"
#include "kafs_v7_layout.h"
#include "kafs_v7_runtime_transaction.h"
#include "kafs_v7_runtime_view.h"

#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static int run_command(char *const argv[])
{
  pid_t pid = fork();
  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
    execvp(argv[0], argv);
    _exit(127);
  }
  int status = 0;
  if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status))
    return -1;
  return WEXITSTATUS(status) == 0 ? 0 : -1;
}

static int format_image(const char *path)
{
  char *argv[] = {(char *)kafs_test_mkfs_bin(), (char *)path, (char *)"--format-version",
                  (char *)"7", (char *)"--size-bytes", (char *)"128M",
                  (char *)"--v7-group-count", (char *)"4", (char *)"--yes", NULL};
  return run_command(argv);
}

static int fsck_image(const char *path)
{
  char *argv[] = {(char *)kafs_test_fsck_bin(), (char *)"--check", (char *)path, NULL};
  return run_command(argv);
}

static void set_direct_reference(kafs_v7_inode_t *inode, uint32_t slot, uint64_t logical_block)
{
  uint32_t reference = htole32((uint32_t)logical_block + 1u);
  memcpy(inode->inline_or_block_refs + slot * sizeof(reference), &reference, sizeof(reference));
}

static uint64_t get_direct_reference(const kafs_v7_inode_t *inode, uint32_t slot)
{
  uint32_t reference = 0u;
  memcpy(&reference, inode->inline_or_block_refs + slot * sizeof(reference), sizeof(reference));
  return (uint64_t)le32toh(reference) - 1u;
}

static int has_direct_reference(const kafs_v7_inode_t *inode, uint32_t slot)
{
  uint32_t reference = 0u;
  memcpy(&reference, inode->inline_or_block_refs + slot * sizeof(reference), sizeof(reference));
  return le32toh(reference) != 0u;
}

static int test_inline_promotion(kafs_context_t *ctx, kafs_inocnt_t ino)
{
  const size_t inline_capacity = sizeof(((kafs_v7_inode_t *)0)->inline_or_block_refs);
  uint8_t original[sizeof(((kafs_v7_inode_t *)0)->inline_or_block_refs)];
  for (size_t i = 0u; i < sizeof(original); ++i)
    original[i] = (uint8_t)(i * 13u + 5u);

  kafs_v7_inode_t inode;
  memset(&inode, 0, sizeof(inode));
  inode.mode = htole16(S_IFREG | 0644u);
  inode.size = htole64(inline_capacity);
  inode.link_count = htole16(1u);
  memcpy(inode.inline_or_block_refs, original, sizeof(original));
  kafs_v7_journal_patch_t patch = {
      .target_type = KAFS_V7_JOURNAL_TARGET_INODE,
      .logical_index = ino,
      .patch_bytes = sizeof(inode),
      .patch = &inode,
      .free_inodes_delta = -1,
  };
  kafs_v7_runtime_transaction_result_t transaction;
  int rc = kafs_v7_runtime_transaction_commit(ctx->c_v7_runtime_transactions, &patch, 1u,
                                               &transaction);

  const kafs_v7_inode_t *mapped = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(ctx, ino);
  kafs_v7_inode_t before;
  if (rc == 0 && mapped)
    memcpy(&before, mapped, sizeof(before));
  else if (rc == 0)
    rc = -EIO;
  uint8_t byte = 0x5au;
  if (rc == 0 && kafs_v7_fuse_write_direct(ctx, ino, &byte, 1u, inline_capacity + 1u, NULL) !=
                     -EOPNOTSUPP)
    rc = -1;
  mapped = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(ctx, ino);
  if (rc == 0 && (!mapped || memcmp(mapped, &before, sizeof(before)) != 0))
    rc = -1;

  size_t oversized_bytes = (size_t)ctx->c_v7_block_size - inline_capacity + 1u;
  uint8_t *oversized = malloc(oversized_bytes);
  if (rc == 0 && !oversized)
    rc = -ENOMEM;
  if (rc == 0)
  {
    memset(oversized, 0xa7, oversized_bytes);
    if (kafs_v7_fuse_write_direct(ctx, ino, oversized, oversized_bytes, inline_capacity, NULL) !=
        -EOPNOTSUPP)
      rc = -1;
  }
  free(oversized);
  mapped = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(ctx, ino);
  if (rc == 0 && (!mapped || memcmp(mapped, &before, sizeof(before)) != 0))
    rc = -1;

  static const uint8_t appended[] = {0x91u, 0x82u, 0x73u, 0x64u, 0x55u, 0x46u, 0x37u};
  kafs_v7_fuse_write_result_t result;
  if (rc == 0)
    rc = kafs_v7_fuse_write_direct(ctx, ino, appended, sizeof(appended), inline_capacity, &result);
  if (rc == (int)sizeof(appended))
    rc = 0;
  if (rc != 0)
    fprintf(stderr, "inline regular promotion failed: %d\n", rc);

  mapped = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(ctx, ino);
  if (rc == 0 &&
      (!mapped || le64toh(mapped->size) != inline_capacity + sizeof(appended) ||
       le32toh(mapped->blocks) != 1u || !has_direct_reference(mapped, 0u) ||
       result.retained_logical_block != KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK ||
       result.retirement_rc != 0))
    rc = -1;
  for (uint32_t slot = 1u; rc == 0 && slot < KAFS_V7_INODE_REFERENCE_COUNT; ++slot)
    if (has_direct_reference(mapped, slot))
      rc = -1;

  uint8_t *expected = NULL;
  uint64_t physical_off = 0u;
  if (rc == 0)
  {
    expected = calloc(1u, ctx->c_v7_block_size);
    if (!expected)
      rc = -ENOMEM;
  }
  if (rc == 0)
  {
    memcpy(expected, original, sizeof(original));
    memcpy(expected + inline_capacity, appended, sizeof(appended));
    rc = kafs_ctx_v7_data_ref_physical_offset(ctx, (kafs_blkcnt_t)result.new_logical_block + 1u,
                                              &physical_off);
  }
  if (rc == 0 && memcmp((uint8_t *)ctx->c_img_base + physical_off, expected,
                        ctx->c_v7_block_size) != 0)
    rc = -1;

  uint64_t promoted_block = result.new_logical_block;
  static const uint8_t replacement[] = {0x11u, 0x22u, 0x33u};
  if (rc == 0)
  {
    memcpy(expected + 10u, replacement, sizeof(replacement));
    rc = kafs_v7_fuse_write_direct(ctx, ino, replacement, sizeof(replacement), 10u, &result);
  }
  if (rc == (int)sizeof(replacement))
    rc = 0;
  if (rc == 0 && (result.retained_logical_block != promoted_block ||
                  result.new_logical_block == promoted_block || result.retirement_rc != 0))
    rc = -1;
  if (rc == 0)
    rc = kafs_ctx_v7_data_ref_physical_offset(ctx, (kafs_blkcnt_t)result.new_logical_block + 1u,
                                              &physical_off);
  if (rc == 0 && memcmp((uint8_t *)ctx->c_img_base + physical_off, expected,
                        ctx->c_v7_block_size) != 0)
    rc = -1;
  kafs_v7_inode_t before_truncate;
  mapped = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(ctx, ino);
  if (rc == 0 && mapped)
    memcpy(&before_truncate, mapped, sizeof(before_truncate));
  else if (rc == 0)
    rc = -EIO;
  kafs_v7_fuse_truncate_result_t truncate_result;
  if (rc == 0 &&
      kafs_v7_fuse_truncate_direct(ctx, ino, inline_capacity, &truncate_result) != -EOPNOTSUPP)
    rc = -1;
  mapped = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(ctx, ino);
  if (rc == 0 && (!mapped || memcmp(mapped, &before_truncate, sizeof(before_truncate)) != 0))
    rc = -1;
  free(expected);
  return rc;
}

static int seed_regular_file(kafs_context_t *ctx, kafs_inocnt_t ino, uint32_t group_id,
                             const void *data, uint64_t logical_blocks_out[3])
{
  kafs_v7_inode_t inode;
  memset(&inode, 0, sizeof(inode));
  inode.mode = htole16(S_IFREG | 0644u);
  inode.size = htole64(1u);
  inode.link_count = htole16(1u);
  inode.inline_or_block_refs[0] = 'x';
  kafs_v7_journal_patch_t patch = {
      .target_type = KAFS_V7_JOURNAL_TARGET_INODE,
      .logical_index = ino,
      .patch_bytes = sizeof(inode),
      .patch = &inode,
      .free_inodes_delta = -1,
  };
  kafs_v7_runtime_transaction_result_t transaction;
  int rc = kafs_v7_runtime_transaction_commit(ctx->c_v7_runtime_transactions, &patch, 1u,
                                               &transaction);
  if (rc != 0)
    return rc;

  kafs_v7_runtime_data_cow_request_t requests[3];
  for (size_t i = 0; i < 3u; ++i)
    requests[i] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = group_id,
        .retained_logical_block = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK,
    };
  kafs_v7_runtime_data_cow_batch_t *operation = NULL;
  kafs_v7_runtime_data_cow_plan_t plans[3];
  rc = kafs_v7_runtime_data_cow_batch_prepare(ctx->c_v7_runtime_transactions, requests, 3u,
                                              &operation, plans);
  if (rc != 0)
    fprintf(stderr, "seed prepare failed: %d group=%u\n", rc, group_id);
  for (size_t i = 0; rc == 0 && i < 3u; ++i)
    rc = kafs_v7_runtime_data_cow_batch_stage(operation, i, data, plans[i].block_size);
  if (rc != 0)
    fprintf(stderr, "seed stage failed: %d\n", rc);
  memset(&inode, 0, sizeof(inode));
  inode.mode = htole16(S_IFREG | 0644u);
  inode.size = htole64(3u * plans[0].block_size);
  inode.link_count = htole16(1u);
  inode.blocks = htole32(3u);
  for (size_t i = 0; rc == 0 && i < 3u; ++i)
    set_direct_reference(&inode, (uint32_t)i, plans[i].logical_block);
  patch.free_inodes_delta = 0;
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_batch_commit(&operation, &patch, 1u, &transaction);
  if (rc != 0)
    fprintf(stderr, "seed commit failed: %d ino=%u\n", rc, (unsigned)ino);
  if (operation)
    (void)kafs_v7_runtime_data_cow_batch_abort(&operation);
  if (rc == 0)
    for (size_t i = 0; i < 3u; ++i)
      logical_blocks_out[i] = plans[i].logical_block;
  return rc;
}

static uint32_t directory_name_hash(const uint8_t *name, size_t name_bytes)
{
  uint32_t hash = UINT32_C(2166136261);
  for (size_t i = 0u; i < name_bytes; ++i)
  {
    hash ^= name[i];
    hash *= UINT32_C(16777619);
  }
  return hash;
}

static int fill_canonical_tombstone_directory(uint8_t *payload, size_t used)
{
  if (!payload || used < KAFS_V7_KDIR_HEADER_BYTES)
    return -EINVAL;
  size_t remaining = used - KAFS_V7_KDIR_HEADER_BYTES;
  size_t off = KAFS_V7_KDIR_HEADER_BYTES;
  uint32_t tombstone_count = 0u;
  while (remaining != 0u)
  {
    size_t record_bytes = remaining;
    if (record_bytes > KAFS_V7_KDIR_RECORD_PREFIX_BYTES + 255u)
      record_bytes = KAFS_V7_KDIR_RECORD_PREFIX_BYTES + 255u;
    size_t remainder = remaining - record_bytes;
    if (remainder != 0u && remainder < KAFS_V7_KDIR_RECORD_PREFIX_BYTES + 1u)
      record_bytes -= KAFS_V7_KDIR_RECORD_PREFIX_BYTES + 1u - remainder;
    if (record_bytes < KAFS_V7_KDIR_RECORD_PREFIX_BYTES + 1u)
      return -EINVAL;

    size_t name_bytes = record_bytes - KAFS_V7_KDIR_RECORD_PREFIX_BYTES;
    kafs_v7_kdir_record_t *record = (kafs_v7_kdir_record_t *)(payload + off);
    record->record_length = htole16((uint16_t)record_bytes);
    record->flags = htole16(KAFS_V7_KDIR_FLAG_TOMBSTONE);
    record->inode = htole32(KAFS_INO_ROOTDIR);
    record->name_bytes = htole16((uint16_t)name_bytes);
    memset(record->name, (int)('a' + tombstone_count % 26u), name_bytes);
    record->name_hash = htole32(directory_name_hash(record->name, name_bytes));
    tombstone_count++;
    off += record_bytes;
    remaining -= record_bytes;
  }
  kafs_v7_kdir_header_t *header = (kafs_v7_kdir_header_t *)payload;
  header->magic = htole32(KAFS_V7_KDIR_MAGIC);
  header->version = htole16(KAFS_V7_KDIR_VERSION);
  header->tombstone_count = htole32(tombstone_count);
  header->record_bytes = htole32((uint32_t)(used - KAFS_V7_KDIR_HEADER_BYTES));
  return 0;
}

static int seed_direct_directory(
    kafs_context_t *ctx, kafs_inocnt_t ino, uint32_t group_id, uint32_t block_count,
    size_t spare_bytes, uint64_t logical_blocks_out[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT])
{
  size_t payload_bytes = (size_t)ctx->c_v7_block_size * block_count;
  if (block_count == 0u || block_count > KAFS_V7_INODE_DIRECT_REFERENCE_COUNT ||
      spare_bytes >= payload_bytes - KAFS_V7_KDIR_HEADER_BYTES - KAFS_V7_KDIR_RECORD_PREFIX_BYTES)
    return -EINVAL;
  uint8_t *payload = calloc(1u, payload_bytes);
  if (!payload)
    return -ENOMEM;
  size_t used = payload_bytes - spare_bytes;
  int rc = fill_canonical_tombstone_directory(payload, used);

  kafs_v7_runtime_data_cow_request_t requests[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT];
  for (size_t i = 0u; i < block_count; ++i)
    requests[i] = (kafs_v7_runtime_data_cow_request_t){
        .group_id = group_id,
        .retained_logical_block = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK,
    };
  kafs_v7_runtime_data_cow_batch_t *operation = NULL;
  kafs_v7_runtime_data_cow_plan_t plans[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT];
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_batch_prepare(ctx->c_v7_runtime_transactions, requests,
                                                block_count, &operation, plans);
  for (size_t i = 0u; rc == 0 && i < block_count; ++i)
    rc = kafs_v7_runtime_data_cow_batch_stage(operation, i,
                                              payload + i * ctx->c_v7_block_size,
                                              ctx->c_v7_block_size);
  free(payload);

  kafs_v7_inode_t inode;
  memset(&inode, 0, sizeof(inode));
  inode.mode = htole16(S_IFDIR | 0755u);
  inode.size = htole64(used);
  inode.link_count = htole16(1u);
  inode.blocks = htole32(block_count);
  for (size_t i = 0u; rc == 0 && i < block_count; ++i)
  {
    set_direct_reference(&inode, (uint32_t)i, plans[i].logical_block);
    logical_blocks_out[i] = plans[i].logical_block;
  }
  kafs_v7_journal_patch_t patch = {
      .target_type = KAFS_V7_JOURNAL_TARGET_INODE,
      .logical_index = ino,
      .patch_bytes = sizeof(inode),
      .patch = &inode,
  };
  kafs_v7_runtime_transaction_result_t transaction;
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_batch_commit(&operation, &patch, 1u, &transaction);
  if (operation)
    (void)kafs_v7_runtime_data_cow_batch_abort(&operation);
  return rc;
}

typedef struct direct_directory_case
{
  const char *name;
  uint32_t initial_blocks;
  int grow;
  int require_bitmap_boundary;
  int expected_rc;
} direct_directory_case_t;

static int test_direct_directory_transitions(kafs_context_t *ctx)
{
  static const direct_directory_case_t cases[] = {
      {.name = "append-1", .initial_blocks = 1u},
      {.name = "grow-1", .initial_blocks = 1u, .grow = 1},
      {.name = "append-6", .initial_blocks = 6u},
      {.name = "grow-6", .initial_blocks = 6u, .grow = 1},
      {.name = "grow-11", .initial_blocks = 11u, .grow = 1, .require_bitmap_boundary = 1},
      {.name = "append-12", .initial_blocks = KAFS_V7_INODE_DIRECT_REFERENCE_COUNT},
      {.name = "full-12",
       .initial_blocks = KAFS_V7_INODE_DIRECT_REFERENCE_COUNT,
       .grow = 1,
       .expected_rc = -ENOSPC},
  };
  const kafs_inocnt_t parent_ino = KAFS_INO_ROOTDIR;
  const kafs_v7_inode_runtime_shard_t *shard = kafs_ctx_v7_inode_shard_for_ino(ctx, parent_ino);
  if (!shard)
    return -1;
  for (size_t case_id = 0u; case_id < sizeof(cases) / sizeof(cases[0]); ++case_id)
  {
    const direct_directory_case_t *test = &cases[case_id];
    size_t appended_bytes = KAFS_V7_KDIR_RECORD_PREFIX_BYTES + strlen(test->name);
    size_t spare_bytes = test->grow ? appended_bytes - 1u : appended_bytes;
    uint64_t retained[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT] = {0};
    int rc = seed_direct_directory(ctx, parent_ino, shard->group_id, test->initial_blocks,
                                   spare_bytes, retained);
    if (rc != 0)
    {
      fprintf(stderr, "direct directory case %s seed failed: %d\n", test->name, rc);
      return -1;
    }
    const kafs_v7_inode_t *mapped = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(ctx, parent_ino);
    kafs_v7_inode_t before;
    if (mapped)
      memcpy(&before, mapped, sizeof(before));
    else
      return -1;

    kafs_inocnt_t child_ino = 0u;
    int retirement_rc = 0;
    if (rc == 0)
      rc = kafs_v7_fuse_create_in_direct_directory(ctx, parent_ino, test->name, 0644u, 1u, 2u,
                                                    &child_ino, &retirement_rc);
    if (rc != test->expected_rc)
    {
      fprintf(stderr, "direct directory case %s returned %d expected %d\n", test->name, rc,
              test->expected_rc);
      return -1;
    }
    mapped = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(ctx, parent_ino);
    if (test->expected_rc != 0)
    {
      if (!mapped || memcmp(mapped, &before, sizeof(before)) != 0 || child_ino != 0u ||
          retirement_rc != 0)
      {
        fprintf(stderr, "direct directory case %s mutated on failure\n", test->name);
        return -1;
      }
      continue;
    }

    uint32_t expected_blocks = test->initial_blocks + (uint32_t)test->grow;
    const kafs_v7_inode_t *child = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(ctx, child_ino);
    if (!mapped || !child || le32toh(mapped->blocks) != expected_blocks ||
        !S_ISREG(le16toh(child->mode)) || retirement_rc != 0)
    {
      fprintf(stderr, "direct directory case %s result mismatch\n", test->name);
      return -1;
    }
    uint64_t first_bitmap_word = get_direct_reference(mapped, 0u) / 64u;
    int crossed_bitmap_boundary = 0;
    for (uint32_t slot = 0u; slot < expected_blocks; ++slot)
    {
      if (get_direct_reference(mapped, slot) / 64u != first_bitmap_word)
        crossed_bitmap_boundary = 1;
      if (!has_direct_reference(mapped, slot) ||
          (slot < test->initial_blocks && get_direct_reference(mapped, slot) == retained[slot]))
      {
        fprintf(stderr, "direct directory case %s slot %u was not replaced\n", test->name, slot);
        return -1;
      }
    }
    if (test->require_bitmap_boundary && !crossed_bitmap_boundary)
    {
      fprintf(stderr, "direct directory case %s did not cross a bitmap word boundary\n",
              test->name);
      return -1;
    }
  }
  return 0;
}

static int test_direct_overwrite(void)
{
  const char *path = "v7-fuse-write.img";
  if (format_image(path) != 0)
    return -1;
  int fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  uint64_t file_size = 0u;
  int rc = kafs_offline_detect_file_size(fd, &file_size);
  void *image = MAP_FAILED;
  if (rc == 0)
  {
    image = mmap(NULL, (size_t)file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (image == MAP_FAILED)
      rc = -errno;
  }
  kafs_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));
  ctx.c_fd = fd;
  ctx.c_img_base = image;
  ctx.c_img_size = (size_t)file_size;
  ctx.c_superblock = image == MAP_FAILED ? NULL : (kafs_ssuperblock_t *)image;
  if (rc == 0)
    rc = kafs_v7_runtime_view_admit_fd(&ctx, fd, ctx.c_superblock, file_size);
  if (rc != 0)
    fprintf(stderr, "runtime view admit failed: %d\n", rc);
  if (rc == 0)
    rc = kafs_v7_runtime_transaction_service_init(fd, ctx.c_superblock, file_size,
                                                   &ctx.c_v7_runtime_transactions);
  if (rc != 0)
    fprintf(stderr, "transaction service init failed: %d\n", rc);
  kafs_v7_fuse_policy_set_controlled_write(&ctx, 1);

  if (rc == 0)
  {
    kafs_v7_runtime_data_cow_request_t invalid[2] = {
        {.group_id = UINT32_MAX, .retained_logical_block = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK},
        {.group_id = UINT32_MAX, .retained_logical_block = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK},
    };
    kafs_v7_runtime_data_cow_plan_t invalid_plans[2];
    kafs_v7_runtime_data_cow_batch_t *invalid_operation = NULL;
    if (kafs_v7_runtime_data_cow_batch_prepare(ctx.c_v7_runtime_transactions, invalid, 2u,
                                               &invalid_operation, invalid_plans) != -EXDEV ||
        invalid_operation != NULL)
      rc = -1;
  }

  kafs_inocnt_t ino = 2u;
  if (rc == 0)
    rc = test_inline_promotion(&ctx, 3u);
  const kafs_v7_inode_runtime_shard_t *shard = NULL;
  if (rc == 0)
    shard = kafs_ctx_v7_inode_shard_for_ino(&ctx, ino);
  if (rc == 0 && !shard)
    rc = -1;
  uint8_t *before = NULL;
  uint8_t *after = NULL;
  if (rc == 0)
  {
    before = malloc(ctx.c_v7_block_size);
    after = malloc(ctx.c_v7_block_size);
    if (!before || !after)
      rc = -ENOMEM;
  }
  uint64_t seeded[3] = {0};
  uint64_t retained = 0u;
  if (rc == 0)
  {
    memset(before, 0x5a, ctx.c_v7_block_size);
    memset(after, 0xa5, ctx.c_v7_block_size);
    rc = seed_regular_file(&ctx, ino, shard->group_id, before, seeded);
    retained = seeded[0];
    if (rc != 0)
      fprintf(stderr, "regular file seed failed: %d\n", rc);
  }
  if (rc == 0)
  {
    int boundary_rc =
        kafs_v7_fuse_write_direct(&ctx, ino, after, 1u, 3u * ctx.c_v7_block_size + 1u, NULL);
    if (boundary_rc != -EOPNOTSUPP)
    {
      fprintf(stderr, "hole write boundary returned %d\n", boundary_rc);
      rc = -1;
    }
  }
  kafs_v7_fuse_write_result_t result;
  uint64_t physical_off = 0u;
  if (rc == 0)
  {
    uint8_t patch[17];
    memset(patch, 0xc3, sizeof(patch));
    memcpy(after, before, ctx.c_v7_block_size);
    memcpy(after + 31u, patch, sizeof(patch));
    rc = kafs_v7_fuse_write_direct(&ctx, ino, patch, sizeof(patch), 31u, &result);
  }
  if (rc < 0)
    fprintf(stderr, "partial overwrite failed: %d\n", rc);
  if (rc == 17)
    rc = 0;
  if (rc == 0 && (result.retained_logical_block != retained || result.retirement_rc != 0 ||
                  result.new_logical_block == retained))
  {
    fprintf(stderr, "partial overwrite result mismatch\n");
    rc = -1;
  }
  if (rc == 0)
    rc = kafs_ctx_v7_data_ref_physical_offset(&ctx, (kafs_blkcnt_t)result.new_logical_block + 1u,
                                              &physical_off);
  if (rc == 0 && memcmp((uint8_t *)image + physical_off, after, ctx.c_v7_block_size) != 0)
  {
    fprintf(stderr, "partial overwrite payload mismatch\n");
    rc = -1;
  }
  const kafs_v7_inode_t *inode = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(&ctx, ino);
  if (rc == 0 && (!inode || get_direct_reference(inode, 0u) != result.new_logical_block))
  {
    fprintf(stderr, "partial overwrite inode mismatch\n");
    rc = -1;
  }
  retained = result.new_logical_block;
  if (rc == 0)
  {
    memset(after, 0xa5, ctx.c_v7_block_size);
    rc = kafs_v7_fuse_write_direct(&ctx, ino, after, ctx.c_v7_block_size, 0u, &result);
  }
  if (rc == (int)ctx.c_v7_block_size)
    rc = 0;
  if (rc == 0 && (result.retained_logical_block != retained || result.retirement_rc != 0 ||
                  result.new_logical_block == retained))
  {
    fprintf(stderr, "full overwrite result mismatch\n");
    rc = -1;
  }
  if (rc == 0)
    rc = kafs_ctx_v7_data_ref_physical_offset(&ctx, (kafs_blkcnt_t)result.new_logical_block + 1u,
                                              &physical_off);
  if (rc == 0 && memcmp((uint8_t *)image + physical_off, after, ctx.c_v7_block_size) != 0)
  {
    fprintf(stderr, "full overwrite payload mismatch\n");
    rc = -1;
  }

  uint8_t *multi = NULL;
  uint8_t *expected = NULL;
  size_t multi_size = ctx.c_v7_block_size + 37u;
  uint64_t multi_offset = ctx.c_v7_block_size - 11u;
  if (rc == 0)
  {
    multi = malloc(multi_size);
    expected = malloc(3u * ctx.c_v7_block_size);
    if (!multi || !expected)
      rc = -ENOMEM;
  }
  if (rc == 0)
  {
    memset(multi, 0x7c, multi_size);
    memset(expected, 0x5a, 3u * ctx.c_v7_block_size);
    memset(expected, 0xa5, ctx.c_v7_block_size);
    memcpy(expected + multi_offset, multi, multi_size);
    rc = kafs_v7_fuse_write_direct(&ctx, ino, multi, multi_size, multi_offset, &result);
  }
  if (rc == (int)multi_size)
    rc = 0;
  if (rc != 0)
    fprintf(stderr, "multi-block overwrite failed: %d\n", rc);
  inode = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(&ctx, ino);
  for (uint32_t slot_id = 0; rc == 0 && slot_id < 3u; ++slot_id)
  {
    uint64_t logical = get_direct_reference(inode, slot_id);
    if (logical == seeded[slot_id])
    {
      fprintf(stderr, "multi-block slot %u retained old reference\n", slot_id);
      rc = -1;
    }
    if (rc == 0)
      rc = kafs_ctx_v7_data_ref_physical_offset(&ctx, (kafs_blkcnt_t)logical + 1u,
                                                &physical_off);
    if (rc == 0 && memcmp((uint8_t *)image + physical_off,
                          expected + slot_id * ctx.c_v7_block_size,
                          ctx.c_v7_block_size) != 0)
    {
      fprintf(stderr, "multi-block slot %u payload mismatch\n", slot_id);
      rc = -1;
    }
  }

  uint8_t *growth = NULL;
  uint8_t *grown = NULL;
  size_t growth_size = ctx.c_v7_block_size + 37u;
  uint64_t growth_offset = 3u * ctx.c_v7_block_size - 13u;
  if (rc == 0)
  {
    growth = malloc(growth_size);
    grown = calloc(5u, ctx.c_v7_block_size);
    if (!growth || !grown)
      rc = -ENOMEM;
  }
  if (rc == 0)
  {
    memset(growth, 0x6d, growth_size);
    memcpy(grown, expected, 3u * ctx.c_v7_block_size);
    memcpy(grown + growth_offset, growth, growth_size);
    rc = kafs_v7_fuse_write_direct(&ctx, ino, growth, growth_size, growth_offset, &result);
  }
  if (rc == (int)growth_size)
    rc = 0;
  if (rc != 0)
    fprintf(stderr, "direct growth failed: %d\n", rc);
  inode = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(&ctx, ino);
  uint64_t grown_size = growth_offset + growth_size;
  if (rc == 0 && (!inode || le64toh(inode->size) != grown_size || le32toh(inode->blocks) != 5u))
  {
    fprintf(stderr, "direct growth inode size/block count mismatch\n");
    rc = -1;
  }
  for (uint32_t slot_id = 0; rc == 0 && slot_id < 5u; ++slot_id)
  {
    uint64_t logical = get_direct_reference(inode, slot_id);
    rc = kafs_ctx_v7_data_ref_physical_offset(&ctx, (kafs_blkcnt_t)logical + 1u, &physical_off);
    if (rc == 0 && memcmp((uint8_t *)image + physical_off,
                          grown + slot_id * ctx.c_v7_block_size,
                          ctx.c_v7_block_size) != 0)
    {
      fprintf(stderr, "direct growth slot %u payload mismatch\n", slot_id);
      rc = -1;
    }
  }
  kafs_v7_fuse_truncate_result_t truncate_result;
  uint64_t truncate_size = 2u * ctx.c_v7_block_size + 23u;
  if (rc == 0)
    rc = kafs_v7_fuse_truncate_direct(&ctx, ino, truncate_size, &truncate_result);
  if (rc != 0)
    fprintf(stderr, "partial direct truncate failed: %d\n", rc);
  inode = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(&ctx, ino);
  if (rc == 0 &&
      (!inode || le64toh(inode->size) != truncate_size || le32toh(inode->blocks) != 3u ||
       truncate_result.retirement_rc != 0))
  {
    fprintf(stderr, "partial direct truncate inode mismatch\n");
    rc = -1;
  }
  if (rc == 0)
  {
    uint64_t logical = get_direct_reference(inode, 2u);
    rc = kafs_ctx_v7_data_ref_physical_offset(&ctx, (kafs_blkcnt_t)logical + 1u, &physical_off);
    if (rc == 0 && memcmp((uint8_t *)image + physical_off,
                          grown + 2u * ctx.c_v7_block_size, 23u) != 0)
    {
      fprintf(stderr, "partial direct truncate tail mismatch\n");
      rc = -1;
    }
    for (uint32_t i = 23u; rc == 0 && i < ctx.c_v7_block_size; ++i)
      if (*((uint8_t *)image + physical_off + i) != 0u)
      {
        fprintf(stderr, "partial direct truncate tail not zeroed\n");
        rc = -1;
      }
  }
  if (rc == 0 && (has_direct_reference(inode, 3u) || has_direct_reference(inode, 4u)))
  {
    fprintf(stderr, "partial direct truncate retained removed references\n");
    rc = -1;
  }
  if (rc == 0)
    rc = kafs_v7_fuse_truncate_direct(&ctx, ino, ctx.c_v7_block_size, &truncate_result);
  inode = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(&ctx, ino);
  if (rc == 0 &&
      (!inode || le64toh(inode->size) != ctx.c_v7_block_size || le32toh(inode->blocks) != 1u ||
       truncate_result.retirement_rc != 0))
  {
    fprintf(stderr, "aligned direct truncate inode mismatch\n");
    rc = -1;
  }
  if (rc == 0)
    rc = test_direct_directory_transitions(&ctx);
  free(growth);
  free(grown);
  free(multi);
  free(expected);

  free(before);
  free(after);
  kafs_v7_runtime_transaction_service_destroy(ctx.c_v7_runtime_transactions);
  ctx.c_v7_runtime_transactions = NULL;
  kafs_ctx_v7_runtime_view_clear(&ctx);
  if (image != MAP_FAILED)
    munmap(image, (size_t)file_size);
  close(fd);
  if (rc == 0)
    rc = fsck_image(path);
  if (rc != 0)
    fprintf(stderr, "post-write fsck failed: %d\n", rc);
  return rc;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("v7-fuse-write") != 0)
    return 1;
  int rc = test_direct_overwrite();
  if (rc != 0)
    fprintf(stderr, "v7 FUSE write smoke test failed: %d\n", rc);
  return rc == 0 ? 0 : 1;
}
