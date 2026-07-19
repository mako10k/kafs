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

static int seed_regular_file(kafs_context_t *ctx, kafs_inocnt_t ino, uint32_t group_id,
                             const void *data, uint64_t *logical_block_out)
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

  kafs_v7_runtime_data_cow_request_t request = {
      .group_id = group_id,
      .retained_logical_block = KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK,
  };
  kafs_v7_runtime_data_cow_t *operation = NULL;
  kafs_v7_runtime_data_cow_plan_t plan;
  rc = kafs_v7_runtime_data_cow_prepare(ctx->c_v7_runtime_transactions, &request, &operation,
                                        &plan);
  if (rc != 0)
    fprintf(stderr, "seed prepare failed: %d group=%u\n", rc, group_id);
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_stage(operation, data, plan.block_size);
  if (rc != 0)
    fprintf(stderr, "seed stage failed: %d\n", rc);
  memset(&inode, 0, sizeof(inode));
  inode.mode = htole16(S_IFREG | 0644u);
  inode.size = htole64(plan.block_size);
  inode.link_count = htole16(1u);
  inode.blocks = htole32(1u);
  if (rc == 0)
    set_direct_reference(&inode, 0u, plan.logical_block);
  patch.free_inodes_delta = 0;
  kafs_v7_runtime_data_cow_result_t result;
  if (rc == 0)
    rc = kafs_v7_runtime_data_cow_commit(&operation, &patch, 1u, &result);
  if (rc != 0)
    fprintf(stderr, "seed commit failed: %d ino=%u\n", rc, (unsigned)ino);
  if (operation)
    (void)kafs_v7_runtime_data_cow_abort(&operation);
  if (rc == 0)
    *logical_block_out = result.data.logical_block;
  return rc;
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

  kafs_inocnt_t ino = 2u;
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
  uint64_t retained = 0u;
  if (rc == 0)
  {
    memset(before, 0x5a, ctx.c_v7_block_size);
    memset(after, 0xa5, ctx.c_v7_block_size);
    rc = seed_regular_file(&ctx, ino, shard->group_id, before, &retained);
    if (rc != 0)
      fprintf(stderr, "regular file seed failed: %d\n", rc);
  }
  if (rc == 0 &&
      kafs_v7_fuse_write_direct(&ctx, ino, after, 2u, ctx.c_v7_block_size - 1u, NULL) !=
          -EOPNOTSUPP)
    rc = -1;
  if (rc == 0 &&
      kafs_v7_fuse_write_direct(&ctx, ino, after, 1u, ctx.c_v7_block_size, NULL) != -EFBIG)
    rc = -1;
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
    rc = -1;
  if (rc == 0)
    rc = kafs_ctx_v7_data_ref_physical_offset(&ctx, (kafs_blkcnt_t)result.new_logical_block + 1u,
                                              &physical_off);
  if (rc == 0 && memcmp((uint8_t *)image + physical_off, after, ctx.c_v7_block_size) != 0)
    rc = -1;
  const kafs_v7_inode_t *inode = (const kafs_v7_inode_t *)kafs_ctx_v7_inode(&ctx, ino);
  if (rc == 0 && (!inode || get_direct_reference(inode, 0u) != result.new_logical_block))
    rc = -1;
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
    rc = -1;
  if (rc == 0)
    rc = kafs_ctx_v7_data_ref_physical_offset(&ctx, (kafs_blkcnt_t)result.new_logical_block + 1u,
                                              &physical_off);
  if (rc == 0 && memcmp((uint8_t *)image + physical_off, after, ctx.c_v7_block_size) != 0)
    rc = -1;

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
