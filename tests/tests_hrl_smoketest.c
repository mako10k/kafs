#include "kafs.h"
#include "kafs_block.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_hash.h"
#include "kafs_legacy_map_layout.h"
#include "test_utils.h"

#include <assert.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

int main(void)
{
  if (kafs_test_enter_tmpdir("hrl_smoketest") != 0)
    return 77;

  const char *img = "./hrl_smoke.img";
  kafs_context_t ctx;
  off_t mapsize;
  assert(kafs_test_mkimg_with_hrl(img, 64 * 1024 * 1024u, 12, 2048, &ctx, &mapsize) == 0);

  // Write two equal blocks via HRL
  kafs_blksize_t bs = kafs_sb_blksize_get(ctx.c_superblock);
  char *buf = malloc(bs);
  memset(buf, 'A', bs);
  kafs_hrid_t h1, h2;
  int n1, n2;
  kafs_blkcnt_t b1, b2;
  assert(kafs_hrl_put(&ctx, buf, &h1, &n1, &b1) == 0);
  assert(kafs_hrl_put(&ctx, buf, &h2, &n2, &b2) == 0);
  // same content -> same block, not new on second
  assert(n1 == 1);
  assert(n2 == 0);
  assert(b1 == b2);
  assert(h1 == h2);

  // The first decrement leaves one live reference.
  assert(kafs_hrl_dec_ref(&ctx, h1) == 0);

  // A failed final release must preserve the entry and allocation for retry.
  int rw_fd = ctx.c_fd;
  int ro_fd = open(img, O_RDONLY);
  assert(ro_fd >= 0);
  ctx.c_fd = ro_fd;
  assert(kafs_hrl_dec_ref(&ctx, h2) == -EIO);
  assert(kafs_blk_get_usage_locked(&ctx, b1) != 0);
  ctx.c_fd = rw_fd;
  close(ro_fd);

  // Retrying with a writable descriptor should free the entry and block.
  assert(kafs_hrl_dec_ref(&ctx, h2) == 0);
  assert(kafs_blk_get_usage_locked(&ctx, b1) == 0);

  // Ensure freeing again fails
  assert(kafs_hrl_dec_ref(&ctx, h1) != 0);

  // Reopen after free-list links have been persisted in free entries.
  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);

  int fd = open(img, O_RDWR);
  assert(fd >= 0);
  void *map = mmap(NULL, (size_t)mapsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  assert(map != MAP_FAILED);

  kafs_context_t reopened = {0};
  reopened.c_img_base = map;
  reopened.c_img_size = (size_t)mapsize;
  reopened.c_superblock = (kafs_ssuperblock_t *)map;
  reopened.c_mapsize = (size_t)mapsize;
  reopened.c_fd = fd;
  {
    off_t meta_off = (off_t)sizeof(kafs_ssuperblock_t);
    off_t bmask = (off_t)bs - 1;
    kafs_blkcnt_t blkcnt = kafs_sb_blkcnt_get(reopened.c_superblock);
    kafs_inocnt_t inocnt = kafs_sb_inocnt_get(reopened.c_superblock);

    meta_off = (meta_off + bmask) & ~bmask;
    reopened.c_blkmasktbl = (kafs_blkmask_t *)((char *)map + meta_off);
    meta_off += (off_t)((blkcnt + 7u) >> 3);
    meta_off = (meta_off + 7) & ~7;
    meta_off = (meta_off + bmask) & ~bmask;
    reopened.c_inotbl = (kafs_sinode_t *)((char *)map + meta_off);
    meta_off += (off_t)sizeof(kafs_sinode_t) * (off_t)inocnt;
    reopened.c_blo_search = 0;
    reopened.c_ino_search = 0;
  }
  uint64_t valid_entry_offset = kafs_sb_hrl_entry_offset_get(reopened.c_superblock);
  uint64_t valid_index_offset = kafs_sb_hrl_index_offset_get(reopened.c_superblock);
  uint32_t *first_index_word = (uint32_t *)((char *)map + valid_index_offset);
  uint32_t saved_index_word = *first_index_word;
  *first_index_word = UINT32_C(0x12345678);
  kafs_sb_hrl_entry_offset_set(reopened.c_superblock, (uint64_t)mapsize + 1u);
  assert(kafs_hrl_open(&reopened) == -EIO);
  assert(kafs_hrl_format(&reopened) == -EIO);
  assert(*first_index_word == UINT32_C(0x12345678));
  kafs_sb_hrl_entry_offset_set(reopened.c_superblock, valid_entry_offset);
  *first_index_word = saved_index_word;

  kafs_ssuperblock_t undersized = *reopened.c_superblock;
  undersized.s_r_blkcnt = kafs_blkcnt_htos(1u);
  kafs_sb_hrl_index_offset_set(&undersized, 0u);
  kafs_sb_hrl_index_size_set(&undersized, 0u);
  kafs_sb_hrl_entry_offset_set(&undersized, 0u);
  kafs_sb_hrl_entry_cnt_set(&undersized, 0u);
  kafs_sb_journal_offset_set(&undersized, 0u);
  kafs_sb_journal_size_set(&undersized, 0u);
  kafs_sb_pendinglog_offset_set(&undersized, 0u);
  kafs_sb_pendinglog_size_set(&undersized, 0u);
  kafs_legacy_map_layout_t undersized_layout;
  assert(kafs_legacy_map_layout_compute(&undersized, &undersized_layout) == -EINVAL);
  assert(kafs_hrl_open(&reopened) == 0);

  memset(buf, 'B', bs);
  assert(kafs_hrl_put(&reopened, buf, &h1, &n1, &b1) == 0);
  assert(n1 == 1);
  assert(kafs_hrl_dec_ref(&reopened, h1) == 0);

  // Clean up
  munmap(reopened.c_superblock, mapsize);
  close(reopened.c_fd);
  unlink(img);
  free(buf);
  return 0;
}
