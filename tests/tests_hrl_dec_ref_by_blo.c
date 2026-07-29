#include "kafs.h"
#include "kafs_block.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_hash.h"
#include "test_utils.h"

#include <assert.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

int main(void)
{
  if (kafs_test_enter_tmpdir("hrl_dec_ref_by_blo") != 0)
    return 77;

  const char *img = "./hrl_dec_ref_by_blo.img";
  kafs_context_t ctx;
  off_t mapsize;
  assert(kafs_test_mkimg_with_hrl(img, 64 * 1024 * 1024u, 12, 1024, &ctx, &mapsize) == 0);

  // write a distinct block
  kafs_blksize_t bs = kafs_sb_blksize_get(ctx.c_superblock);
  char *buf = malloc(bs);
  for (kafs_blksize_t i = 0; i < bs; ++i)
    buf[i] = (char)(i & 0xFF);

  kafs_hrid_t h;
  int is_new;
  kafs_blkcnt_t blo;
  assert(kafs_hrl_put(&ctx, buf, &h, &is_new, &blo) == 0);
  assert(is_new == 1);
  assert(kafs_hrl_inc_ref(&ctx, h) == 0); // take a second reference (total refs = 2)

  // Leave one reference, then verify final release by physical block is retryable.
  assert(kafs_hrl_dec_ref(&ctx, h) == 0);

  int rw_fd = ctx.c_fd;
  int ro_fd = open(img, O_RDONLY);
  assert(ro_fd >= 0);
  ctx.c_fd = ro_fd;
  assert(kafs_hrl_dec_ref_by_blo(&ctx, blo) == -EIO);
  assert(kafs_blk_get_usage_locked(&ctx, blo) != 0);
  ctx.c_fd = rw_fd;
  close(ro_fd);

  assert(kafs_hrl_dec_ref_by_blo(&ctx, blo) == 0);
  assert(kafs_blk_get_usage_locked(&ctx, blo) == 0);

  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);
  unlink(img);
  free(buf);
  return 0;
}
