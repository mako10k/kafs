#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_block.h"
#include "kafs_hash.h"
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
  const char *img = "./hrl_unconfigured.img";
  kafs_context_t ctx;
  off_t mapsize;
  assert(kafs_test_mkimg_no_hrl(img, 32 * 1024 * 1024u, 12, 1024, &ctx, &mapsize) == 0);

  // allocate a raw data block (simulating legacy path)
  kafs_blkcnt_t blo = KAFS_BLO_NONE;
  assert(kafs_blk_alloc(&ctx, &blo) == 0);
  assert(blo != KAFS_BLO_NONE);

  // write some data directly
  kafs_blksize_t bs = kafs_sb_blksize_get(ctx.c_superblock);
  char *buf = malloc(bs);
  memset(buf, 0x5A, bs);
  ssize_t w = pwrite(ctx.c_fd, buf, bs, (off_t)blo << kafs_sb_log_blksize_get(ctx.c_superblock));
  assert(w == (ssize_t)bs);

  // dec_ref_by_blo should fall back to direct free when HRL is off
  assert(kafs_hrl_dec_ref_by_blo(&ctx, blo) == 0);

  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);
  unlink(img);
  free(buf);
  return 0;
}
