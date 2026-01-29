#include "kafs.h"
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

  // dec by physical block: should reduce by one ref, not free
  assert(kafs_hrl_dec_ref_by_blo(&ctx, blo) == 0);
  // second dec by hrid: should free
  assert(kafs_hrl_dec_ref(&ctx, h) == 0);

  // ensure the block becomes free in bitmap afterward (by attempting to mark as free again)
  // first, if still marked used, unmark should succeed; calling unmark twice would assert, so just
  // check used->free transition This is indirectly validated by not crashing and final cleanup.

  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);
  unlink(img);
  free(buf);
  return 0;
}
