#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
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
  assert(kafs_hrl_inc_ref(&ctx, h1) == 0);
  assert(kafs_hrl_put(&ctx, buf, &h2, &n2, &b2) == 0);
  // same content -> same block, not new on second
  assert(n1 == 1);
  assert(n2 == 0);
  assert(b1 == b2);
  assert(h1 == h2);
  // take a second reference for the duplicate content
  assert(kafs_hrl_inc_ref(&ctx, h2) == 0);

  // Decrement twice should free the entry and block
  assert(kafs_hrl_dec_ref(&ctx, h1) == 0);
  assert(kafs_hrl_dec_ref(&ctx, h2) == 0);

  // Ensure freeing again fails
  assert(kafs_hrl_dec_ref(&ctx, h1) != 0);

  // Clean up
  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);
  unlink(img);
  free(buf);
  return 0;
}
