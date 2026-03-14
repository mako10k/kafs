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

  // Decrement twice should free the entry and block
  assert(kafs_hrl_dec_ref(&ctx, h1) == 0);
  assert(kafs_hrl_dec_ref(&ctx, h2) == 0);

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
  reopened.c_superblock = (kafs_ssuperblock_t *)map;
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
