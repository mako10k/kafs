#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_block.h"
#include "kafs_hash.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static int mkimg_no_hrl(const char *path, size_t bytes, unsigned log_bs, unsigned inodes, kafs_context_t *out_ctx, off_t *out_mapsize) {
  int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
  if (fd < 0) { perror("open"); return -1; }
  kafs_blkcnt_t blkcnt = (kafs_blkcnt_t)(bytes >> log_bs);
  kafs_blksize_t bs = 1u << log_bs;
  kafs_blksize_t bmask = bs - 1u;

  off_t mapsize = 0;
  mapsize += sizeof(kafs_ssuperblock_t);
  mapsize = (mapsize + bmask) & ~bmask;
  void *blkmask_off = (void*)mapsize;
  mapsize += (blkcnt + 7) >> 3;
  mapsize = (mapsize + 7) & ~7;
  mapsize = (mapsize + bmask) & ~bmask;
  void *inotbl_off = (void*)mapsize;
  mapsize += sizeof(kafs_sinode_t) * inodes;
  mapsize = (mapsize + bmask) & ~bmask;

  if (ftruncate(fd, mapsize + (off_t)bs * blkcnt) < 0) { perror("ftruncate"); return -1; }

  kafs_ssuperblock_t *sb = mmap(NULL, mapsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (sb == MAP_FAILED) { perror("mmap"); return -1; }
  memset((char*)sb + (intptr_t)blkmask_off, 0, ((size_t)blkcnt + 7) >> 3);
  memset((char*)sb + (intptr_t)inotbl_off, 0, sizeof(kafs_sinode_t) * inodes);

  kafs_sb_log_blksize_set(sb, log_bs);
  kafs_sb_magic_set(sb, KAFS_MAGIC);
  kafs_sb_format_version_set(sb, KAFS_FORMAT_VERSION);
  kafs_sb_hash_fast_set(sb, KAFS_HASH_FAST_XXH64);
  kafs_sb_hash_strong_set(sb, KAFS_HASH_STRONG_BLAKE3_256);
  // HRL fields left zero intentionally

  sb->s_inocnt = kafs_inocnt_htos(inodes);
  sb->s_blkcnt = kafs_blkcnt_htos(blkcnt);
  sb->s_r_blkcnt = kafs_blkcnt_htos(blkcnt);
  kafs_blkcnt_t fdb = (kafs_blkcnt_t)(mapsize >> log_bs);
  sb->s_first_data_block = kafs_blkcnt_htos(fdb);
  kafs_sb_blkcnt_free_set(sb, blkcnt - fdb);

  kafs_context_t c = {0};
  c.c_superblock = sb;
  c.c_fd = fd;
  c.c_blkmasktbl = (kafs_blkmask_t*)((char*)sb + (intptr_t)blkmask_off);
  c.c_inotbl = (kafs_sinode_t*)((char*)sb + (intptr_t)inotbl_off);
  c.c_blo_search = 0;
  c.c_ino_search = 0;
  c.c_hrl_index = NULL;
  c.c_hrl_bucket_cnt = 0; // HRL unconfigured

  for (kafs_blkcnt_t blo = 0; blo < fdb; ++blo) kafs_blk_set_usage(&c, blo, KAFS_TRUE);

  *out_ctx = c;
  *out_mapsize = mapsize;
  return 0;
}

int main(void) {
  const char *img = "./hrl_unconfigured.img";
  kafs_context_t ctx; off_t mapsize;
  assert(mkimg_no_hrl(img, 32*1024*1024u, 12, 1024, &ctx, &mapsize) == 0);

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
