#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_block.h"
#include "kafs_inode.h"
#include "kafs_hash.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static int mkimg(const char *path, size_t bytes, unsigned log_bs, unsigned inodes,
                 kafs_context_t *out_ctx, off_t *out_mapsize)
{
  int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
  if (fd < 0)
  {
    perror("open");
    return -1;
  }
  kafs_blkcnt_t blkcnt = (kafs_blkcnt_t)(bytes >> log_bs);
  kafs_blksize_t bs = 1u << log_bs;
  kafs_blksize_t bmask = bs - 1u;

  off_t mapsize = 0;
  mapsize += sizeof(kafs_ssuperblock_t);
  mapsize = (mapsize + bmask) & ~bmask;
  void *blkmask_off = (void *)mapsize;
  mapsize += (blkcnt + 7) >> 3;
  mapsize = (mapsize + 7) & ~7;
  mapsize = (mapsize + bmask) & ~bmask;
  void *inotbl_off = (void *)mapsize;
  mapsize += sizeof(kafs_sinode_t) * inodes;
  mapsize = (mapsize + bmask) & ~bmask;

  // HRL areas
  uint32_t bucket_cnt = 1024;
  while ((bucket_cnt << 1) <= (uint32_t)(blkcnt / 4))
    bucket_cnt <<= 1;
  size_t hrl_index_size = (size_t)bucket_cnt * sizeof(uint32_t);
  off_t hrl_index_off = mapsize;
  mapsize += hrl_index_size;
  mapsize = (mapsize + 7) & ~7;
  uint32_t entry_cnt = (uint32_t)(blkcnt / 2);
  off_t hrl_entry_off = mapsize;
  mapsize += (off_t)entry_cnt * (off_t)sizeof(kafs_hrl_entry_t);
  mapsize = (mapsize + bmask) & ~bmask;

  if (ftruncate(fd, mapsize + (off_t)bs * blkcnt) < 0)
  {
    perror("ftruncate");
    return -1;
  }

  kafs_ssuperblock_t *sb = mmap(NULL, mapsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (sb == MAP_FAILED)
  {
    perror("mmap");
    return -1;
  }
  memset((char *)sb + (intptr_t)blkmask_off, 0, ((size_t)blkcnt + 7) >> 3);
  memset((char *)sb + (intptr_t)inotbl_off, 0, sizeof(kafs_sinode_t) * inodes);

  kafs_sb_log_blksize_set(sb, log_bs);
  kafs_sb_magic_set(sb, KAFS_MAGIC);
  kafs_sb_format_version_set(sb, KAFS_FORMAT_VERSION);
  kafs_sb_hash_fast_set(sb, KAFS_HASH_FAST_XXH64);
  kafs_sb_hash_strong_set(sb, KAFS_HASH_STRONG_BLAKE3_256);
  kafs_sb_hrl_index_offset_set(sb, (uint64_t)hrl_index_off);
  kafs_sb_hrl_index_size_set(sb, (uint64_t)hrl_index_size);
  kafs_sb_hrl_entry_offset_set(sb, (uint64_t)hrl_entry_off);
  kafs_sb_hrl_entry_cnt_set(sb, (uint32_t)entry_cnt);

  sb->s_inocnt = kafs_inocnt_htos(inodes);
  sb->s_blkcnt = kafs_blkcnt_htos(blkcnt);
  sb->s_r_blkcnt = kafs_blkcnt_htos(blkcnt);
  kafs_blkcnt_t fdb = (kafs_blkcnt_t)(mapsize >> log_bs);
  sb->s_first_data_block = kafs_blkcnt_htos(fdb);
  kafs_sb_blkcnt_free_set(sb, blkcnt - fdb);

  kafs_context_t c = {0};
  c.c_superblock = sb;
  c.c_fd = fd;
  c.c_blkmasktbl = (kafs_blkmask_t *)((char *)sb + (intptr_t)blkmask_off);
  c.c_inotbl = (kafs_sinode_t *)((char *)sb + (intptr_t)inotbl_off);
  c.c_blo_search = 0;
  c.c_ino_search = 0;
  c.c_hrl_index = (void *)((char *)sb + hrl_index_off);
  c.c_hrl_bucket_cnt = bucket_cnt;

  // mark pre-data blocks used
  for (kafs_blkcnt_t blo = 0; blo < fdb; ++blo)
    kafs_blk_set_usage(&c, blo, KAFS_TRUE);

  // format HRL
  kafs_hrl_format(&c);

  *out_ctx = c;
  *out_mapsize = mapsize;
  return 0;
}

int main(void)
{
  const char *img = "./hrl_smoke.img";
  kafs_context_t ctx;
  off_t mapsize;
  assert(mkimg(img, 64 * 1024 * 1024u, 12, 2048, &ctx, &mapsize) == 0);

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
