#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_block.h"
#include "kafs_inode.h"
#include "kafs_hash.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

int kafs_test_mkimg(const char *path, size_t bytes, unsigned log_bs, unsigned inodes,
                    int enable_hrl, kafs_context_t *out_ctx, off_t *out_mapsize)
{
  int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
  if (fd < 0)
    return -errno;

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

  // HRL 領域（任意）
  uint32_t bucket_cnt = 0;
  size_t hrl_index_size = 0;
  off_t hrl_index_off = 0;
  uint32_t entry_cnt = 0;
  off_t hrl_entry_off = 0;
  if (enable_hrl)
  {
    bucket_cnt = 1024;
    while ((bucket_cnt << 1) <= (uint32_t)(blkcnt / 4))
      bucket_cnt <<= 1;
    hrl_index_size = (size_t)bucket_cnt * sizeof(uint32_t);
    hrl_index_off = mapsize;
    mapsize += hrl_index_size;
    mapsize = (mapsize + 7) & ~7;
    entry_cnt = (uint32_t)(blkcnt / 2);
    hrl_entry_off = mapsize;
    mapsize += (off_t)entry_cnt * (off_t)sizeof(kafs_hrl_entry_t);
    mapsize = (mapsize + bmask) & ~bmask;
  }

  // In-image journal region (fixed 1MiB for tests)
  size_t journal_bytes = 1u << 20;
  off_t journal_off = mapsize;
  mapsize += (off_t)journal_bytes;
  mapsize = (mapsize + bmask) & ~bmask;

  if (ftruncate(fd, mapsize + (off_t)bs * blkcnt) < 0)
  {
    int err = -errno;
    close(fd);
    return err;
  }

  kafs_ssuperblock_t *sb = mmap(NULL, mapsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (sb == MAP_FAILED)
  {
    int err = -errno;
    close(fd);
    return err;
  }
  memset((char *)sb + (intptr_t)blkmask_off, 0, ((size_t)blkcnt + 7) >> 3);
  memset((char *)sb + (intptr_t)inotbl_off, 0, sizeof(kafs_sinode_t) * inodes);

  kafs_sb_log_blksize_set(sb, log_bs);
  kafs_sb_magic_set(sb, KAFS_MAGIC);
  kafs_sb_format_version_set(sb, KAFS_FORMAT_VERSION);
  kafs_sb_hash_fast_set(sb, KAFS_HASH_FAST_XXH64);
  kafs_sb_hash_strong_set(sb, KAFS_HASH_STRONG_BLAKE3_256);
  if (enable_hrl)
  {
    kafs_sb_hrl_index_offset_set(sb, (uint64_t)hrl_index_off);
    kafs_sb_hrl_index_size_set(sb, (uint64_t)hrl_index_size);
    kafs_sb_hrl_entry_offset_set(sb, (uint64_t)hrl_entry_off);
    kafs_sb_hrl_entry_cnt_set(sb, (uint32_t)entry_cnt);
  }
  kafs_sb_journal_offset_set(sb, (uint64_t)journal_off);
  kafs_sb_journal_size_set(sb, (uint64_t)journal_bytes);
  kafs_sb_journal_flags_set(sb, 0);

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
  c.c_hrl_index = enable_hrl ? (void *)((char *)sb + hrl_index_off) : NULL;
  c.c_hrl_bucket_cnt = enable_hrl ? bucket_cnt : 0u;

  for (kafs_blkcnt_t blo = 0; blo < fdb; ++blo)
    kafs_blk_set_usage(&c, blo, KAFS_TRUE);
  if (enable_hrl)
    kafs_hrl_format(&c);

  *out_ctx = c;
  *out_mapsize = mapsize;
  return 0;
}
