#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_block.h"
#include "kafs_inode.h"
#include "kafs_hash.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static void usage(const char *prog) {
  fprintf(stderr, "Usage: %s <image> [--size-bytes N|-s N] [--blksize-log L|-b L] [--inodes I|-i I]\n", prog);
  fprintf(stderr, "  defaults: N=1GiB, L=12 (4096B), I=65536\n");
}

int main(int argc, char **argv) {
  const char *img = NULL;
  kafs_logblksize_t log_blksize = 12; // 4096B
  kafs_blksize_t blksize = 1u << log_blksize;
  kafs_blksize_t blksizemask = blksize - 1u;
  off_t bytes = 1024ll * 1024ll * 1024ll; // 1GiB
  kafs_inocnt_t inocnt = 65536;          // number of inodes

  for (int i = 1; i < argc; ++i) {
  if ((strcmp(argv[i], "--size-bytes") == 0 || strcmp(argv[i], "-s") == 0) && i + 1 < argc) {
      bytes = strtoll(argv[++i], NULL, 0);
  } else if ((strcmp(argv[i], "--blksize-log") == 0 || strcmp(argv[i], "-b") == 0) && i + 1 < argc) {
      log_blksize = (kafs_logblksize_t)strtoul(argv[++i], NULL, 0);
      blksize = 1u << log_blksize; blksizemask = blksize - 1u;
  } else if ((strcmp(argv[i], "--inodes") == 0 || strcmp(argv[i], "-i") == 0) && i + 1 < argc) {
      inocnt = (kafs_inocnt_t)strtoul(argv[++i], NULL, 0);
    } else if (argv[i][0] != '-' && img == NULL) {
      img = argv[i];
    } else {
      usage(argv[0]);
      return 2;
    }
  }
  if (!img) { usage(argv[0]); return 2; }

  kafs_blkcnt_t blkcnt = (kafs_blkcnt_t)(bytes >> log_blksize);

  kafs_context_t ctx = {0};
  ctx.c_fd = open(img, O_RDWR | O_CREAT | O_TRUNC, 0666);
  if (ctx.c_fd < 0) { perror("open"); return 1; }
  ctx.c_blo_search = 0; ctx.c_ino_search = 0;

  // レイアウト計算
  off_t mapsize = 0;
  mapsize += sizeof(kafs_ssuperblock_t);
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  ctx.c_superblock = NULL;
  ctx.c_blkmasktbl = (void*)mapsize;

  assert(sizeof(kafs_blkmask_t) <= 8);
  mapsize += (blkcnt - 7) >> 3; // bitmap (bytes)
  mapsize = (mapsize + 7) & ~7; // 64-bit align
  mapsize = (mapsize + blksizemask) & ~blksizemask; // block align
  ctx.c_inotbl = (void*)mapsize;

  mapsize += sizeof(kafs_sinode_t) * inocnt;
  mapsize = (mapsize + blksizemask) & ~blksizemask;

  if (ftruncate(ctx.c_fd, mapsize + (off_t)blksize * blkcnt) < 0) {
    perror("ftruncate");
    return 1;
  }

  ctx.c_superblock = mmap(NULL, mapsize, PROT_READ | PROT_WRITE, MAP_SHARED, ctx.c_fd, 0);
  if (ctx.c_superblock == MAP_FAILED) { perror("mmap"); return 1; }
  ctx.c_blkmasktbl = (void*)ctx.c_superblock + (intptr_t)ctx.c_blkmasktbl;
  ctx.c_inotbl = (void*)ctx.c_superblock + (intptr_t)ctx.c_inotbl;

  // スーパーブロック基本
  kafs_sb_log_blksize_set(ctx.c_superblock, log_blksize);
  kafs_sb_magic_set(ctx.c_superblock, KAFS_MAGIC);
  kafs_sb_format_version_set(ctx.c_superblock, KAFS_FORMAT_VERSION);
  kafs_sb_hash_fast_set(ctx.c_superblock, KAFS_HASH_FAST_XXH64);
  kafs_sb_hash_strong_set(ctx.c_superblock, KAFS_HASH_STRONG_BLAKE3_256);
  // HRL領域は未割当
  kafs_sb_hrl_index_offset_set(ctx.c_superblock, 0);
  kafs_sb_hrl_index_size_set(ctx.c_superblock, 0);
  kafs_sb_hrl_entry_offset_set(ctx.c_superblock, 0);
  kafs_sb_hrl_entry_cnt_set(ctx.c_superblock, 0);

  // R/O items
  ctx.c_superblock->s_inocnt = kafs_inocnt_htos(inocnt);
  ctx.c_superblock->s_blkcnt = kafs_blkcnt_htos(blkcnt * 0.95);
  ctx.c_superblock->s_r_blkcnt = kafs_blkcnt_htos(blkcnt);
  kafs_blkcnt_t fdb = (kafs_blkcnt_t)(mapsize >> log_blksize);
  ctx.c_superblock->s_first_data_block = kafs_blkcnt_htos(fdb);
  kafs_sb_blkcnt_free_set(ctx.c_superblock, blkcnt - fdb);

  for (kafs_blkcnt_t blo = 0; blo < fdb; ++blo)
    kafs_blk_set_usage(&ctx, blo, KAFS_TRUE);

  // root inode
  ctx.c_inotbl->i_mode.value = 0xffff;
  kafs_sinode_t *inoent_rootdir = &ctx.c_inotbl[KAFS_INO_ROOTDIR];
  kafs_ino_mode_set(inoent_rootdir, S_IFDIR | 0755);
  kafs_ino_uid_set(inoent_rootdir, 0);
  kafs_ino_size_set(inoent_rootdir, 0);
  kafs_time_t now = kafs_now();
  kafs_time_t nulltime = (kafs_time_t){0,0};
  kafs_ino_atime_set(inoent_rootdir, now);
  kafs_ino_ctime_set(inoent_rootdir, now);
  kafs_ino_mtime_set(inoent_rootdir, now);
  kafs_ino_dtime_set(inoent_rootdir, nulltime);
  kafs_ino_gid_set(inoent_rootdir, 0);
  kafs_ino_linkcnt_set(inoent_rootdir, 1);
  kafs_ino_blocks_set(inoent_rootdir, 0);
  kafs_ino_dev_set(inoent_rootdir, 0);
  // ルートディレクトリのエントリは省略（"." は readdir で注入、".." は任意）

  // HRL 初期化（ひな型）
  (void)kafs_hrl_format(&ctx);

  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);

  fprintf(stderr, "Formatted %s: size=%lld bytes, blksize=%u, blocks=%u, inodes=%u\n",
          img, (long long)bytes, (unsigned)blksize, (unsigned)blkcnt, (unsigned)inocnt);
  return 0;
}
