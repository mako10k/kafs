#define KAFS_NO_MAIN
#include "kafs.c"
#include "test_utils.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static void init_test_inode(kafs_sinode_t *inoent, kafs_mode_t mode, kafs_off_t size,
                            kafs_blkcnt_t blo)
{
  memset(inoent, 0, sizeof(*inoent));
  kafs_ino_mode_set(inoent, mode);
  kafs_ino_uid_set(inoent, (kafs_uid_t)getuid());
  kafs_ino_gid_set(inoent, (kafs_gid_t)getgid());
  kafs_ino_size_set(inoent, size);
  kafs_time_t now = kafs_now();
  kafs_time_t zero_time = (kafs_time_t){0, 0};
  kafs_ino_atime_set(inoent, now);
  kafs_ino_ctime_set(inoent, now);
  kafs_ino_mtime_set(inoent, now);
  kafs_ino_dtime_set(inoent, zero_time);
  kafs_ino_linkcnt_set(inoent, 1);
  kafs_ino_blocks_set(inoent, 1);
  inoent->i_blkreftbl[0] = kafs_blkcnt_htos(blo);
}

int main(void)
{
  if (kafs_test_enter_tmpdir("bg_dedup_skip_dirs") != 0)
    return 77;

  const char *img = "./bg_dedup_skip_dirs.img";
  kafs_context_t ctx;
  off_t mapsize = 0;
  assert(kafs_test_mkimg_with_hrl(img, 64 * 1024 * 1024u, 12, 64, &ctx, &mapsize) == 0);

  struct stat st;
  assert(fstat(ctx.c_fd, &st) == 0);
  ctx.c_img_base = mmap(NULL, (size_t)st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, ctx.c_fd, 0);
  assert(ctx.c_img_base != MAP_FAILED);
  ctx.c_img_size = (size_t)st.st_size;

  assert(kafs_ctx_locks_init(&ctx) == 0);
  assert(kafs_hrl_open(&ctx) == 0);

  kafs_inocnt_t inocnt = kafs_sb_inocnt_get(ctx.c_superblock);
  ctx.c_ino_epoch = calloc((size_t)inocnt, sizeof(uint32_t));
  assert(ctx.c_ino_epoch != NULL);
  for (kafs_inocnt_t ino = 0; ino < inocnt; ++ino)
    ctx.c_ino_epoch[ino] = 1u;

  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx.c_superblock);
  char *block = malloc((size_t)blksize);
  assert(block != NULL);
  memset(block, 0x5a, (size_t)blksize);

  kafs_hrid_t hrid = 0;
  int is_new = 0;
  kafs_blkcnt_t hrl_blo = KAFS_BLO_NONE;
  assert(kafs_hrl_put(&ctx, block, &hrid, &is_new, &hrl_blo) == 0);
  assert(hrl_blo != KAFS_BLO_NONE);

  kafs_blkcnt_t dir_blo = KAFS_BLO_NONE;
  assert(kafs_blk_alloc(&ctx, &dir_blo) == 0);
  assert(dir_blo != KAFS_BLO_NONE);
  assert(kafs_blk_write(&ctx, dir_blo, block) == 0);

  kafs_inocnt_t dir_ino = KAFS_INO_ROOTDIR + 1u;
  init_test_inode(&ctx.c_inotbl[dir_ino], S_IFDIR | 0755, (kafs_off_t)blksize, dir_blo);

  ctx.c_bg_dedup_ino_cursor = (uint32_t)dir_ino;
  ctx.c_bg_dedup_iblk_cursor = 0;
  ctx.c_bg_dedup_anchor_valid = 0;
  ctx.c_bg_dedup_cooldown_until_ns = 0;

  kafs_bg_dedup_step(&ctx, 0);

  kafs_blkcnt_t final_blo = kafs_blkcnt_stoh(ctx.c_inotbl[dir_ino].i_blkreftbl[0]);
  if (final_blo != dir_blo)
  {
    fprintf(stderr,
            "background dedup touched directory block: dir_ino=%u old=%u new=%u hrl=%u\n",
            (unsigned)dir_ino, (unsigned)dir_blo, (unsigned)final_blo, (unsigned)hrl_blo);
    return 1;
  }

  free(block);
  free(ctx.c_ino_epoch);
  ctx.c_ino_epoch = NULL;
  kafs_ctx_locks_destroy(&ctx);
  (void)kafs_hrl_close(&ctx);
  munmap(ctx.c_img_base, ctx.c_img_size);
  munmap(ctx.c_superblock, (size_t)mapsize);
  close(ctx.c_fd);
  unlink(img);
  return 0;
}
