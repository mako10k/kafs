#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_inode.h"
#include "kafs_dirent.h"
#include "test_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static void tlogf(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  fputc('\n', stderr);
  va_end(ap);
}

static const kafs_test_mount_options_t k_mount_options = {
    .debug = "3",
    .log_path = "minisrv.log",
    .timeout_ms = 10000,
};

int main(void)
{
  const unsigned log_bs = 12;
  const kafs_blksize_t bs = 1u << log_bs;
  const char *img = "prune.img";
  const char *mnt = "mnt-prune";

  if (kafs_test_enter_tmpdir("prune_indirect_single") != 0)
    return 77;

  kafs_context_t ctx;
  off_t mapsize;
  if (kafs_test_mkimg_with_hrl(img, 64u * 1024u * 1024u, log_bs, 4096, &ctx, &mapsize) != 0)
  {
    tlogf("mkimg failed");
    return 77;
  }
  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);

  pid_t srv = kafs_test_start_kafs(img, mnt, &k_mount_options);
  if (srv <= 0)
  {
    kafs_test_dump_log("minisrv.log", "mount failed");
    tlogf("mount failed");
    return 77;
  }

  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/file", mnt);
  int fd = open(path, O_CREAT | O_WRONLY, 0644);
  if (fd < 0)
  {
    tlogf("create failed:%s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  off_t data_off = (off_t)12 * bs;
  char *buf = malloc(bs);
  memset(buf, 0xAB, bs);
  ssize_t written = pwrite(fd, buf, bs, data_off);
  free(buf);
  if (written != (ssize_t)bs)
  {
    tlogf("pwrite data failed: %s", strerror(errno));
    close(fd);
    kafs_test_dump_log("minisrv.log", "write failed");
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  char *zero = calloc(1, bs);
  written = pwrite(fd, zero, bs, data_off);
  free(zero);
  if (written != (ssize_t)bs)
  {
    tlogf("pwrite zero failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  fsync(fd);
  close(fd);

  kafs_test_stop_kafs(mnt, srv);

  int ifd = open(img, O_RDONLY);
  if (ifd < 0)
  {
    tlogf("open img failed:%s", strerror(errno));
    return 1;
  }
  void *base = mmap(NULL, mapsize, PROT_READ, MAP_SHARED, ifd, 0);
  if (base == MAP_FAILED)
  {
    tlogf("mmap failed:%s", strerror(errno));
    close(ifd);
    return 1;
  }

  kafs_ssuperblock_t *sb = (kafs_ssuperblock_t *)base;
  kafs_blkcnt_t blkcnt = kafs_sb_blkcnt_get(sb);
  kafs_blksize_t bs_ro = kafs_sb_blksize_get(sb);
  kafs_blksize_t bmask = bs_ro - 1u;
  off_t offset = 0;
  offset = (offset + sizeof(kafs_ssuperblock_t) + bmask) & ~bmask;
  offset += ((blkcnt + 7) >> 3);
  offset = (offset + 7) & ~7;
  offset = (offset + bmask) & ~bmask;
  kafs_sinode_t *inotbl = (kafs_sinode_t *)((char *)base + offset);
  kafs_sinode_t *root = &inotbl[KAFS_INO_ROOTDIR];

  const char *name = "file";
  size_t name_len = strlen(name);
  off_t dir_off = 0;
  kafs_sdirent_t dirent;
  kafs_inocnt_t ino = KAFS_INO_NONE;
  while (dir_off < (off_t)kafs_ino_size_get(root))
  {
    size_t hdr = offsetof(kafs_sdirent_t, d_filename);
    memcpy(&dirent, (char *)root->i_blkreftbl + dir_off, hdr);
    kafs_filenamelen_t dir_len = kafs_dirent_filenamelen_get(&dirent);
    if (dir_len == name_len && memcmp((char *)root->i_blkreftbl + dir_off + hdr, name, name_len) == 0)
    {
      ino = kafs_dirent_ino_get(&dirent);
      break;
    }
    dir_off += hdr + dir_len;
  }
  if (ino == KAFS_INO_NONE)
  {
    tlogf("dir lookup failed");
    munmap(base, mapsize);
    close(ifd);
    return 1;
  }

  kafs_sinode_t *fileino = &inotbl[ino];
  if (kafs_blkcnt_stoh(fileino->i_blkreftbl[12]) != 0)
  {
    tlogf("expected i_blkreftbl[12]==0 after prune, got non-zero");
    munmap(base, mapsize);
    close(ifd);
    return 1;
  }

  munmap(base, mapsize);
  close(ifd);
  tlogf("prune_indirect_single OK");
  return 0;
}
