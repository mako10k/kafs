#include "kafs_context.h"
#include "test_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static void tlogf(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  fputc('\n', stderr);
  va_end(ap);
}

static int is_mounted_fuse(const char *mnt)
{
  char absmnt[PATH_MAX];
  const char *want = mnt;
  if (realpath(mnt, absmnt) != NULL)
    want = absmnt;
  FILE *fp = fopen("/proc/mounts", "r");
  if (!fp)
    return 0;
  char dev[256], dir[256], type[64];
  int mounted = 0;
  while (fscanf(fp, "%255s %255s %63s %*[^\n]\n", dev, dir, type) == 3)
  {
    if (strcmp(dir, want) == 0 && strncmp(type, "fuse", 4) == 0)
    {
      mounted = 1;
      break;
    }
  }
  fclose(fp);
  return mounted;
}

static pid_t spawn_kafs(const char *img, const char *mnt)
{
  mkdir(mnt, 0700);
  pid_t pid = fork();
  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
    setenv("KAFS_IMAGE", img, 1);
    int lfd = open("minisrv.log", O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (lfd >= 0)
    {
      dup2(lfd, STDERR_FILENO);
      dup2(lfd, STDOUT_FILENO);
      close(lfd);
    }
    const char *kafs = kafs_test_kafs_bin();
    char *args[] = {(char *)kafs, (char *)mnt, "-f", NULL};
    execvp(args[0], args);
    _exit(127);
  }
  for (int i = 0; i < 50; ++i)
  {
    if (is_mounted_fuse(mnt))
      return pid;
    struct timespec ts = {0, 100 * 1000 * 1000};
    nanosleep(&ts, NULL);
  }
  kill(pid, SIGTERM);
  waitpid(pid, NULL, 0);
  return -1;
}

static int run_cmd(char *const argv[])
{
  pid_t p = fork();
  if (p < 0)
    return -errno;
  if (p == 0)
  {
    execvp(argv[0], argv);
    _exit(127);
  }
  int st = 0;
  if (waitpid(p, &st, 0) < 0)
    return -errno;
  return (WIFEXITED(st) && WEXITSTATUS(st) == 0) ? 0 : -1;
}

static const char *pick_fsck_bin(void)
{
  const char *p = getenv("KAFS_TEST_FSCK");
  if (p && *p)
    return p;
  return "./fsck.kafs";
}

static int count_tombstones(const char *img, uint32_t *out_count)
{
  if (!img || !out_count)
    return -EINVAL;
  *out_count = 0;

  int fd = open(img, O_RDONLY);
  if (fd < 0)
    return -errno;

  kafs_ssuperblock_t sb;
  if (pread(fd, &sb, sizeof(sb), 0) != (ssize_t)sizeof(sb))
  {
    close(fd);
    return -EIO;
  }

  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(&sb);
  kafs_blksize_t blksize = 1u << log_blksize;
  kafs_blksize_t blksizemask = blksize - 1u;
  kafs_inocnt_t inocnt = kafs_sb_inocnt_get(&sb);
  kafs_blkcnt_t r_blkcnt = kafs_sb_r_blkcnt_get(&sb);

  off_t mapsize = 0;
  mapsize += sizeof(kafs_ssuperblock_t);
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  off_t blkmask_off = mapsize;
  mapsize += (r_blkcnt + 7) >> 3;
  mapsize = (mapsize + 7) & ~7;
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  off_t inotbl_off = mapsize;
  mapsize += (off_t)sizeof(kafs_sinode_t) * inocnt;
  mapsize = (mapsize + blksizemask) & ~blksizemask;

  void *base = mmap(NULL, (size_t)mapsize, PROT_READ, MAP_SHARED, fd, 0);
  if (base == MAP_FAILED)
  {
    close(fd);
    return -errno;
  }

  (void)blkmask_off;
  kafs_sinode_t *inotbl = (kafs_sinode_t *)((char *)base + inotbl_off);
  for (kafs_inocnt_t ino = KAFS_INO_ROOTDIR; ino < inocnt; ++ino)
  {
    kafs_sinode_t *e = &inotbl[ino];
    if (!kafs_ino_get_usage(e))
      continue;
    if (kafs_ino_linkcnt_get(e) != 0)
      continue;
    kafs_time_t dtime = kafs_ino_dtime_get(e);
    if (dtime.tv_sec == 0 && dtime.tv_nsec == 0)
      continue;
    (*out_count)++;
  }

  munmap(base, (size_t)mapsize);
  close(fd);
  return 0;
}

static int is_still_mounted(const char *mnt) { return is_mounted_fuse(mnt); }

static void stop_kafs(const char *mnt, pid_t pid)
{
  // 1) 先にサーバプロセスを止める（FUSEの応答待ちを避ける）
  kill(pid, SIGTERM);
  (void)waitpid(pid, NULL, 0);

  // 2) 通常のアンマウントを数回試行
  for (int i = 0; i < 5; ++i)
  {
    if (!is_still_mounted(mnt))
      break;
    char *um1[] = {"fusermount3", "-u", (char *)mnt, NULL};
    if (run_cmd(um1) == 0 && !is_still_mounted(mnt))
      break;
    // 少し待つ
    struct timespec ts = {0, 100 * 1000 * 1000};
    nanosleep(&ts, NULL);
  }

  // 3) まだ残っていたら lazy unmount でフォールバック
  if (is_still_mounted(mnt))
  {
    char *um3[] = {"fusermount3", "-u", "-z", (char *)mnt, NULL};
    (void)run_cmd(um3);
  }

  // 4) 念のため旧fusermountにもフォールバック
  if (is_still_mounted(mnt))
  {
    char *um2[] = {"fusermount", "-u", (char *)mnt, NULL};
    (void)run_cmd(um2);
  }
}

int main(void)
{
  if (kafs_test_enter_tmpdir("open_unlink_visibility") != 0)
    return 77;

  const char *img = "unlink.img";
  const char *mnt = "mnt-unlink";
  kafs_context_t ctx;
  off_t mapsize;
  if (kafs_test_mkimg_with_hrl(img, 16u * 1024u * 1024u, 12, 2048, &ctx, &mapsize) != 0)
  {
    return 77;
  }
  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);

  pid_t srv = spawn_kafs(img, mnt);
  if (srv <= 0)
  {
    return 77;
  }

  char p[PATH_MAX];
  snprintf(p, sizeof(p), "%s/file.txt", mnt);
  int fdw = open(p, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fdw < 0)
  {
    stop_kafs(mnt, srv);
    return 1;
  }
  write(fdw, "abcdef", 6);
  close(fdw);

  int fdr = open(p, O_RDONLY);
  if (fdr < 0)
  {
    stop_kafs(mnt, srv);
    return 1;
  }
  // path unlink
  if (unlink(p) != 0)
  {
    stop_kafs(mnt, srv);
    return 1;
  }
  // 読めること
  char buf[16] = {0};
  ssize_t r = read(fdr, buf, sizeof(buf));
  if (r <= 0)
  {
    tlogf("read after unlink failed");
    close(fdr);
    stop_kafs(mnt, srv);
    return 1;
  }
  close(fdr);
  // パスは消えている
  if (access(p, F_OK) == 0)
  {
    tlogf("path still exists");
    stop_kafs(mnt, srv);
    return 1;
  }

  stop_kafs(mnt, srv);

  const char *img2 = "dir-unlink-leak.img";
  const char *mnt2 = "mnt-dir-unlink";
  if (kafs_test_mkimg_with_hrl(img2, 128u * 1024u * 1024u, 12, 4096, &ctx, &mapsize) != 0)
    return 77;
  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);

  srv = spawn_kafs(img2, mnt2);
  if (srv <= 0)
    return 77;

  for (int i = 1; i <= 8; ++i)
  {
    snprintf(p, sizeof(p), "%s/f%d", mnt2, i);
    int fd = open(p, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0)
    {
      tlogf("create tiny file failed: %s", strerror(errno));
      stop_kafs(mnt2, srv);
      return 1;
    }
    if (write(fd, "x", 1) != 1)
    {
      tlogf("write tiny file failed: %s", strerror(errno));
      close(fd);
      stop_kafs(mnt2, srv);
      return 1;
    }
    close(fd);
  }

  for (int i = 1; i <= 8; ++i)
  {
    snprintf(p, sizeof(p), "%s/f%d", mnt2, i);
    if (unlink(p) != 0)
    {
      tlogf("unlink tiny file failed: %s", strerror(errno));
      stop_kafs(mnt2, srv);
      return 1;
    }
  }

  stop_kafs(mnt2, srv);

  char *fsck_argv[] = {(char *)pick_fsck_bin(), "--check-hrl-blo-refcounts", (char *)img2, NULL};
  if (run_cmd(fsck_argv) != 0)
  {
    tlogf("hrl refcount check failed after deleting 8 tiny files");
    return 1;
  }

  uint32_t tombstones = 0;
  if (count_tombstones(img2, &tombstones) != 0)
  {
    tlogf("failed to count tombstones after unlink");
    return 1;
  }
  if (tombstones == 0)
  {
    tlogf("expected tombstones after immediate stop");
    return 1;
  }

  char *fsck_orphan_argv[] = {(char *)pick_fsck_bin(), "--check-dirent-ino-orphans", (char *)img2,
                              NULL};
  if (run_cmd(fsck_orphan_argv) != 0)
  {
    tlogf("orphan check should ignore tombstones");
    return 1;
  }

  srv = spawn_kafs(img2, mnt2);
  if (srv <= 0)
    return 77;
  {
    struct timespec ts = {2, 0};
    nanosleep(&ts, NULL);
  }
  stop_kafs(mnt2, srv);

  tombstones = 0;
  if (count_tombstones(img2, &tombstones) != 0)
  {
    tlogf("failed to count tombstones after GC window");
    return 1;
  }
  if (tombstones != 0)
  {
    tlogf("tombstone GC did not reclaim unlinked files: count=%u", tombstones);
    return 1;
  }

  const char *img3 = "pressure-unlink.img";
  const char *mnt3 = "mnt-pressure-unlink";
  if (kafs_test_mkimg_with_hrl(img3, 32u * 1024u * 1024u, 12, 12, &ctx, &mapsize) != 0)
    return 77;
  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);

  srv = spawn_kafs(img3, mnt3);
  if (srv <= 0)
    return 77;

  for (int i = 1; i <= 7; ++i)
  {
    snprintf(p, sizeof(p), "%s/fill%d", mnt3, i);
    int fd = open(p, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0)
    {
      tlogf("create pressure filler failed: %s", strerror(errno));
      stop_kafs(mnt3, srv);
      return 1;
    }
    if (write(fd, "z", 1) != 1)
    {
      tlogf("write pressure filler failed: %s", strerror(errno));
      close(fd);
      stop_kafs(mnt3, srv);
      return 1;
    }
    close(fd);
  }

  snprintf(p, sizeof(p), "%s/victim", mnt3);
  {
    int fd = open(p, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0)
    {
      tlogf("create pressure victim failed: %s", strerror(errno));
      stop_kafs(mnt3, srv);
      return 1;
    }
    if (write(fd, "q", 1) != 1)
    {
      tlogf("write pressure victim failed: %s", strerror(errno));
      close(fd);
      stop_kafs(mnt3, srv);
      return 1;
    }
    close(fd);
  }
  if (unlink(p) != 0)
  {
    tlogf("unlink pressure victim failed: %s", strerror(errno));
    stop_kafs(mnt3, srv);
    return 1;
  }

  stop_kafs(mnt3, srv);

  tombstones = 0;
  if (count_tombstones(img3, &tombstones) != 0)
  {
    tlogf("failed to count tombstones after pressure unlink");
    return 1;
  }
  if (tombstones != 0)
  {
    tlogf("pressure path should skip logical delete: count=%u", tombstones);
    return 1;
  }

  tlogf("open_unlink_visibility OK");
  return 0;
}
