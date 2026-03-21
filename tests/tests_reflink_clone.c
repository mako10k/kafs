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
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#ifdef __linux__
#include <linux/fs.h>
#endif

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

static int run_kafsctl_cp_reflink(const char *src, const char *dst)
{
  const char *kafsctl = kafs_test_kafsctl_bin();
  pid_t pid = fork();
  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
    char *args[] = {(char *)kafsctl, "cp", (char *)src, (char *)dst, "--reflink", NULL};
    execvp(args[0], args);
    _exit(127);
  }

  int st = 0;
  if (waitpid(pid, &st, 0) < 0)
    return -errno;
  if (!WIFEXITED(st))
    return -1;
  return WEXITSTATUS(st);
}

static const kafs_test_mount_options_t k_mount_options = {
    .log_path = "minisrv.log",
    .timeout_ms = 5000,
};

int main(void)
{
#ifndef __linux__
  return 77;
#else
  const char *img = "reflink.img";
  const char *mnt = "mnt-reflink";

  if (kafs_test_enter_tmpdir("reflink_clone") != 0)
    return 77;

  kafs_context_t ctx;
  off_t mapsize;
  if (kafs_test_mkimg_with_hrl(img, 64u * 1024u * 1024u, 12, 4096, &ctx, &mapsize) != 0)
  {
    tlogf("mkimg failed");
    return 77;
  }
  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);

  pid_t srv = kafs_test_start_kafs(img, mnt, &k_mount_options);
  if (srv <= 0)
  {
    tlogf("mount failed");
    return 77;
  }

  char psrc[PATH_MAX];
  char pdst[PATH_MAX];
  snprintf(psrc, sizeof(psrc), "%s/src", mnt);
  snprintf(pdst, sizeof(pdst), "%s/dst", mnt);

  int s = open(psrc, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (s < 0)
  {
    tlogf("open(src) failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  const size_t bs = 4096;
  unsigned char buf[bs];
  for (size_t i = 0; i < bs; ++i)
    buf[i] = (unsigned char)(i & 0xFF);
  for (int blk = 0; blk < 3; ++blk)
  {
    buf[0] = (unsigned char)blk;
    if (write(s, buf, bs) != (ssize_t)bs)
    {
      tlogf("write(src) failed: %s", strerror(errno));
      close(s);
      kafs_test_stop_kafs(mnt, srv);
      return 1;
    }
  }
  close(s);

  s = open(psrc, O_RDONLY);
  if (s < 0)
  {
    tlogf("open(src-ro) failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  int d = open(pdst, O_CREAT | O_TRUNC | O_RDWR, 0644);
  if (d < 0)
  {
    tlogf("open(dst) failed: %s", strerror(errno));
    close(s);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  if (ioctl(d, FICLONE, s) != 0)
  {
    if (errno != EOPNOTSUPP)
    {
      tlogf("ioctl(FICLONE) failed: %s", strerror(errno));
      close(d);
      close(s);
      kafs_test_stop_kafs(mnt, srv);
      return 1;
    }

    // FUSE typically does not support ioctl(FICLONE) at the VFS layer; fall back to kafsctl.
    close(d);
    close(s);

    int rc = run_kafsctl_cp_reflink(psrc, pdst);
    if (rc != 0)
    {
      tlogf("kafsctl reflink copy failed: rc=%d", rc);
      kafs_test_stop_kafs(mnt, srv);
      return 1;
    }

    s = open(psrc, O_RDONLY);
    if (s < 0)
    {
      tlogf("open(src-ro) after kafsctl failed: %s", strerror(errno));
      kafs_test_stop_kafs(mnt, srv);
      return 1;
    }
    d = open(pdst, O_RDWR);
    if (d < 0)
    {
      tlogf("open(dst) after kafsctl failed: %s", strerror(errno));
      close(s);
      kafs_test_stop_kafs(mnt, srv);
      return 1;
    }
  }

  // modify dst and ensure src stays unchanged
  const char patch[] = "ZZZ";
  if (pwrite(d, patch, sizeof(patch) - 1, 100) != (ssize_t)(sizeof(patch) - 1))
  {
    tlogf("pwrite(dst) failed: %s", strerror(errno));
    close(d);
    close(s);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  char r1[4] = {0}, r2[4] = {0};
  if (pread(d, r1, 3, 100) != 3)
  {
    tlogf("pread(dst) failed: %s", strerror(errno));
    close(d);
    close(s);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (pread(s, r2, 3, 100) != 3)
  {
    tlogf("pread(src) failed: %s", strerror(errno));
    close(d);
    close(s);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (memcmp(r1, "ZZZ", 3) != 0)
  {
    tlogf("dst patch verify failed");
    close(d);
    close(s);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (memcmp(r2, "ZZZ", 3) == 0)
  {
    tlogf("src unexpectedly modified (CoW broken)");
    close(d);
    close(s);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  close(d);
  close(s);
  kafs_test_stop_kafs(mnt, srv);
  return 0;
#endif
}
