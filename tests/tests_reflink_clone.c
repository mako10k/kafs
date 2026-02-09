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

static void stop_kafs(const char *mnt, pid_t pid)
{
  char *um1[] = {"fusermount3", "-u", (char *)mnt, NULL};
  if (fork() == 0)
  {
    execvp(um1[0], um1);
    _exit(127);
  }
  else
  {
    wait(NULL);
  }
  kill(pid, SIGTERM);
  waitpid(pid, NULL, 0);
}

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

  pid_t srv = spawn_kafs(img, mnt);
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
    stop_kafs(mnt, srv);
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
      stop_kafs(mnt, srv);
      return 1;
    }
  }
  close(s);

  s = open(psrc, O_RDONLY);
  if (s < 0)
  {
    tlogf("open(src-ro) failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  int d = open(pdst, O_CREAT | O_TRUNC | O_RDWR, 0644);
  if (d < 0)
  {
    tlogf("open(dst) failed: %s", strerror(errno));
    close(s);
    stop_kafs(mnt, srv);
    return 1;
  }

  if (ioctl(d, FICLONE, s) != 0)
  {
    if (errno != EOPNOTSUPP)
    {
      tlogf("ioctl(FICLONE) failed: %s", strerror(errno));
      close(d);
      close(s);
      stop_kafs(mnt, srv);
      return 1;
    }

    // FUSE typically does not support ioctl(FICLONE) at the VFS layer; fall back to kafsctl.
    close(d);
    close(s);

    char cmd[PATH_MAX * 3];
    snprintf(cmd, sizeof(cmd), "./kafsctl cp %s /src /dst --reflink", mnt);
    int rc = system(cmd);
    if (rc != 0)
    {
      tlogf("kafsctl reflink copy failed: rc=%d", rc);
      stop_kafs(mnt, srv);
      return 1;
    }

    s = open(psrc, O_RDONLY);
    if (s < 0)
    {
      tlogf("open(src-ro) after kafsctl failed: %s", strerror(errno));
      stop_kafs(mnt, srv);
      return 1;
    }
    d = open(pdst, O_RDWR);
    if (d < 0)
    {
      tlogf("open(dst) after kafsctl failed: %s", strerror(errno));
      close(s);
      stop_kafs(mnt, srv);
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
    stop_kafs(mnt, srv);
    return 1;
  }

  char r1[4] = {0}, r2[4] = {0};
  if (pread(d, r1, 3, 100) != 3)
  {
    tlogf("pread(dst) failed: %s", strerror(errno));
    close(d);
    close(s);
    stop_kafs(mnt, srv);
    return 1;
  }
  if (pread(s, r2, 3, 100) != 3)
  {
    tlogf("pread(src) failed: %s", strerror(errno));
    close(d);
    close(s);
    stop_kafs(mnt, srv);
    return 1;
  }
  if (memcmp(r1, "ZZZ", 3) != 0)
  {
    tlogf("dst patch verify failed");
    close(d);
    close(s);
    stop_kafs(mnt, srv);
    return 1;
  }
  if (memcmp(r2, "ZZZ", 3) == 0)
  {
    tlogf("src unexpectedly modified (CoW broken)");
    close(d);
    close(s);
    stop_kafs(mnt, srv);
    return 1;
  }

  close(d);
  close(s);
  stop_kafs(mnt, srv);
  return 0;
#endif
}
