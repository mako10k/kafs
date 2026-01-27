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
    char *args[] = {"./kafs", (char *)mnt, "-f", NULL};
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
  tlogf("open_unlink_visibility OK");
  return 0;
}
