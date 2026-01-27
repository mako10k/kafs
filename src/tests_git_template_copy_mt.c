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

static pid_t spawn_kafs_mt(const char *img, const char *mnt)
{
  mkdir(mnt, 0700);
  pid_t pid = fork();
  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
    setenv("KAFS_IMAGE", img, 1);
    setenv("KAFS_MT", "1", 1); // マルチスレッドで動かす
    setenv("KAFS_DEBUG", "2", 1);
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

static int write_pattern_file(const char *path, size_t sz, unsigned seed)
{
  int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0)
    return -errno;
  unsigned x = seed ? seed : 1u;
  char buf[8192];
  size_t left = sz;
  while (left > 0)
  {
    size_t n = left < sizeof(buf) ? left : sizeof(buf);
    for (size_t i = 0; i < n; ++i)
    {
      x = 1664525u * x + 1013904223u;
      buf[i] = (char)(x >> 24);
    }
    ssize_t w = write(fd, buf, n);
    if (w < 0)
    {
      int e = -errno;
      close(fd);
      return e;
    }
    left -= (size_t)w;
  }
  close(fd);
  return 0;
}

static int copy_file(const char *src, const char *dst)
{
  int s = open(src, O_RDONLY);
  if (s < 0)
    return -errno;
  int d = open(dst, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (d < 0)
  {
    int e = -errno;
    close(s);
    return e;
  }
  char buf[8192];
  ssize_t n;
  while ((n = read(s, buf, sizeof(buf))) > 0)
  {
    ssize_t off = 0;
    while (off < n)
    {
      ssize_t w = write(d, buf + off, (size_t)(n - off));
      if (w < 0)
      {
        int e = -errno;
        close(s);
        close(d);
        return e;
      }
      off += w;
    }
  }
  int rc = (n < 0) ? -errno : 0;
  close(s);
  close(d);
  return rc;
}

static int checksum_file(const char *path, unsigned *out)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;
  unsigned sum = 0;
  char buf[8192];
  ssize_t n;
  while ((n = read(fd, buf, sizeof(buf))) > 0)
  {
    for (ssize_t i = 0; i < n; ++i)
      sum = (sum * 131u) + (unsigned)(unsigned char)buf[i];
  }
  int rc = (n < 0) ? -errno : 0;
  close(fd);
  if (rc == 0)
    *out = sum;
  return rc;
}

int main(void)
{
  const char *img = "clone-mt.img";
  const char *mnt = "mnt-clone-mt";
  const char *host_tmpl = "host-tmpl-mt";

  // 1) 事前にホスト側に多数のテンプレートファイルを作る
  mkdir(host_tmpl, 0700);
  const int N = 32; // 十分な数を一気にコピー
  unsigned expect_sum[N];
  for (int i = 0; i < N; ++i)
  {
    char p[PATH_MAX];
    int pl = snprintf(p, sizeof(p), "%s/f%02d.sample", host_tmpl, i);
    if (pl < 0 || pl >= (int)sizeof(p))
    {
      tlogf("src path too long");
      return 77;
    }
    size_t sz;
    switch (i % 5)
    {
    case 0:
      sz = 0;
      break;
    case 1:
      sz = 17;
      break;
    case 2:
      sz = 8192;
      break;
    case 3:
      sz = 65536;
      break;
    default:
      sz = 12345;
    }
    if (write_pattern_file(p, sz, (unsigned)(0xC001u + i)) != 0)
    {
      tlogf("prepare src failed: %d", errno);
      return 77;
    }
    if (checksum_file(p, &expect_sum[i]) != 0)
    {
      tlogf("src checksum failed");
      return 77;
    }
  }

  // 2) 画像を作成してマウント（KAFS_MT=1）
  kafs_context_t ctx;
  off_t mapsize;
  if (kafs_test_mkimg_with_hrl(img, 64u * 1024u * 1024u, 12, 4096, &ctx, &mapsize) != 0)
  {
    tlogf("mkimg failed");
    return 77;
  }
  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);

  pid_t srv = spawn_kafs_mt(img, mnt);
  if (srv <= 0)
  {
    tlogf("mount failed");
    return 77;
  }

  // 3) 目的ディレクトリ（.git/hooks）を作る
  char target[PATH_MAX];
  snprintf(target, sizeof(target), "%s/clone/.git/hooks", mnt);
  if (mkdir("mnt-clone-mt/clone", 0777) != 0 && errno != EEXIST)
  {
    tlogf("mkdir clone failed:%s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  if (mkdir("mnt-clone-mt/clone/.git", 0777) != 0 && errno != EEXIST)
  {
    tlogf("mkdir .git failed:%s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  if (mkdir(target, 0777) != 0 && errno != EEXIST)
  {
    tlogf("mkdir hooks failed:%s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }

  // 4) 並行コピー（ワーカー4つ）
  const int W = 4;
  pid_t workers[W];
  memset(workers, 0, sizeof(workers));
  for (int w = 0; w < W; ++w)
  {
    pid_t c = fork();
    if (c == 0)
    {
      for (int i = w; i < N; i += W)
      {
        char src[PATH_MAX];
        int sl = snprintf(src, sizeof(src), "%s/f%02d.sample", host_tmpl, i);
        if (sl < 0 || sl >= (int)sizeof(src))
        {
          tlogf("src path too long %d", i);
          _exit(11);
        }
        char dst[PATH_MAX];
        int dl = snprintf(dst, sizeof(dst), "%s/f%02d.sample", target, i);
        if (dl < 0 || dl >= (int)sizeof(dst))
        {
          tlogf("dst path too long %d", i);
          _exit(12);
        }
        int rc = copy_file(src, dst);
        if (rc != 0)
        {
          tlogf("copy %d failed:%s", i, strerror(-rc));
          _exit(10);
        }
      }
      _exit(0);
    }
    workers[w] = c;
  }
  int ok = 1;
  for (int w = 0; w < W; ++w)
  {
    int st = 0;
    waitpid(workers[w], &st, 0);
    if (!WIFEXITED(st) || WEXITSTATUS(st) != 0)
      ok = 0;
  }

  // 5) 検証（サイズ・チェックサム）
  if (ok)
  {
    for (int i = 0; i < N; ++i)
    {
      char dst[PATH_MAX];
      int dl = snprintf(dst, (size_t)sizeof(dst), "%s/f%02d.sample", target, i);
      if (dl < 0 || dl >= (int)sizeof(dst))
      {
        tlogf("dst path too long %d", i);
        ok = 0;
        break;
      }
      unsigned got = 0;
      int rc = checksum_file(dst, &got);
      if (rc != 0)
      {
        tlogf("verify open failed %d:%s", i, strerror(-rc));
        ok = 0;
        break;
      }
      if (got != expect_sum[i])
      {
        tlogf("checksum mismatch %d", i);
        ok = 0;
        break;
      }
    }
  }

  if (!ok)
  {
    // 失敗時はログを出力
    FILE *fp = fopen("minisrv.log", "r");
    if (fp)
    {
      char line[512];
      while (fgets(line, sizeof(line), fp))
        fputs(line, stderr);
      fclose(fp);
    }
    stop_kafs(mnt, srv);
    return 1;
  }

  stop_kafs(mnt, srv);
  tlogf("git_template_copy_mt OK");
  return 0;
}
