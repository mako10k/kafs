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

static const kafs_test_mount_options_t k_mount_options = {
    .log_path = "minisrv.log",
    .timeout_ms = 5000,
};

static int write_file(const char *path, const char *data, size_t len)
{
  int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0)
    return -errno;
  ssize_t w = write(fd, data, len);
  int e = (w == (ssize_t)len) ? 0 : -errno;
  close(fd);
  return e;
}

static int read_file(const char *path, char *buf, size_t cap)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;
  ssize_t r = read(fd, buf, cap);
  int e = (r >= 0) ? (int)r : -errno;
  close(fd);
  return e;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("rename_overwrite_dirfsync") != 0)
    return 77;

  const char *img = "rename.img";
  const char *mnt = "mnt-rename";
  kafs_context_t ctx;
  off_t mapsize;
  if (kafs_test_mkimg_with_hrl(img, 32u * 1024u * 1024u, 12, 2048, &ctx, &mapsize) != 0)
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

  // 作業ディレクトリ
  char d[PATH_MAX];
  int n = snprintf(d, sizeof(d), "%s/dir", mnt);
  if (n < 0 || n >= (int)sizeof(d))
  {
    kafs_test_stop_kafs(mnt, srv);
    return 77;
  }
  if (mkdir(d, 0777) != 0 && errno != EEXIST)
  {
    tlogf("mkdir dir failed:%s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  char a[PATH_MAX], b[PATH_MAX];
  snprintf(a, sizeof(a), "%s/dir/a.txt", mnt);
  snprintf(b, sizeof(b), "%s/dir/b.txt", mnt);
  if (write_file(a, "AAAA", 4) != 0)
  {
    tlogf("write a failed");
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (write_file(b, "BBBB", 4) != 0)
  {
    tlogf("write b failed");
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  // rename で上書き（atomic replace）
  if (rename(a, b) != 0)
  {
    tlogf("rename failed:%s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  // 直後にbの内容がAAAAか確認
  char buf[8] = {0};
  int r = read_file(b, buf, sizeof(buf));
  if (r < 4 || strncmp(buf, "AAAA", 4) != 0)
  {
    tlogf("post-rename content:%.*s", r > 0 ? r : 0, buf);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  // 親ディレクトリをfsyncして永続性確認
  int dfd = open(d, O_RDONLY | O_DIRECTORY);
  if (dfd >= 0)
  {
    fsync(dfd);
    close(dfd);
  }

  // 再マウントしてもAAAAであること
  kafs_test_stop_kafs(mnt, srv);
  srv = kafs_test_start_kafs(img, mnt, &k_mount_options);
  if (srv <= 0)
  {
    tlogf("remount failed");
    return 77;
  }
  memset(buf, 0, sizeof(buf));
  r = read_file(b, buf, sizeof(buf));
  if (r < 4 || strncmp(buf, "AAAA", 4) != 0)
  {
    tlogf("after remount content:%.*s", r > 0 ? r : 0, buf);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  kafs_test_stop_kafs(mnt, srv);
  tlogf("rename_overwrite_dirfsync OK");
  return 0;
}
