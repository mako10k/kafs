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

static const kafs_test_mount_options_t k_mount_options = {
    .debug = "1",
    .log_path = "minisrv.log",
    .timeout_ms = 5000,
};

int main(void)
{
  if (kafs_test_enter_tmpdir("fs_semantics") != 0)
    return 77;

  const char *img = "semantics.img";
  const char *mnt = "mnt-semantics";
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

  char p[PATH_MAX];
  // 1) ENOTDIR: create file then try to mkdir under it
  snprintf(p, sizeof(p), "%s/file", mnt);
  int fd = open(p, O_CREAT | O_WRONLY, 0644);
  if (fd < 0)
  {
    tlogf("create file failed:%s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);
  snprintf(p, sizeof(p), "%s/file/child", mnt);
  if (mkdir(p, 0700) == 0 || errno != ENOTDIR)
  {
    tlogf("expected ENOTDIR, got %d", errno);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  // 2) O_TRUNC on existing file
  snprintf(p, sizeof(p), "%s/trunc", mnt);
  fd = open(p, O_CREAT | O_WRONLY, 0644);
  if (fd < 0)
  {
    tlogf("create trunc failed:%s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  const char *data = "abc";
  if (write(fd, data, 3) != 3)
  {
    tlogf("write trunc failed:%s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);
  fd = open(p, O_WRONLY | O_TRUNC, 0644);
  if (fd < 0)
  {
    tlogf("open O_TRUNC failed:%s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);
  struct stat st = {0};
  if (stat(p, &st) != 0 || st.st_size != 0)
  {
    tlogf("truncate not applied size=%ld", (long)st.st_size);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  // 3) Permission checks: final dir requires write/exec for create
  snprintf(p, sizeof(p), "%s/dir", mnt);
  if (mkdir(p, 0755) != 0)
  {
    tlogf("mkdir dir failed:%s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (chmod(p, 0555) != 0)
  {
    tlogf("chmod failed:%s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  char p2[PATH_MAX];
  snprintf(p2, sizeof(p2), "%s/dir/new", mnt);
  errno = 0;
  int rc = open(p2, O_CREAT | O_WRONLY, 0644);
  if (rc >= 0)
  {
    close(rc);
    tlogf("create should have failed in ro dir");
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (errno != EACCES && errno != EPERM)
  {
    tlogf("expected EACCES/EPERM, got %d", errno);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  kafs_test_stop_kafs(mnt, srv);
  tlogf("fs_semantics OK");
  return 0;
}
