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

static pid_t spawn_kafs(const char *img, const char *mnt, const char *debug)
{
  mkdir(mnt, 0700);
  pid_t pid = fork();
  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
    setenv("KAFS_IMAGE", img, 1);
    if (debug)
      setenv("KAFS_DEBUG", debug, 1);
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

int main(void)
{
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

  pid_t srv = spawn_kafs(img, mnt, "1");
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
    stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);
  snprintf(p, sizeof(p), "%s/file/child", mnt);
  if (mkdir(p, 0700) == 0 || errno != ENOTDIR)
  {
    tlogf("expected ENOTDIR, got %d", errno);
    stop_kafs(mnt, srv);
    return 1;
  }

  // 2) O_TRUNC on existing file
  snprintf(p, sizeof(p), "%s/trunc", mnt);
  fd = open(p, O_CREAT | O_WRONLY, 0644);
  if (fd < 0)
  {
    tlogf("create trunc failed:%s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  const char *data = "abc";
  write(fd, data, 3);
  close(fd);
  fd = open(p, O_WRONLY | O_TRUNC, 0644);
  if (fd < 0)
  {
    tlogf("open O_TRUNC failed:%s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);
  struct stat st = {0};
  if (stat(p, &st) != 0 || st.st_size != 0)
  {
    tlogf("truncate not applied size=%ld", (long)st.st_size);
    stop_kafs(mnt, srv);
    return 1;
  }

  // 3) Permission checks: final dir requires write/exec for create
  snprintf(p, sizeof(p), "%s/dir", mnt);
  if (mkdir(p, 0755) != 0)
  {
    tlogf("mkdir dir failed:%s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  if (chmod(p, 0555) != 0)
  {
    tlogf("chmod failed:%s", strerror(errno));
    stop_kafs(mnt, srv);
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
    stop_kafs(mnt, srv);
    return 1;
  }
  if (errno != EACCES && errno != EPERM)
  {
    tlogf("expected EACCES/EPERM, got %d", errno);
    stop_kafs(mnt, srv);
    return 1;
  }

  stop_kafs(mnt, srv);
  tlogf("fs_semantics OK");
  return 0;
}
