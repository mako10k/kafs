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
    ssize_t w = write(d, buf, (size_t)n);
    if (w != n)
    {
      int e = -errno;
      close(s);
      close(d);
      return e;
    }
  }
  int rc = (n < 0) ? -errno : 0;
  close(s);
  close(d);
  return rc;
}

int main(void)
{
  const char *img = "clone.img";
  const char *mnt = "mnt-clone";
  kafs_context_t ctx;
  off_t mapsize;
  if (kafs_test_mkimg_with_hrl(img, 32u * 1024u * 1024u, 12, 2048, &ctx, &mapsize) != 0)
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

  // prepare template dir with some files
  char tdir[PATH_MAX];
  snprintf(tdir, sizeof(tdir), "%s/tmpl", mnt);
  if (mkdir(tdir, 0777) != 0)
  {
    tlogf("mkdir tmpl failed:%s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  char sfile[PATH_MAX];
  snprintf(sfile, sizeof(sfile), "%s/tmpl/a.txt", mnt);
  int fd = open(sfile, O_CREAT | O_WRONLY, 0644);
  if (fd < 0)
  {
    tlogf("create a.txt failed:%s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  write(fd, "hello", 5);
  close(fd);

  // create target dir and copy
  // ensure parent dirs
  char target[PATH_MAX];
  snprintf(target, sizeof(target), "%s/target", mnt);
  if (mkdir(target, 0777) != 0 && errno != EEXIST)
  {
    tlogf("mkdir target failed:%s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  char cmd[PATH_MAX];
  snprintf(cmd, sizeof(cmd), "%s/target/.git", mnt);
  if (mkdir(cmd, 0777) != 0 && errno != EEXIST)
  {
    tlogf("mkdir .git failed:%s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  char ddir[PATH_MAX];
  snprintf(ddir, sizeof(ddir), "%s/target/.git/hooks", mnt);
  if (mkdir(ddir, 0777) != 0 && errno != EEXIST)
  {
    tlogf("mkdir hooks failed:%s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }

  char dst[PATH_MAX];
  snprintf(dst, sizeof(dst), "%s/target/.git/hooks/a.txt", mnt);
  int rc = copy_file(sfile, dst);
  if (rc != 0)
  {
    tlogf("copy failed:%s", strerror(-rc));
    stop_kafs(mnt, srv);
    return 1;
  }

  // verify content
  char buf[16] = {0};
  fd = open(dst, O_RDONLY);
  if (fd < 0)
  {
    tlogf("open dst failed:%s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  read(fd, buf, sizeof(buf));
  close(fd);
  if (strncmp(buf, "hello", 5) != 0)
  {
    tlogf("content mismatch: %s", buf);
    stop_kafs(mnt, srv);
    return 1;
  }

  stop_kafs(mnt, srv);
  tlogf("clone_template_copy OK");
  return 0;
}
