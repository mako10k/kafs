#include "kafs_context.h"
#include "test_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
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
    int lfd = open("links.log", O_CREAT | O_TRUNC | O_WRONLY, 0644);
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

static int run_kafsctl(char *const args[])
{
  pid_t pid = fork();
  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
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

int main(void)
{
#ifndef __linux__
  return 77;
#else
  const char *img = "kafsctl-links.img";
  const char *mnt = "mnt-links";

  if (kafs_test_enter_tmpdir("kafsctl_links") != 0)
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

  const char *kafsctl = kafs_test_kafsctl_bin();
  char src[PATH_MAX];
  char sdst[PATH_MAX];
  snprintf(src, sizeof(src), "%s/src", mnt);
  snprintf(sdst, sizeof(sdst), "%s/sym", mnt);

  int fd = open(src, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0)
  {
    tlogf("open(src) failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  if (write(fd, "hello", 5) != 5)
  {
    tlogf("write(src) failed: %s", strerror(errno));
    close(fd);
    stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);

  char *symlink_args[] = {(char *)kafsctl, "symlink", "literal-target", sdst, NULL};
  int rc = run_kafsctl(symlink_args);
  if (rc != 0)
  {
    tlogf("kafsctl symlink failed: rc=%d", rc);
    stop_kafs(mnt, srv);
    return 1;
  }

  char linkbuf[PATH_MAX];
  ssize_t n = readlink(sdst, linkbuf, sizeof(linkbuf) - 1);
  if (n < 0)
  {
    tlogf("readlink failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  linkbuf[n] = '\0';
  if (strcmp(linkbuf, "literal-target") != 0)
  {
    tlogf("symlink target verification failed: %s", linkbuf);
    stop_kafs(mnt, srv);
    return 1;
  }

  char host_src[] = "/tmp/kafsctl-links-host-XXXXXX";
  int host_fd = mkstemp(host_src);
  if (host_fd < 0)
  {
    tlogf("mkstemp failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  close(host_fd);

  char bad_dst[PATH_MAX];
  snprintf(bad_dst, sizeof(bad_dst), "%s/cross", mnt);
  char *bad_ln_args[] = {(char *)kafsctl, "ln", host_src, bad_dst, NULL};
  rc = run_kafsctl(bad_ln_args);
  unlink(host_src);
  if (rc == 0)
  {
    tlogf("cross-fs hardlink unexpectedly succeeded");
    stop_kafs(mnt, srv);
    return 1;
  }
  if (access(bad_dst, F_OK) == 0)
  {
    tlogf("cross-fs hardlink created destination unexpectedly");
    stop_kafs(mnt, srv);
    return 1;
  }

  stop_kafs(mnt, srv);
  return 0;
#endif
}
