#include "kafs_context.h"
#include "test_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
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

// Simple log
static void tlogf(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  fputc('\n', stderr);
  va_end(ap);
}

// Resolve kafs executable path
static const char *pick_kafs_exe(void)
{
  static char chosen[PATH_MAX];
  const char *env = getenv("KAFS_TEST_KAFS");
  if (env && *env)
    return env;
  const char *cands[] = {"./kafs", "../src/kafs", "./src/kafs", "src/kafs", "kafs", NULL};
  for (int i = 0; cands[i]; ++i)
  {
    const char *c = cands[i];
    if (strchr(c, '/'))
    {
      if (access(c, X_OK) == 0)
      {
        strncpy(chosen, c, sizeof(chosen) - 1);
        chosen[sizeof(chosen) - 1] = '\0';
        return chosen;
      }
    }
    else
    {
      return c;
    }
  }
  return "kafs";
}

static int run_cmd(char *const argv[])
{
  pid_t pid = fork();
  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
    execvp(argv[0], argv);
    _exit(127);
  }
  int status = 0;
  if (waitpid(pid, &status, 0) < 0)
    return -errno;
  if (WIFEXITED(status) && WEXITSTATUS(status) == 0)
    return 0;
  return -1;
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

static pid_t mount_kafs(const char *img, const char *mnt)
{
  const char *kafs_exe = pick_kafs_exe();
  mkdir(mnt, 0700);
  pid_t pid = fork();
  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
    setenv("KAFS_IMAGE", img, 1);
    setenv("KAFS_DEBUG", "1", 1);
    // redirect server logs to file for later inspection
    int lfd = open("minisrv.log", O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (lfd >= 0)
    {
      dup2(lfd, STDERR_FILENO);
      dup2(lfd, STDOUT_FILENO);
      close(lfd);
    }
    char *args[] = {(char *)kafs_exe, (char *)mnt, "-f", "-o", "allow_other", NULL};
    execvp(kafs_exe, args);
    perror("execvp kafs");
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

static void unmount_kafs(const char *mnt, pid_t pid)
{
  char *um1[] = {"fusermount3", "-u", (char *)mnt, NULL};
  if (run_cmd(um1) != 0)
  {
    char *um2[] = {"fusermount", "-u", (char *)mnt, NULL};
    (void)run_cmd(um2);
  }
  kill(pid, SIGTERM);
  waitpid(pid, NULL, 0);
}

int main(void)
{
  if (kafs_test_enter_tmpdir("min_git_hooks") != 0)
    return 77;

  const char *img = "mini.img";
  const char *mnt = "mnt-mini";
  kafs_context_t ctx;
  off_t mapsize;
  if (kafs_test_mkimg_with_hrl(img, 64u * 1024u * 1024u, 12 /*4KiB*/, 4096, &ctx, &mapsize) != 0)
  {
    tlogf("mkimg failed");
    return 1;
  }
  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);

  pid_t srv = mount_kafs(img, mnt);
  if (srv <= 0)
  {
    tlogf("mount failed (fuse perms?)");
    return 77;
  }

  // Ensure root of mount is writable by current user
  uid_t uid = getuid();
  gid_t gid = getgid();
  if (chown(mnt, uid, gid) != 0)
  {
    tlogf("chown mount root failed: %s", strerror(errno));
  }
  if (chmod(mnt, 0777) != 0)
  {
    tlogf("chmod mount root failed: %s", strerror(errno));
  }

  // 1) mkdir -p .git/hooks
  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/.git", mnt);
  if (mkdir(path, 0777) != 0 && errno != EEXIST)
  {
    tlogf("mkdir .git failed: %s", strerror(errno));
    unmount_kafs(mnt, srv);
    return 1;
  }
  snprintf(path, sizeof(path), "%s/.git/hooks", mnt);
  if (mkdir(path, 0777) != 0 && errno != EEXIST)
  {
    tlogf("mkdir .git/hooks failed: %s", strerror(errno));
    unmount_kafs(mnt, srv);
    return 1;
  }

  // sanity: check types of .git and .git/hooks
  struct stat st1 = {0}, st2 = {0};
  char p1[PATH_MAX], p2[PATH_MAX];
  snprintf(p1, sizeof(p1), "%s/.git", mnt);
  snprintf(p2, sizeof(p2), "%s/.git/hooks", mnt);
  if (lstat(p1, &st1) == 0)
    tlogf("TYPE .git: %s", S_ISDIR(st1.st_mode) ? "dir" : (S_ISREG(st1.st_mode) ? "reg" : "other"));
  if (lstat(p2, &st2) == 0)
    tlogf("TYPE .git/hooks: %s",
          S_ISDIR(st2.st_mode) ? "dir" : (S_ISREG(st2.st_mode) ? "reg" : "other"));

  // tiny settle
  struct timespec ts = {0, 50 * 1000 * 1000};
  nanosleep(&ts, NULL);

  // 2) create a file under hooks
  snprintf(path, sizeof(path), "%s/.git/hooks/post-checkout", mnt);
  int fd = open(path, O_CREAT | O_WRONLY, 0755);
  if (fd < 0)
  {
    tlogf("open hooks file failed: %s", strerror(errno));
    // show server log tail
    FILE *lf = fopen("minisrv.log", "r");
    if (lf)
    {
      fseek(lf, 0, SEEK_END);
      long sz = ftell(lf);
      long tail = 4000;
      if (sz < tail)
        tail = sz;
      fseek(lf, -tail, SEEK_END);
      char buf[512];
      size_t n;
      tlogf("--- server log tail ---");
      while ((n = fread(buf, 1, sizeof(buf), lf)) > 0)
        fwrite(buf, 1, n, stderr);
      fclose(lf);
    }
    unmount_kafs(mnt, srv);
    return 1;
  }
  const char *data = "#!/bin/sh\nexit 0\n";
  (void)!write(fd, data, (unsigned)strlen(data));
  close(fd);

  // 3) mkdir/rmdir empty
  snprintf(path, sizeof(path), "%s/empty", mnt);
  if (mkdir(path, 0700) != 0)
  {
    tlogf("mkdir empty failed: %s", strerror(errno));
    unmount_kafs(mnt, srv);
    return 1;
  }
  if (rmdir(path) != 0)
  {
    tlogf("rmdir empty failed: %s", strerror(errno));
    unmount_kafs(mnt, srv);
    return 1;
  }

  unmount_kafs(mnt, srv);
  tlogf("mini git hooks test finished OK");
  return 0;
}
