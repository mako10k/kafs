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

static int read_whole_file(const char *path, char *buf, size_t size)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;

  size_t off = 0;
  while (off < size)
  {
    ssize_t n = read(fd, buf + off, size - off);
    if (n < 0)
    {
      int rc = -errno;
      close(fd);
      return rc;
    }
    if (n == 0)
      break;
    off += (size_t)n;
  }

  close(fd);
  return (int)off;
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
  char hdst[PATH_MAX];
  char sdst[PATH_MAX];
  snprintf(src, sizeof(src), "%s/src", mnt);
  snprintf(hdst, sizeof(hdst), "%s/hard", mnt);
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

  char *ln_args[] = {(char *)kafsctl, "ln", src, hdst, NULL};
  int rc = run_kafsctl(ln_args);
  if (rc != 0)
  {
    tlogf("kafsctl ln failed: rc=%d", rc);
    stop_kafs(mnt, srv);
    return 1;
  }

  struct stat st_src;
  struct stat st_hdst;
  if (stat(src, &st_src) != 0 || stat(hdst, &st_hdst) != 0)
  {
    tlogf("stat after ln failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  if (st_src.st_ino != st_hdst.st_ino || st_src.st_nlink < 2)
  {
    tlogf("hardlink verification failed: src ino=%lu nlink=%lu dst ino=%lu nlink=%lu",
          (unsigned long)st_src.st_ino, (unsigned long)st_src.st_nlink,
          (unsigned long)st_hdst.st_ino, (unsigned long)st_hdst.st_nlink);
    stop_kafs(mnt, srv);
    return 1;
  }

  char dir_dst[PATH_MAX];
  snprintf(dir_dst, sizeof(dir_dst), "%s/dirdst", mnt);
  if (mkdir(dir_dst, 0755) != 0)
  {
    tlogf("mkdir(dirdst) failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }

  char *ln_dir_args[] = {(char *)kafsctl, "ln", src, dir_dst, NULL};
  rc = run_kafsctl(ln_dir_args);
  if (rc != 0)
  {
    tlogf("kafsctl ln into dir failed: rc=%d", rc);
    stop_kafs(mnt, srv);
    return 1;
  }

  char dir_link[PATH_MAX];
  if (snprintf(dir_link, sizeof(dir_link), "%s/src", dir_dst) >= (int)sizeof(dir_link))
  {
    tlogf("dir link path too long");
    stop_kafs(mnt, srv);
    return 1;
  }
  struct stat st_dir_link;
  if (stat(dir_link, &st_dir_link) != 0)
  {
    tlogf("stat dir link failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  if (st_dir_link.st_ino != st_src.st_ino)
  {
    tlogf("dir hardlink verification failed");
    stop_kafs(mnt, srv);
    return 1;
  }

  char forced_dst[PATH_MAX];
  snprintf(forced_dst, sizeof(forced_dst), "%s/forced", mnt);
  fd = open(forced_dst, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0)
  {
    tlogf("open(forced_dst) failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);

  char *ln_force_args[] = {(char *)kafsctl, "ln", "-f", src, forced_dst, NULL};
  rc = run_kafsctl(ln_force_args);
  if (rc != 0)
  {
    tlogf("kafsctl ln -f failed: rc=%d", rc);
    stop_kafs(mnt, srv);
    return 1;
  }

  struct stat st_forced;
  if (stat(forced_dst, &st_forced) != 0)
  {
    tlogf("stat forced link failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  if (st_forced.st_ino != st_src.st_ino)
  {
    tlogf("forced hardlink verification failed");
    stop_kafs(mnt, srv);
    return 1;
  }

  char cpdst[PATH_MAX];
  snprintf(cpdst, sizeof(cpdst), "%s/copy", mnt);
  char *cp_args[] = {(char *)kafsctl, "cp", src, cpdst, NULL};
  rc = run_kafsctl(cp_args);
  if (rc != 0)
  {
    tlogf("kafsctl cp failed: rc=%d", rc);
    stop_kafs(mnt, srv);
    return 1;
  }

  struct stat st_cpdst;
  if (stat(cpdst, &st_cpdst) != 0)
  {
    tlogf("stat after cp failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  if (st_cpdst.st_ino == st_src.st_ino || st_cpdst.st_size != 5)
  {
    tlogf("copy destination verification failed");
    stop_kafs(mnt, srv);
    return 1;
  }

  char copybuf[6] = {0};
  int nread = read_whole_file(cpdst, copybuf, 5);
  if (nread != 5)
  {
    tlogf("read copied file failed: %s", nread < 0 ? strerror(-nread) : "short read");
    stop_kafs(mnt, srv);
    return 1;
  }
  if (memcmp(copybuf, "hello", 5) != 0)
  {
    tlogf("copied file content verification failed");
    stop_kafs(mnt, srv);
    return 1;
  }

  char *cp_same_inode_args[] = {(char *)kafsctl, "cp", src, hdst, NULL};
  rc = run_kafsctl(cp_same_inode_args);
  if (rc == 0)
  {
    tlogf("same-inode cp unexpectedly succeeded");
    stop_kafs(mnt, srv);
    return 1;
  }

  char linkbuf_verify[6] = {0};
  nread = read_whole_file(src, linkbuf_verify, 5);
  if (nread != 5)
  {
    tlogf("read src after failed cp failed: %s", nread < 0 ? strerror(-nread) : "short read");
    stop_kafs(mnt, srv);
    return 1;
  }
  if (memcmp(linkbuf_verify, "hello", 5) != 0)
  {
    tlogf("same-inode cp modified source unexpectedly");
    stop_kafs(mnt, srv);
    return 1;
  }

  if (stat(src, &st_src) != 0 || stat(hdst, &st_hdst) != 0)
  {
    tlogf("stat after failed same-inode cp failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  if (st_src.st_ino != st_hdst.st_ino || st_src.st_size != 5 || st_hdst.st_size != 5)
  {
    tlogf("same-inode cp changed hardlink metadata unexpectedly");
    stop_kafs(mnt, srv);
    return 1;
  }

  char *ln_symlink_args[] = {(char *)kafsctl, "ln", "-s", "literal-target", sdst, NULL};
  rc = run_kafsctl(ln_symlink_args);
  if (rc != 0)
  {
    tlogf("kafsctl ln -s failed: rc=%d", rc);
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

  char sym_dir[PATH_MAX];
  snprintf(sym_dir, sizeof(sym_dir), "%s/symdir", mnt);
  if (mkdir(sym_dir, 0755) != 0)
  {
    tlogf("mkdir(symdir) failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }

  char *ln_symlink_dir_args[] = {(char *)kafsctl, "ln", "-s", "leaf-target", sym_dir, NULL};
  rc = run_kafsctl(ln_symlink_dir_args);
  if (rc != 0)
  {
    tlogf("kafsctl ln -s into dir failed: rc=%d", rc);
    stop_kafs(mnt, srv);
    return 1;
  }

  char sym_dir_link[PATH_MAX];
  if (snprintf(sym_dir_link, sizeof(sym_dir_link), "%s/leaf-target", sym_dir) >=
      (int)sizeof(sym_dir_link))
  {
    tlogf("sym dir link path too long");
    stop_kafs(mnt, srv);
    return 1;
  }
  n = readlink(sym_dir_link, linkbuf, sizeof(linkbuf) - 1);
  if (n < 0)
  {
    tlogf("readlink dir symlink failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  linkbuf[n] = '\0';
  if (strcmp(linkbuf, "leaf-target") != 0)
  {
    tlogf("dir symlink target verification failed: %s", linkbuf);
    stop_kafs(mnt, srv);
    return 1;
  }

  char sym_forced[PATH_MAX];
  snprintf(sym_forced, sizeof(sym_forced), "%s/sym-force", mnt);
  fd = open(sym_forced, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0)
  {
    tlogf("open(sym_forced) failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);

  char *ln_symlink_force_args[] = {(char *)kafsctl, "ln",       "-s", "-f",
                                   "forced-target", sym_forced, NULL};
  rc = run_kafsctl(ln_symlink_force_args);
  if (rc != 0)
  {
    tlogf("kafsctl ln -s -f failed: rc=%d", rc);
    stop_kafs(mnt, srv);
    return 1;
  }

  n = readlink(sym_forced, linkbuf, sizeof(linkbuf) - 1);
  if (n < 0)
  {
    tlogf("readlink forced symlink failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  linkbuf[n] = '\0';
  if (strcmp(linkbuf, "forced-target") != 0)
  {
    tlogf("forced symlink target verification failed: %s", linkbuf);
    stop_kafs(mnt, srv);
    return 1;
  }

  char sdst_alias[PATH_MAX];
  snprintf(sdst_alias, sizeof(sdst_alias), "%s/sym-alias", mnt);
  char *symlink_args[] = {(char *)kafsctl, "symlink", "alias-target", sdst_alias, NULL};
  rc = run_kafsctl(symlink_args);
  if (rc != 0)
  {
    tlogf("kafsctl symlink alias failed: rc=%d", rc);
    stop_kafs(mnt, srv);
    return 1;
  }

  n = readlink(sdst_alias, linkbuf, sizeof(linkbuf) - 1);
  if (n < 0)
  {
    tlogf("readlink alias failed: %s", strerror(errno));
    stop_kafs(mnt, srv);
    return 1;
  }
  linkbuf[n] = '\0';
  if (strcmp(linkbuf, "alias-target") != 0)
  {
    tlogf("symlink alias target verification failed: %s", linkbuf);
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
