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

static int run_cmd_capture_stdout(char *const argv[], char *stdout_buf, size_t stdout_buf_sz)
{
  int pipefd[2];
  if (pipe(pipefd) != 0)
    return -errno;

  pid_t pid = fork();
  if (pid < 0)
  {
    int saved = errno;
    close(pipefd[0]);
    close(pipefd[1]);
    return -saved;
  }
  if (pid == 0)
  {
    close(pipefd[0]);
    if (dup2(pipefd[1], STDOUT_FILENO) < 0)
      _exit(127);
    close(pipefd[1]);
    execvp(argv[0], argv);
    _exit(127);
  }

  close(pipefd[1]);
  size_t used = 0;
  while (stdout_buf && used + 1u < stdout_buf_sz)
  {
    ssize_t n = read(pipefd[0], stdout_buf + used, stdout_buf_sz - used - 1u);
    if (n < 0)
    {
      int saved = errno;
      close(pipefd[0]);
      (void)waitpid(pid, NULL, 0);
      return -saved;
    }
    if (n == 0)
      break;
    used += (size_t)n;
  }
  close(pipefd[0]);
  if (stdout_buf && stdout_buf_sz > 0)
    stdout_buf[used] = '\0';

  int status = 0;
  if (waitpid(pid, &status, 0) < 0)
    return -errno;
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
    return -EIO;
  return 0;
}

static int json_get_u64(const char *json, const char *key, uint64_t *out)
{
  const char *p = strstr(json, key);
  if (!p || !out)
    return -ENOENT;
  p = strchr(p, ':');
  if (!p)
    return -EINVAL;
  p++;
  while (*p == ' ' || *p == '\t')
    p++;
  char *end = NULL;
  unsigned long long value = strtoull(p, &end, 10);
  if (end == p)
    return -EINVAL;
  *out = (uint64_t)value;
  return 0;
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

  char stats_json[65536];
  const char *kafsctl = kafs_test_kafsctl_bin();
  char *stats_argv[] = {(char *)kafsctl, (char *)"stats", (char *)mnt, (char *)"--json", NULL};
  if (run_cmd_capture_stdout(stats_argv, stats_json, sizeof(stats_json)) != 0)
  {
    tlogf("kafsctl stats --json failed");
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  uint64_t metadata_total = 0;
  if (json_get_u64(stats_json, "\"metadata_write_total\"", &metadata_total) != 0 ||
      metadata_total == 0)
  {
    tlogf("metadata_write_total did not increase");
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (!strstr(stats_json, "\"metadata_write_regions\": [") ||
      !strstr(stats_json, "\"name\": \"inode_table\"") ||
      !strstr(stats_json, "\"name\": \"journal_header\"") ||
      !strstr(stats_json, "\"name\": \"unknown\", \"writes\": 0"))
  {
    tlogf("metadata write region fields missing or unknown writes nonzero");
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  kafs_test_stop_kafs(mnt, srv);
  tlogf("fs_semantics OK");
  return 0;
}
