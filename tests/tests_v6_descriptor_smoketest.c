#include "test_utils.h"

#include "kafs_offline_summary.h"
#include "kafs_superblock.h"
#include "kafs_v6_layout.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static void tlogf(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  fputc('\n', stderr);
  va_end(ap);
}

static int run_cmd_capture_env(char *const argv[], int expected_exit, const char *env_name,
                               const char *env_value, char *out, size_t out_sz)
{
  int pipefd[2];
  if (pipe(pipefd) != 0)
    return -errno;

  pid_t pid = fork();
  if (pid < 0)
  {
    close(pipefd[0]);
    close(pipefd[1]);
    return -errno;
  }
  if (pid == 0)
  {
    close(pipefd[0]);
    dup2(pipefd[1], STDOUT_FILENO);
    dup2(pipefd[1], STDERR_FILENO);
    close(pipefd[1]);
    if (env_name && env_value)
      setenv(env_name, env_value, 1);
    execvp(argv[0], argv);
    _exit(127);
  }

  close(pipefd[1]);
  size_t used = 0;
  for (;;)
  {
    char buf[512];
    ssize_t n = read(pipefd[0], buf, sizeof(buf));
    if (n < 0)
    {
      if (errno == EINTR)
        continue;
      break;
    }
    if (n == 0)
      break;
    if (out && out_sz > 0 && used < out_sz - 1u)
    {
      size_t copy = (size_t)n;
      if (copy > out_sz - 1u - used)
        copy = out_sz - 1u - used;
      memcpy(out + used, buf, copy);
      used += copy;
    }
  }
  close(pipefd[0]);
  if (out && out_sz > 0)
    out[used] = '\0';

  int st = 0;
  if (waitpid(pid, &st, 0) != pid)
    return -errno;
  if (!WIFEXITED(st))
    return -1;
  return (WEXITSTATUS(st) == expected_exit) ? 0 : -1;
}

static int run_cmd_capture(char *const argv[], int expected_exit, char *out, size_t out_sz)
{
  return run_cmd_capture_env(argv, expected_exit, NULL, NULL, out, out_sz);
}

static int check_v6_descriptor_direct(const char *img)
{
  int fd = open(img, O_RDONLY);
  if (fd < 0)
    return -errno;

  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  int rc = kafs_pread_all(fd, &sb, sizeof(sb), 0);
  if (rc == 0)
    rc = kafs_offline_detect_file_size(fd, &file_size);
  if (rc == 0 && kafs_sb_format_version_get(&sb) != KAFS_FORMAT_VERSION_V6)
    rc = -EINVAL;

  kafs_v6_layout_report_t report;
  if (rc == 0)
    rc = kafs_v6_discover_layout(fd, &sb, file_size, &report);
  close(fd);
  if (rc != 0)
    return rc;
  if (!report.anchor_valid || !report.selected_found || report.replica_count != 3u ||
      report.group_count != 1u || report.shard_count != 12u || report.descriptor_bytes == 0u)
    return -EINVAL;
  if (report.replicas[0].status != KAFS_V6_REPLICA_STATUS_SELECTED ||
      report.replicas[1].status != KAFS_V6_REPLICA_STATUS_VALID ||
      report.replicas[2].status != KAFS_V6_REPLICA_STATUS_VALID)
    return -EINVAL;
  return 0;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("v6_descriptor") != 0)
    return 77;

  const char *img = "v6-desc.img";
  if (mkdir("mnt", 0755) != 0)
  {
    tlogf("mkdir mnt failed");
    return 1;
  }
  if (mkdir("mnt-handoff", 0755) != 0)
  {
    tlogf("mkdir mnt-handoff failed");
    return 1;
  }

  char *mkfs_argv[] = {(char *)kafs_test_mkfs_bin(), (char *)img, (char *)"--format-version",
                       (char *)"6", (char *)"--size-bytes", (char *)"64M", (char *)"--yes",
                       NULL};
  char out[8192];
  if (run_cmd_capture(mkfs_argv, 0, out, sizeof(out)) != 0)
  {
    tlogf("mkfs v6 failed: %s", out);
    return 1;
  }

  if (check_v6_descriptor_direct(img) != 0)
  {
    tlogf("direct v6 descriptor discovery failed");
    return 1;
  }

  char *dump_argv[] = {(char *)kafs_test_kafsdump_bin(), (char *)"--json", (char *)img, NULL};
  if (run_cmd_capture(dump_argv, 0, out, sizeof(out)) != 0)
  {
    tlogf("kafsdump --json v6 failed: %s", out);
    return 1;
  }
  if (!strstr(out, "\"v6_layout_descriptor\"") || !strstr(out, "\"status\": \"ok\"") ||
      !strstr(out, "\"replica_count\": 3") || !strstr(out, "\"selected\": true"))
  {
    tlogf("kafsdump JSON missing v6 descriptor fields: %s", out);
    return 1;
  }

  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  if (run_cmd_capture(fsck_argv, 0, out, sizeof(out)) != 0)
  {
    tlogf("fsck v6 descriptor failed: %s", out);
    return 1;
  }
  if (!strstr(out, "v6 descriptor:") || !strstr(out, "status=selected"))
  {
    tlogf("fsck output missing v6 descriptor status: %s", out);
    return 1;
  }

  char *mount_argv[] = {(char *)kafs_test_kafs_bin(), (char *)img, (char *)"mnt", NULL};
  if (run_cmd_capture(mount_argv, 2, out, sizeof(out)) != 0)
  {
    tlogf("v6 runtime mount did not fail as expected: %s", out);
    return 1;
  }
  if (!strstr(out, "admission preflight") || !strstr(out, "metadata checks OK") ||
      !strstr(out, "offline-only"))
  {
    tlogf("v6 runtime mount error missing preflight/offline-only guidance: %s", out);
    return 1;
  }

  char *handoff_argv[] = {(char *)kafs_test_kafs_bin(), (char *)img, (char *)"mnt-handoff",
                          NULL};
  if (run_cmd_capture_env(handoff_argv, 2, "KAFS_V6_ADMISSION_HANDOFF", "1", out, sizeof(out)) !=
      0)
  {
    tlogf("v6 runtime handoff did not fail as expected: %s", out);
    return 1;
  }
  if (!strstr(out, "admission handoff") || !strstr(out, "selected descriptor retained") ||
      !strstr(out, "offline-only"))
  {
    tlogf("v6 runtime handoff error missing descriptor/offline-only guidance: %s", out);
    return 1;
  }

  return 0;
}
