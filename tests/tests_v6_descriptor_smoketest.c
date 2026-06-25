#include "test_utils.h"

#include "kafs_offline_summary.h"
#include "kafs_superblock.h"
#include "kafs_v6_layout.h"

#include <errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
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

static int file_contains(const char *path, const char *needle)
{
  FILE *fp = fopen(path, "r");
  if (!fp)
    return 0;

  char line[512];
  int found = 0;
  while (fgets(line, sizeof(line), fp))
  {
    if (strstr(line, needle))
    {
      found = 1;
      break;
    }
  }
  fclose(fp);
  return found;
}

static int check_v6_readonly_mount_smoke(const char *img)
{
  if (access("/dev/fuse", R_OK | W_OK) != 0)
  {
    tlogf("skip v6 readonly mount smoke: /dev/fuse unavailable");
    return 0;
  }

  const char *mnt = "mnt-readonly-smoke";
  const char *log_path = "v6-readonly-smoke.log";
  kafs_test_mount_options_t options = {
      .debug = "1",
      .log_path = log_path,
      .extra_options = "ro,no_writeback_cache",
      .timeout_ms = 10000,
  };

  setenv("KAFS_V6_READONLY_SMOKE", "1", 1);
  pid_t srv = kafs_test_start_kafs(img, mnt, &options);
  unsetenv("KAFS_V6_READONLY_SMOKE");
  if (srv <= 0)
  {
    kafs_test_dump_log(log_path, "v6 readonly smoke mount failed");
    return 77;
  }

  int rc = 0;
  struct stat st = {0};
  if (stat(mnt, &st) != 0 || !S_ISDIR(st.st_mode))
  {
    tlogf("v6 readonly smoke root stat failed: %s", strerror(errno));
    rc = 1;
    goto out_stop;
  }

  struct statvfs sv = {0};
  if (statvfs(mnt, &sv) != 0 || sv.f_blocks == 0)
  {
    tlogf("v6 readonly smoke statvfs failed: %s", strerror(errno));
    rc = 1;
    goto out_stop;
  }

  DIR *dir = opendir(mnt);
  if (!dir)
  {
    tlogf("v6 readonly smoke opendir failed: %s", strerror(errno));
    rc = 1;
    goto out_stop;
  }
  int saw_dot = 0;
  errno = 0;
  for (struct dirent *de = readdir(dir); de; de = readdir(dir))
  {
    if (strcmp(de->d_name, ".") == 0)
      saw_dot = 1;
  }
  int saved_errno = errno;
  closedir(dir);
  if (saved_errno != 0 || !saw_dot)
  {
    tlogf("v6 readonly smoke readdir failed: errno=%s saw_dot=%d", strerror(saved_errno),
          saw_dot);
    rc = 1;
    goto out_stop;
  }

  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/new.txt", mnt);
  int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd >= 0)
  {
    close(fd);
    tlogf("v6 readonly smoke create unexpectedly succeeded");
    rc = 1;
    goto out_stop;
  }
  if (errno != EROFS)
  {
    tlogf("v6 readonly smoke create errno=%s (expected EROFS)", strerror(errno));
    rc = 1;
    goto out_stop;
  }

  snprintf(path, sizeof(path), "%s/subdir", mnt);
  if (mkdir(path, 0755) == 0)
  {
    tlogf("v6 readonly smoke mkdir unexpectedly succeeded");
    rc = 1;
    goto out_stop;
  }
  if (errno != EROFS)
  {
    tlogf("v6 readonly smoke mkdir errno=%s (expected EROFS)", strerror(errno));
    rc = 1;
    goto out_stop;
  }

out_stop:
  kafs_test_stop_kafs(mnt, srv);
  if (rc == 0 && !file_contains(log_path, "format v6 readonly smoke"))
  {
    tlogf("v6 readonly smoke log missing admission message");
    rc = 1;
  }
  return rc;
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

  int readonly_rc = check_v6_readonly_mount_smoke(img);
  if (readonly_rc == 77)
    return 77;
  if (readonly_rc != 0)
    return 1;

  return 0;
}
