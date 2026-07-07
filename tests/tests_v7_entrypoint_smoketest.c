#include "test_utils.h"

#include "kafs_descriptor_layout.h"
#include "kafs_offline_summary.h"
#include "kafs_superblock.h"

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

static int run_cmd_capture(char *const argv[], int expected_exit, char *out, size_t out_sz)
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
    dup2(pipefd[1], STDOUT_FILENO);
    dup2(pipefd[1], STDERR_FILENO);
    close(pipefd[1]);
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

static int expect_contains(const char *label, const char *text, const char *needle)
{
  if (strstr(text, needle))
    return 0;
  tlogf("%s missing expected text: %s\noutput:\n%s", label, needle, text);
  return 1;
}

static int expect_not_contains(const char *label, const char *text, const char *needle)
{
  if (!strstr(text, needle))
    return 0;
  tlogf("%s contained unexpected text: %s\noutput:\n%s", label, needle, text);
  return 1;
}

static int check_v7_descriptor_direct(const char *img)
{
  int fd = open(img, O_RDONLY);
  if (fd < 0)
    return -errno;

  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  int rc = kafs_pread_all(fd, &sb, sizeof(sb), 0);
  if (rc == 0)
    rc = kafs_offline_detect_file_size(fd, &file_size);
  if (rc == 0 && kafs_sb_format_version_get(&sb) != KAFS_FORMAT_VERSION_V7)
    rc = -EINVAL;

  kafs_descriptor_layout_report_t report;
  if (rc == 0)
    rc = kafs_descriptor_discover_layout(fd, &sb, file_size, &report);
  close(fd);
  if (rc != 0)
    return rc;
  if (!report.anchor_valid || !report.selected_found || report.replica_count != 3u ||
      report.group_count != 1u || report.shard_count != 12u || report.descriptor_bytes == 0u)
    return -EINVAL;
  return 0;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("v7-entrypoint") != 0)
  {
    tlogf("failed to enter tmpdir");
    return 1;
  }
  if (mkdir("mnt", 0755) != 0)
  {
    tlogf("mkdir mnt failed");
    return 1;
  }

  char out[16384];
  char *help_argv[] = {(char *)kafs_test_kafs_v7_bin(), (char *)"--help", NULL};
  if (run_cmd_capture(help_argv, 0, out, sizeof(out)) != 0)
  {
    tlogf("kafs-v7 --help failed: %s", out);
    return 1;
  }
  if (expect_contains("kafs-v7 help", out, "format v7 runtime entrypoint") ||
      expect_contains("kafs-v7 help", out, "Format v7 image path") ||
      expect_contains("kafs-v7 help", out, "controlled write contract"))
    return 1;

  const char *img = "v7.img";
  char *mkfs_argv[] = {(char *)kafs_test_mkfs_bin(), (char *)img, (char *)"--format-version",
                       (char *)"7", (char *)"--size-bytes", (char *)"64M", (char *)"--yes",
                       NULL};
  if (run_cmd_capture(mkfs_argv, 0, out, sizeof(out)) != 0)
  {
    tlogf("mkfs v7 failed: %s", out);
    return 1;
  }
  if (check_v7_descriptor_direct(img) != 0)
  {
    tlogf("direct v7 descriptor discovery failed");
    return 1;
  }

  char *dump_argv[] = {(char *)kafs_test_kafsdump_bin(), (char *)"--json", (char *)img, NULL};
  if (run_cmd_capture(dump_argv, 0, out, sizeof(out)) != 0)
  {
    tlogf("kafsdump --json v7 failed: %s", out);
    return 1;
  }
  if (expect_contains("v7 kafsdump", out, "\"format_version\": 7") ||
      expect_contains("v7 kafsdump", out, "\"layout_descriptor\"") ||
      expect_contains("v7 kafsdump", out, "\"status\": \"ok\"") ||
      expect_contains("v7 kafsdump", out, "\"replica_count\": 3") ||
      expect_not_contains("v7 kafsdump", out, "\"v6_layout_descriptor\""))
    return 1;

  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  if (run_cmd_capture(fsck_argv, 0, out, sizeof(out)) != 0)
  {
    tlogf("fsck v7 descriptor failed: %s", out);
    return 1;
  }
  if (expect_contains("v7 fsck", out, "format v7 fsck policy") ||
      expect_contains("v7 fsck", out, "layout descriptor:") ||
      expect_contains("v7 fsck", out, "status=selected"))
    return 1;

  char *mount_argv[] = {(char *)kafs_test_kafs_bin(), (char *)img, (char *)"mnt", NULL};
  if (run_cmd_capture(mount_argv, 2, out, sizeof(out)) != 0)
  {
    tlogf("production kafs did not reject v7 as expected: %s", out);
    return 1;
  }
  if (expect_contains("v7 production reject", out,
                      "unsupported format version: v7 runtime admission is owned by kafs-v7") ||
      expect_contains("v7 production reject", out, "kafs-v7 --inspection-mount") ||
      expect_contains("v7 production reject", out, "kafs-v7 --controlled-write-mount") ||
      expect_not_contains("v7 production reject", out, "kafs-v6 --inspection-mount"))
    return 1;

  const char *img_v6 = "v6.img";
  char *mkfs_v6_argv[] = {(char *)kafs_test_mkfs_bin(), (char *)img_v6,
                          (char *)"--format-version", (char *)"6", (char *)"--size-bytes",
                          (char *)"64M", (char *)"--yes", NULL};
  if (run_cmd_capture(mkfs_v6_argv, 0, out, sizeof(out)) != 0)
  {
    tlogf("mkfs v6 fixture failed: %s", out);
    return 1;
  }
  char *kafsv7_v6_argv[] = {(char *)kafs_test_kafs_v7_bin(), (char *)"--image", (char *)img_v6,
                            (char *)"--inspection-mount", (char *)"mnt", (char *)"-o",
                            (char *)"ro", NULL};
  if (run_cmd_capture(kafsv7_v6_argv, 2, out, sizeof(out)) != 0)
  {
    tlogf("kafs-v7 did not reject v6 image as expected: %s", out);
    return 1;
  }
  if (expect_contains("kafs-v7 rejects v6", out, "expected format v7") ||
      expect_contains("kafs-v7 rejects v6", out, "image is format v6"))
    return 1;

  return 0;
}
