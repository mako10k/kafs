#include "test_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
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

static ssize_t pread_full(int fd, void *buf, size_t len, off_t offset)
{
  size_t total = 0;

  while (total < len)
  {
    ssize_t nread = pread(fd, (char *)buf + total, len - total, offset + (off_t)total);
    if (nread < 0)
    {
      if (errno == EINTR)
        continue;
      return -1;
    }
    if (nread == 0)
      break;
    total += (size_t)nread;
  }

  return (ssize_t)total;
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
  for (;;)
  {
    char discard[1024];
    char *dst = discard;
    size_t cap = sizeof(discard);
    if (stdout_buf && used + 1 < stdout_buf_sz)
    {
      dst = stdout_buf + used;
      cap = stdout_buf_sz - used - 1;
    }
    ssize_t n = read(pipefd[0], dst, cap);
    if (n < 0)
    {
      if (errno == EINTR)
        continue;
      int saved = errno;

      close(pipefd[0]);
      (void)waitpid(pid, NULL, 0);
      return -saved;
    }
    if (n == 0)
      break;
    if (stdout_buf && used + 1 < stdout_buf_sz)
    {
      size_t avail = stdout_buf_sz - used - 1;
      size_t keep = (size_t)n < avail ? (size_t)n : avail;
      used += keep;
    }
  }
  if (stdout_buf && stdout_buf_sz > 0)
    stdout_buf[used] = '\0';
  close(pipefd[0]);

  int status = 0;
  if (waitpid(pid, &status, 0) < 0)
    return -errno;
  if (WIFEXITED(status))
    return WEXITSTATUS(status);
  return 255;
}

static int mkfs_v5_image(const char *img)
{
  const char *mkfs = kafs_test_mkfs_bin();

  if (!mkfs)
    return -ENOENT;

  char *argv[] = {
      (char *)mkfs,
      (char *)"--format-version",
      (char *)"5",
      (char *)img,
      (char *)"-s",
      (char *)"64M",
      NULL,
  };
  return run_cmd(argv);
}

static int assert_file_readback(const char *path, const char *expected, size_t expected_len,
                                const char *label)
{
  char actual[8192];
  int fd = open(path, O_RDONLY);

  if (fd < 0)
  {
    tlogf("open %s failed: %s", label, strerror(errno));
    return -errno;
  }

  ssize_t nread = pread_full(fd, actual, expected_len, 0);
  close(fd);
  if (nread != (ssize_t)expected_len || memcmp(actual, expected, expected_len) != 0)
  {
    tlogf("readback mismatch for %s nread=%zd expected=%zu", label, nread, expected_len);
    return -EIO;
  }
  return 0;
}

static int inspect_offline_tool_output(const char *img)
{
  char info_stdout[4096];
  char dump_stdout[8192];
  char dump_json_stdout[8192];
  const char *info = kafs_test_kafs_info_bin();
  const char *dump = kafs_test_kafsdump_bin();
  int rc;

  if (!info || !dump)
    return -ENOENT;

  char *info_argv[] = {(char *)info, (char *)img, NULL};
  rc = run_cmd_capture_stdout(info_argv, info_stdout, sizeof(info_stdout));
  if (rc != 0)
    return -EIO;
  if (!strstr(info_stdout, "tail layouts: regular=2 tail_only=1 mixed_full_tail=1") ||
      !strstr(info_stdout, "tail usage: live_slots=2 live_bytes=500"))
    return -EPROTO;

  char *dump_argv[] = {(char *)dump, (char *)img, NULL};
  rc = run_cmd_capture_stdout(dump_argv, dump_stdout, sizeof(dump_stdout));
  if (rc != 0)
    return -EIO;
  if (!strstr(dump_stdout, "live_bytes: 500") ||
      !strstr(dump_stdout, "tail_only_regular: 1") ||
      !strstr(dump_stdout, "mixed_full_tail_regular: 1"))
    return -EPROTO;

  char *dump_json_argv[] = {(char *)dump, (char *)"--json", (char *)img, NULL};
  rc = run_cmd_capture_stdout(dump_json_argv, dump_json_stdout, sizeof(dump_json_stdout));
  if (rc != 0)
    return -EIO;
  if (!strstr(dump_json_stdout, "\"live_bytes\": 500") ||
      !strstr(dump_json_stdout, "\"tail_only_regular\": 1") ||
      !strstr(dump_json_stdout, "\"mixed_full_tail_regular\": 1") ||
      !strstr(dump_json_stdout, "\"classes\": ["))
    return -EPROTO;

  return 0;
}

static const kafs_test_mount_options_t k_mount_options = {
    .debug = "1",
    .log_path = "v5_default_promotion.log",
    .timeout_ms = 5000,
};

int main(void)
{
  enum
  {
    k_blksize = 4096,
    k_small_size = 300,
    k_mixed_seed_size = 300,
    k_initial_size = k_blksize + 300,
    k_final_size = k_blksize + 200,
    k_tail_patch_off = k_blksize + 40,
    k_tail_patch_len = 96,
    k_append_len = 64,
  };

  if (kafs_test_enter_tmpdir("v5_default_promotion") != 0)
    return 77;
  if (access("/dev/fuse", R_OK | W_OK) != 0)
    return 77;

  const char *img = "v5-default-promotion.img";
  const char *mnt = "mnt-v5-default";
  if (mkfs_v5_image(img) != 0)
  {
    tlogf("mkfs v5 image failed");
    return 77;
  }

  pid_t srv = kafs_test_start_kafs(img, mnt, &k_mount_options);
  if (srv <= 0)
  {
    tlogf("initial v5 scaffold mount failed");
    return 77;
  }

  char small_payload[k_small_size];
  char mixed_payload[k_initial_size + k_append_len];
  char patch[k_tail_patch_len];
  char path[PATH_MAX];
  for (size_t i = 0; i < sizeof(small_payload); ++i)
    small_payload[i] = (char)('a' + (i % 26));
  for (size_t i = 0; i < sizeof(mixed_payload); ++i)
    mixed_payload[i] = (char)('a' + (i % 26));
  for (size_t i = 0; i < sizeof(patch); ++i)
    patch[i] = (char)('Z' - (i % 26));

  snprintf(path, sizeof(path), "%s/small", mnt);
  int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0)
  {
    tlogf("create small failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (write(fd, small_payload, sizeof(small_payload)) != (ssize_t)sizeof(small_payload))
  {
    tlogf("write small failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);

  snprintf(path, sizeof(path), "%s/mixed", mnt);
  fd = open(path, O_CREAT | O_RDWR | O_TRUNC, 0644);
  if (fd < 0)
  {
    tlogf("create mixed failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (write(fd, mixed_payload, k_mixed_seed_size) != k_mixed_seed_size)
  {
    tlogf("initial mixed seed write failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (pwrite(fd, mixed_payload + k_mixed_seed_size, k_initial_size - k_mixed_seed_size,
             k_mixed_seed_size) != k_initial_size - k_mixed_seed_size)
  {
    tlogf("mixed promotion write failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (pwrite(fd, patch, sizeof(patch), k_tail_patch_off) != (ssize_t)sizeof(patch))
  {
    tlogf("mixed tail overwrite failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  memcpy(mixed_payload + k_tail_patch_off, patch, sizeof(patch));
  if (pwrite(fd, mixed_payload + k_initial_size, k_append_len, k_initial_size) != k_append_len)
  {
    tlogf("mixed append failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (ftruncate(fd, k_final_size) != 0)
  {
    tlogf("mixed truncate failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);

  snprintf(path, sizeof(path), "%s/small", mnt);
  if (assert_file_readback(path, small_payload, sizeof(small_payload), "mounted small") != 0)
  {
    kafs_test_dump_log(k_mount_options.log_path, "mounted small readback failed");
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  snprintf(path, sizeof(path), "%s/mixed", mnt);
  if (assert_file_readback(path, mixed_payload, k_final_size, "mounted mixed") != 0)
  {
    kafs_test_dump_log(k_mount_options.log_path, "mounted mixed readback failed");
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  kafs_test_stop_kafs(mnt, srv);

  const char *remount = "mnt-v5-default-remount";
  pid_t remount_srv = kafs_test_start_kafs(img, remount, &k_mount_options);
  if (remount_srv <= 0)
  {
    kafs_test_dump_log(k_mount_options.log_path, "default-promotion remount should succeed");
    tlogf("default-promotion remount failed");
    return 1;
  }

  snprintf(path, sizeof(path), "%s/small", remount);
  if (assert_file_readback(path, small_payload, sizeof(small_payload), "remounted small") != 0)
  {
    kafs_test_stop_kafs(remount, remount_srv);
    return 1;
  }
  snprintf(path, sizeof(path), "%s/mixed", remount);
  if (assert_file_readback(path, mixed_payload, k_final_size, "remounted mixed") != 0)
  {
    kafs_test_stop_kafs(remount, remount_srv);
    return 1;
  }

  kafs_test_stop_kafs(remount, remount_srv);

  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  int rc = run_cmd(fsck_argv);
  if (rc != 0)
  {
    tlogf("fsck failed after v5 default-promotion flow");
    return 1;
  }

  rc = inspect_offline_tool_output(img);
  if (rc != 0)
  {
    tlogf("offline tool inspection failed rc=%d", rc);
    return 1;
  }

  tlogf("v5_default_promotion_smoketest OK");
  return 0;
}