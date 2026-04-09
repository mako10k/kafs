#include "test_utils.h"

#include "kafs_superblock.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
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

static ssize_t read_full(int fd, void *buf, size_t len)
{
  size_t total = 0;

  while (total < len)
  {
    ssize_t nread = read(fd, (char *)buf + total, len - total);
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

static int mkfs_v5_image(const char *img)
{
  const char *mkfs = kafs_test_mkfs_bin();

  if (!mkfs)
    return -ENOENT;

  char *argv[] = {
      (char *)mkfs,
      (char *)"--yes",
      (char *)"--format-version",
      (char *)"5",
      (char *)img,
      (char *)"-s",
      (char *)"64M",
      NULL,
  };
  return run_cmd(argv);
}

static int corrupt_v5_tailmeta_header(const char *img)
{
  int fd = open(img, O_RDWR);
  if (fd < 0)
    return -errno;

  kafs_ssuperblock_t sb;
  ssize_t nread = pread(fd, &sb, sizeof(sb), 0);
  if (nread != (ssize_t)sizeof(sb))
  {
    close(fd);
    return -EIO;
  }

  uint64_t tailmeta_off = kafs_sb_tailmeta_offset_get(&sb);
  uint32_t bad_magic = 0;
  ssize_t nwritten = pwrite(fd, &bad_magic, sizeof(bad_magic), (off_t)tailmeta_off);
  close(fd);
  return (nwritten == (ssize_t)sizeof(bad_magic)) ? 0 : -EIO;
}

static int clear_v5_tailmeta_region(const char *img)
{
  int fd = open(img, O_RDWR);
  if (fd < 0)
    return -errno;

  kafs_ssuperblock_t sb;
  ssize_t nread = pread(fd, &sb, sizeof(sb), 0);
  if (nread != (ssize_t)sizeof(sb))
  {
    close(fd);
    return -EIO;
  }

  kafs_sb_feature_flags_set(&sb, kafs_sb_feature_flags_get(&sb) & ~KAFS_FEATURE_TAIL_META_REGION);
  kafs_sb_tailmeta_offset_set(&sb, 0);
  kafs_sb_tailmeta_size_set(&sb, 0);

  ssize_t nwritten = pwrite(fd, &sb, sizeof(sb), 0);
  close(fd);
  return (nwritten == (ssize_t)sizeof(sb)) ? 0 : -EIO;
}

static const kafs_test_mount_options_t k_mount_options = {
    .debug = "1",
    .log_path = "v5_mount.log",
    .timeout_ms = 5000,
};

int main(void)
{
  if (kafs_test_enter_tmpdir("v5_mount_smoketest") != 0)
    return 77;
  if (access("/dev/fuse", R_OK | W_OK) != 0)
    return 77;

  const char *img = "tailmeta-v5.img";
  const char *mnt = "mnt-v5";

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

  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/hello.txt", mnt);
  int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0)
  {
    tlogf("create hello.txt failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  const char payload[] = "v5";
  if (write(fd, payload, sizeof(payload) - 1) != (ssize_t)(sizeof(payload) - 1))
  {
    tlogf("write hello.txt failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  close(fd);

  struct stat st = {0};
  if (stat(path, &st) != 0)
  {
    tlogf("stat hello.txt failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (st.st_size != (off_t)(sizeof(payload) - 1))
  {
    tlogf("unexpected hello.txt size: %ld", (long)st.st_size);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  kafs_test_stop_kafs(mnt, srv);

  const char *remount = "mnt-v5-remount";
  pid_t remount_srv = kafs_test_start_kafs(img, remount, &k_mount_options);
  if (remount_srv <= 0)
  {
    kafs_test_dump_log(k_mount_options.log_path, "non-empty v5 remount should succeed");
    tlogf("non-empty v5 remount failed");
    return 1;
  }

  snprintf(path, sizeof(path), "%s/hello.txt", remount);
  memset(&st, 0, sizeof(st));
  if (stat(path, &st) != 0)
  {
    tlogf("stat hello.txt after remount failed: %s", strerror(errno));
    kafs_test_stop_kafs(remount, remount_srv);
    return 1;
  }
  if (st.st_size != (off_t)(sizeof(payload) - 1))
  {
    tlogf("unexpected hello.txt size after remount: %ld", (long)st.st_size);
    kafs_test_stop_kafs(remount, remount_srv);
    return 1;
  }

  char verify[sizeof(payload)];
  fd = open(path, O_RDONLY);
  if (fd < 0)
  {
    tlogf("open hello.txt after remount failed: %s", strerror(errno));
    kafs_test_stop_kafs(remount, remount_srv);
    return 1;
  }
  ssize_t nread = read_full(fd, verify, sizeof(verify) - 1);
  close(fd);
  if (nread < 0)
  {
    tlogf("read hello.txt after remount failed: %s", strerror(errno));
    kafs_test_stop_kafs(remount, remount_srv);
    return 1;
  }
  if (nread != (ssize_t)(sizeof(payload) - 1))
  {
    tlogf("hello.txt short read after remount: expected %zu bytes, got %zd",
          sizeof(payload) - 1, nread);
    kafs_test_stop_kafs(remount, remount_srv);
    return 1;
  }
  if (memcmp(verify, payload, sizeof(payload) - 1) != 0)
  {
    tlogf("hello.txt readback mismatch after remount");
    kafs_test_stop_kafs(remount, remount_srv);
    return 1;
  }

  kafs_test_stop_kafs(remount, remount_srv);

  if (corrupt_v5_tailmeta_header(img) != 0)
  {
    tlogf("corrupt v5 tailmeta header failed");
    return 1;
  }

  const char *bad_mnt = "mnt-v5-corrupt";
  pid_t bad_srv = kafs_test_start_kafs(img, bad_mnt, &k_mount_options);
  if (bad_srv > 0)
  {
    kafs_test_dump_log(k_mount_options.log_path, "corrupt v5 image mount should fail");
    kafs_test_stop_kafs(bad_mnt, bad_srv);
    tlogf("corrupt v5 image mount unexpectedly succeeded");
    return 1;
  }

  if (mkfs_v5_image(img) != 0)
  {
    tlogf("mkfs v5 image for missing tailmeta case failed");
    return 77;
  }
  if (clear_v5_tailmeta_region(img) != 0)
  {
    tlogf("clear v5 tailmeta region failed");
    return 1;
  }

  const char *missing_mnt = "mnt-v5-missing-tailmeta";
  pid_t missing_srv = kafs_test_start_kafs(img, missing_mnt, &k_mount_options);
  if (missing_srv > 0)
  {
    kafs_test_dump_log(k_mount_options.log_path, "v5 image without tailmeta should fail");
    kafs_test_stop_kafs(missing_mnt, missing_srv);
    tlogf("v5 image without tailmeta unexpectedly succeeded");
    return 1;
  }

  tlogf("v5_mount_smoketest OK");
  return 0;
}
