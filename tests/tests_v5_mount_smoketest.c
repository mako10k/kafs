#include "test_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

static const char *pick_exe(const char *env_name, const char *const *cands)
{
  const char *envv = getenv(env_name);

  if (envv && *envv)
    return envv;
  for (size_t i = 0; cands[i] != NULL; ++i)
  {
    if (access(cands[i], X_OK) == 0)
      return cands[i];
  }
  return NULL;
}

static const char *pick_mkfs_exe(void)
{
  static const char *const cands[] = {
      "./mkfs.kafs",
      "../src/mkfs.kafs",
      "./src/mkfs.kafs",
      "src/mkfs.kafs",
      "mkfs.kafs",
      NULL,
  };

  return pick_exe("KAFS_TEST_MKFS", cands);
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
  const char *mkfs = pick_mkfs_exe();

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

static int mount_expect_failure(const char *img, const char *mnt, const char *log_path, int timeout_ms)
{
  const char *kafs = kafs_test_kafs_bin();
  pid_t pid;
  int waited = 0;
  const int step_ms = 100;

  if (mkdir(mnt, 0700) != 0 && errno != EEXIST)
    return -errno;

  pid = fork();
  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
    int lfd;

    setenv("KAFS_IMAGE", img, 1);
    lfd = open(log_path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (lfd >= 0)
    {
      dup2(lfd, STDERR_FILENO);
      dup2(lfd, STDOUT_FILENO);
      close(lfd);
    }
    char *args[] = {(char *)kafs, (char *)mnt, (char *)"-f", NULL};
    execvp(args[0], args);
    _exit(127);
  }

  while (waited < timeout_ms)
  {
    int status = 0;
    pid_t rc = waitpid(pid, &status, WNOHANG);

    if (rc == pid)
    {
      if (WIFEXITED(status) && WEXITSTATUS(status) != 0)
        return 0;
      return -1;
    }
    if (rc < 0)
      return -errno;

    struct timespec ts = {0, step_ms * 1000 * 1000};
    nanosleep(&ts, NULL);
    waited += step_ms;
  }

  kill(pid, SIGKILL);
  (void)waitpid(pid, NULL, 0);
  return 1;
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
    return 1;
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
  if (stat(path, &st) != 0 || st.st_size != (off_t)(sizeof(payload) - 1))
  {
    tlogf("unexpected hello.txt size: %ld", (long)st.st_size);
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  kafs_test_stop_kafs(mnt, srv);

  int rc = mount_expect_failure(img, "mnt-v5-remount", "v5_remount.log", 5000);
  if (rc != 0)
  {
    kafs_test_dump_log("v5_remount.log", "non-empty v5 image should be rejected");
    tlogf("non-empty v5 remount unexpectedly succeeded or hung rc=%d", rc);
    return 1;
  }

  tlogf("v5_mount_smoketest OK");
  return 0;
}
