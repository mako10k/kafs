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

static const char *pick_exe(const char *const cands[])
{
  static char chosen[PATH_MAX];
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
  return cands[0];
}

static const char *pick_kafs_exe(void)
{
  const char *env = getenv("KAFS_TEST_KAFS");
  if (env && *env)
    return env;
  const char *cands[] = {"./kafs", "../src/kafs", "./src/kafs", "src/kafs", "kafs", NULL};
  return pick_exe(cands);
}

static const char *pick_kafsctl_exe(void)
{
  const char *env = getenv("KAFS_TEST_KAFSCTL");
  if (env && *env)
    return env;
  const char *cands[] = {"./kafsctl", "../src/kafsctl", "./src/kafsctl", "src/kafsctl", "kafsctl", NULL};
  return pick_exe(cands);
}

static const char *pick_mkfs_exe(void)
{
  const char *env = getenv("KAFS_TEST_MKFS");
  if (env && *env)
    return env;
  const char *cands[] = {"./mkfs.kafs", "../src/mkfs.kafs", "./src/mkfs.kafs", "src/mkfs.kafs", "mkfs.kafs", NULL};
  return pick_exe(cands);
}

static const char *pick_back_exe(void)
{
  const char *env = getenv("KAFS_TEST_KAFS_BACK");
  if (env && *env)
    return env;
  const char *cands[] = {"./kafs-back", "../src/kafs-back", "./src/kafs-back", "src/kafs-back", "kafs-back", NULL};
  return pick_exe(cands);
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

static int run_cmd_capture(char *const argv[], char *out, size_t out_sz)
{
  if (!out || out_sz == 0)
    return -EINVAL;
  out[0] = '\0';
  int pfd[2];
  if (pipe(pfd) != 0)
    return -errno;
  pid_t pid = fork();
  if (pid < 0)
  {
    close(pfd[0]);
    close(pfd[1]);
    return -errno;
  }
  if (pid == 0)
  {
    dup2(pfd[1], STDOUT_FILENO);
    close(pfd[0]);
    close(pfd[1]);
    execvp(argv[0], argv);
    _exit(127);
  }
  close(pfd[1]);
  size_t used = 0;
  for (;;)
  {
    ssize_t r = read(pfd[0], out + used, out_sz - 1 - used);
    if (r < 0)
    {
      close(pfd[0]);
      return -errno;
    }
    if (r == 0)
      break;
    used += (size_t)r;
    if (used >= out_sz - 1)
      break;
  }
  out[used] = '\0';
  close(pfd[0]);

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

static pid_t start_kafs(const char *img, const char *mnt, const char *uds_path)
{
  const char *kafs_exe = pick_kafs_exe();
  mkdir(mnt, 0700);
  pid_t pid = fork();
  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
    setenv("KAFS_IMAGE", img, 1);
    setenv("KAFS_HOTPLUG_UDS", uds_path, 1);
    char *args[] = {(char *)kafs_exe, (char *)mnt, "-f", NULL};
    execvp(kafs_exe, args);
    _exit(127);
  }
  return pid;
}

static pid_t spawn_kafs_back(const char *img, const char *uds_path)
{
  const char *back_exe = pick_back_exe();
  pid_t pid = fork();
  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
    setenv("KAFS_IMAGE", img, 1);
    setenv("KAFS_HOTPLUG_UDS", uds_path, 1);
    char *args[] = {(char *)back_exe, NULL};
    execvp(back_exe, args);
    _exit(127);
  }
  return pid;
}

static int parse_json_u64(const char *buf, const char *key, uint64_t *out)
{
  if (!buf || !key || !out)
    return -EINVAL;
  const char *p = strstr(buf, key);
  if (!p)
    return -ENOENT;
  p += strlen(key);
  while (*p == ' ' || *p == '\t' || *p == ':')
    ++p;
  if (*p < '0' || *p > '9')
    return -EINVAL;
  char *endp = NULL;
  unsigned long long v = strtoull(p, &endp, 10);
  if (!endp || endp == p)
    return -EINVAL;
  *out = (uint64_t)v;
  return 0;
}

static int hotplug_status_json(const char *mnt, char *out, size_t out_sz)
{
  const char *kafsctl = pick_kafsctl_exe();
  char *args[] = {(char *)kafsctl, "hotplug", "status", (char *)mnt, "--json", NULL};
  return run_cmd_capture(args, out, out_sz);
}

static int wait_for_path(const char *path, int timeout_ms)
{
  const int step_ms = 50;
  int waited = 0;
  while (timeout_ms < 0 || waited <= timeout_ms)
  {
    struct stat st;
    if (stat(path, &st) == 0)
      return 0;
    struct timespec ts = {0, step_ms * 1000 * 1000};
    nanosleep(&ts, NULL);
    waited += step_ms;
  }
  return -ETIMEDOUT;
}

static int wait_hotplug_state(const char *mnt, uint64_t want_state, uint64_t *epoch_out,
                              int timeout_ms)
{
  const int step_ms = 100;
  char buf[4096];
  int waited = 0;
  while (timeout_ms < 0 || waited <= timeout_ms)
  {
    if (!is_mounted_fuse(mnt))
    {
      struct timespec ts = {0, step_ms * 1000 * 1000};
      nanosleep(&ts, NULL);
      waited += step_ms;
      continue;
    }
    if (hotplug_status_json(mnt, buf, sizeof(buf)) == 0)
    {
      uint64_t st = 0;
      uint64_t epoch = 0;
      if (parse_json_u64(buf, "\"state\"", &st) == 0 &&
          parse_json_u64(buf, "\"epoch\"", &epoch) == 0)
      {
        if (st == want_state)
        {
          if (epoch_out)
            *epoch_out = epoch;
          return 0;
        }
      }
    }
    struct timespec ts = {0, step_ms * 1000 * 1000};
    nanosleep(&ts, NULL);
    waited += step_ms;
  }
  return -ETIMEDOUT;
}

static int hotplug_env_list_contains(const char *mnt, const char *needle)
{
  char buf[4096];
  const char *kafsctl = pick_kafsctl_exe();
  char *args[] = {(char *)kafsctl, "hotplug", "env", "list", (char *)mnt, NULL};
  if (run_cmd_capture(args, buf, sizeof(buf)) != 0)
    return -1;
  return strstr(buf, needle) ? 1 : 0;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("e2e_hotplug") != 0)
    return 77;

  const char *img = "kafs-e2e.img";
  const char *mnt = "kafs-e2e.mnt";
  const char *uds = "kafs-e2e.sock";

  unlink(uds);
  unlink(img);
  rmdir(mnt);

  const char *mkfs = pick_mkfs_exe();
  char *mkfs_args[] = {(char *)mkfs, (char *)img, "-s", "64M", NULL};
  if (run_cmd(mkfs_args) != 0)
  {
    tlogf("mkfs failed");
    return 77;
  }

  pid_t kafs_pid = start_kafs(img, mnt, uds);
  if (kafs_pid <= 0)
  {
    tlogf("mount failed");
    return 77;
  }

  if (wait_for_path(uds, 5000) != 0)
  {
    tlogf("uds not ready");
    kafs_test_stop_kafs(mnt, kafs_pid);
    return 1;
  }

  pid_t back_pid = spawn_kafs_back(img, uds);
  if (back_pid <= 0)
  {
    tlogf("kafs-back start failed");
    kafs_test_stop_kafs(mnt, kafs_pid);
    return 1;
  }

  if (wait_hotplug_state(mnt, 2u, NULL, 5000) != 0)
  {
    tlogf("hotplug connect timeout");
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }

  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/hello.txt", mnt);
  int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0)
  {
    tlogf("write open failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  const char msg[] = "hello";
  if (write(fd, msg, sizeof(msg)) != (ssize_t)sizeof(msg))
  {
    tlogf("write failed: %s", strerror(errno));
    close(fd);
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  close(fd);

  fd = open(path, O_RDONLY);
  if (fd < 0)
  {
    tlogf("read open failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  char buf[32];
  ssize_t r = read(fd, buf, sizeof(buf));
  close(fd);
  if (r != (ssize_t)sizeof(msg) || memcmp(buf, msg, sizeof(msg)) != 0)
  {
    tlogf("read content mismatch");
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }

  uint64_t epoch_before = 0;
  if (wait_hotplug_state(mnt, 2u, &epoch_before, 1000) != 0)
  {
    tlogf("status read failed");
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }

  const char *kafsctl = pick_kafsctl_exe();
  char *compat_args[] = {(char *)kafsctl, "hotplug", "compat", (char *)mnt, NULL};
  if (run_cmd(compat_args) != 0)
  {
    tlogf("hotplug compat failed");
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }

  char *timeout_args[] = {(char *)kafsctl, "hotplug", "set-timeout", (char *)mnt, "1000", NULL};
  if (run_cmd(timeout_args) != 0)
  {
    tlogf("hotplug set-timeout failed");
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }

  char *env_set_args[] = {(char *)kafsctl, "hotplug", "env", "set", (char *)mnt,
                          "E2E_KEY=E2E_VAL", NULL};
  if (run_cmd(env_set_args) != 0)
  {
    tlogf("hotplug env set failed");
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  if (hotplug_env_list_contains(mnt, "E2E_KEY=E2E_VAL") != 1)
  {
    tlogf("hotplug env list missing key");
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }

  char *env_unset_args[] = {(char *)kafsctl, "hotplug", "env", "unset", (char *)mnt,
                            "E2E_KEY", NULL};
  if (run_cmd(env_unset_args) != 0)
  {
    tlogf("hotplug env unset failed");
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  if (hotplug_env_list_contains(mnt, "E2E_KEY=") != 0)
  {
    tlogf("hotplug env unset did not remove key");
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }

  char *restart_args[] = {(char *)kafsctl, "hotplug", "restart-back", (char *)mnt, NULL};
  if (run_cmd(restart_args) != 0)
  {
    tlogf("hotplug restart failed");
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }

  kill(back_pid, SIGTERM);
  waitpid(back_pid, NULL, 0);
  back_pid = spawn_kafs_back(img, uds);
  if (back_pid <= 0)
  {
    tlogf("kafs-back restart failed");
    kafs_test_stop_kafs(mnt, kafs_pid);
    return 1;
  }

  uint64_t epoch_after = 0;
  if (wait_hotplug_state(mnt, 2u, &epoch_after, 5000) != 0)
  {
    tlogf("hotplug reconnect timeout");
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  if (epoch_after <= epoch_before)
  {
    tlogf("epoch did not advance: before=%llu after=%llu",
          (unsigned long long)epoch_before, (unsigned long long)epoch_after);
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }

  kafs_test_stop_kafs(mnt, kafs_pid);
  kill(back_pid, SIGTERM);
  waitpid(back_pid, NULL, 0);
  unlink(uds);
  unlink(img);
  rmdir(mnt);

  printf("e2e_hotplug OK\n");
  return 0;
}
