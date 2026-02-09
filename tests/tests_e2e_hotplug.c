#include "kafs_context.h"
#include "test_utils.h"

#include "kafs_ioctl.h"

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
#include <sys/syscall.h>
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

static char g_repo_cwd[PATH_MAX];

static const char *pick_exe(const char *const cands[])
{
  static char chosen[PATH_MAX];
  char tmp[PATH_MAX];
  for (int i = 0; cands[i]; ++i)
  {
    const char *c = cands[i];
    if (strchr(c, '/'))
    {
      const char *p = c;
      if (c[0] != '/' && g_repo_cwd[0] != '\0')
      {
        if ((size_t)snprintf(tmp, sizeof(tmp), "%s/%s", g_repo_cwd, c) < sizeof(tmp))
          p = tmp;
      }

      if (access(p, X_OK) == 0)
      {
        strncpy(chosen, p, sizeof(chosen) - 1);
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
  {
    tlogf("pick_kafs_exe: env=%s", env);
    return env;
  }
  const char *cands[] = {"./kafs", "../src/kafs", "./src/kafs", "src/kafs", "kafs", NULL};
  const char *p = pick_exe(cands);
  tlogf("pick_kafs_exe: %s", p);
  return p;
}

static const char *pick_kafsctl_exe(void)
{
  const char *env = getenv("KAFS_TEST_KAFSCTL");
  if (env && *env)
  {
    tlogf("pick_kafsctl_exe: env=%s", env);
    return env;
  }
  const char *cands[] = {"./kafsctl", "../src/kafsctl", "./src/kafsctl", "src/kafsctl", "kafsctl", NULL};
  const char *p = pick_exe(cands);
  tlogf("pick_kafsctl_exe: %s", p);
  return p;
}

static const char *pick_mkfs_exe(void)
{
  const char *env = getenv("KAFS_TEST_MKFS");
  if (env && *env)
  {
    tlogf("pick_mkfs_exe: env=%s", env);
    return env;
  }
  const char *cands[] = {"./mkfs.kafs", "../src/mkfs.kafs", "./src/mkfs.kafs", "src/mkfs.kafs", "mkfs.kafs", NULL};
  const char *p = pick_exe(cands);
  tlogf("pick_mkfs_exe: %s", p);
  return p;
}

static const char *pick_back_exe(void)
{
  const char *env = getenv("KAFS_TEST_KAFS_BACK");
  if (env && *env)
  {
    tlogf("pick_back_exe: env=%s", env);
    return env;
  }
  const char *cands[] = {"./kafs-back", "../src/kafs-back", "./src/kafs-back", "src/kafs-back", "kafs-back", NULL};
  const char *p = pick_exe(cands);
  tlogf("pick_back_exe: %s", p);
  return p;
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

static ssize_t do_copy_file_range(int fd_in, off_t *off_in, int fd_out, off_t *off_out, size_t len,
                                  unsigned int flags)
{
#ifdef SYS_copy_file_range
  return (ssize_t)syscall(SYS_copy_file_range, fd_in, off_in, fd_out, off_out, len, flags);
#else
  (void)fd_in;
  (void)off_in;
  (void)fd_out;
  (void)off_out;
  (void)len;
  (void)flags;
  errno = ENOSYS;
  return -1;
#endif
}

int main(void)
{
  // Deprecated: this test exercises the legacy UDS-based hotplug transport
  // (KAFS_HOTPLUG_UDS). As we migrate away from UDS, skip by default to avoid
  // spending CI/runtime budget on legacy compatibility.
  const char *enable_uds = getenv("KAFS_TEST_ENABLE_UDS");
  if (!enable_uds || strcmp(enable_uds, "1") != 0)
  {
    tlogf("SKIP: deprecated UDS hotplug test (set KAFS_TEST_ENABLE_UDS=1 to run)");
    return 77;
  }

  // Capture the repo working directory before entering the test tmpdir.
  if (!getcwd(g_repo_cwd, sizeof(g_repo_cwd)))
    g_repo_cwd[0] = '\0';

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

  // ---- hotplug-mode IO tests: copy_file_range + KAFS ioctl ----
  char psrc2[PATH_MAX];
  char pdst2[PATH_MAX];
  char preflink[PATH_MAX];
  snprintf(psrc2, sizeof(psrc2), "%s/src.bin", mnt);
  snprintf(pdst2, sizeof(pdst2), "%s/dst.bin", mnt);
  snprintf(preflink, sizeof(preflink), "%s/reflink.bin", mnt);

  int sfd = open(psrc2, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (sfd < 0)
  {
    tlogf("open(src.bin) failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  unsigned char pat[8192];
  for (size_t i = 0; i < sizeof(pat); ++i)
    pat[i] = (unsigned char)(i & 0xFF);
  for (int rep = 0; rep < 8; ++rep)
  {
    pat[0] = (unsigned char)rep;
    if (write(sfd, pat, sizeof(pat)) != (ssize_t)sizeof(pat))
    {
      tlogf("write(src.bin) failed: %s", strerror(errno));
      close(sfd);
      kafs_test_stop_kafs(mnt, kafs_pid);
      kill(back_pid, SIGTERM);
      waitpid(back_pid, NULL, 0);
      return 1;
    }
  }
  close(sfd);

  sfd = open(psrc2, O_RDONLY);
  if (sfd < 0)
  {
    tlogf("open(src.bin ro) failed: %s", strerror(errno));
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  int dfd = open(pdst2, O_CREAT | O_TRUNC | O_RDWR, 0644);
  if (dfd < 0)
  {
    tlogf("open(dst.bin) failed: %s", strerror(errno));
    close(sfd);
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }

  off_t off_in = 0;
  off_t off_out = 0;
  const size_t total = sizeof(pat) * 8u;
  size_t copied = 0;
  while (copied < total)
  {
    size_t want = total - copied;
    if (want > 64u * 1024u)
      want = 64u * 1024u;
    ssize_t n = do_copy_file_range(sfd, &off_in, dfd, &off_out, want, 0);
    if (n < 0)
    {
      tlogf("copy_file_range failed: %s", strerror(errno));
      close(dfd);
      close(sfd);
      kafs_test_stop_kafs(mnt, kafs_pid);
      kill(back_pid, SIGTERM);
      waitpid(back_pid, NULL, 0);
      return 1;
    }
    if (n == 0)
      break;
    copied += (size_t)n;
  }

  // Verify dst content matches src.
  unsigned char rbuf[8192];
  if (lseek(sfd, 0, SEEK_SET) < 0 || lseek(dfd, 0, SEEK_SET) < 0)
  {
    tlogf("lseek failed: %s", strerror(errno));
    close(dfd);
    close(sfd);
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  for (int rep = 0; rep < 8; ++rep)
  {
    if (read(sfd, rbuf, sizeof(rbuf)) != (ssize_t)sizeof(rbuf))
    {
      tlogf("read(src.bin) failed: %s", strerror(errno));
      close(dfd);
      close(sfd);
      kafs_test_stop_kafs(mnt, kafs_pid);
      kill(back_pid, SIGTERM);
      waitpid(back_pid, NULL, 0);
      return 1;
    }
    unsigned char r2[8192];
    if (read(dfd, r2, sizeof(r2)) != (ssize_t)sizeof(r2))
    {
      tlogf("read(dst.bin) failed: %s", strerror(errno));
      close(dfd);
      close(sfd);
      kafs_test_stop_kafs(mnt, kafs_pid);
      kill(back_pid, SIGTERM);
      waitpid(back_pid, NULL, 0);
      return 1;
    }
    if (memcmp(rbuf, r2, sizeof(rbuf)) != 0)
    {
      tlogf("copy_file_range content mismatch");
      close(dfd);
      close(sfd);
      kafs_test_stop_kafs(mnt, kafs_pid);
      kill(back_pid, SIGTERM);
      waitpid(back_pid, NULL, 0);
      return 1;
    }
  }

  // KAFS_IOCTL_GET_STATS
  kafs_stats_t st;
  memset(&st, 0, sizeof(st));
  if (ioctl(dfd, KAFS_IOCTL_GET_STATS, &st) != 0)
  {
    tlogf("ioctl(GET_STATS) failed: %s", strerror(errno));
    close(dfd);
    close(sfd);
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  if (st.struct_size != sizeof(st) || st.version == 0)
  {
    tlogf("GET_STATS invalid response");
    close(dfd);
    close(sfd);
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }

  // KAFS_IOCTL_COPY (reflink)
  kafs_ioctl_copy_t creq;
  memset(&creq, 0, sizeof(creq));
  creq.struct_size = (uint32_t)sizeof(creq);
  creq.flags = KAFS_IOCTL_COPY_F_REFLINK;
  snprintf(creq.src, sizeof(creq.src), "/src.bin");
  snprintf(creq.dst, sizeof(creq.dst), "/reflink.bin");
  if (ioctl(dfd, KAFS_IOCTL_COPY, &creq) != 0)
  {
    tlogf("ioctl(COPY) failed: %s", strerror(errno));
    close(dfd);
    close(sfd);
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }

  int rfd = open(preflink, O_RDWR);
  if (rfd < 0)
  {
    tlogf("open(reflink.bin) failed: %s", strerror(errno));
    close(dfd);
    close(sfd);
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  const char patch[] = "ZZZ";
  if (pwrite(rfd, patch, sizeof(patch) - 1, 123) != (ssize_t)(sizeof(patch) - 1))
  {
    tlogf("pwrite(reflink.bin) failed: %s", strerror(errno));
    close(rfd);
    close(dfd);
    close(sfd);
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  char a[4] = {0}, b[4] = {0};
  if (pread(rfd, a, 3, 123) != 3 || pread(sfd, b, 3, 123) != 3)
  {
    tlogf("pread verify failed: %s", strerror(errno));
    close(rfd);
    close(dfd);
    close(sfd);
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  if (memcmp(a, "ZZZ", 3) != 0)
  {
    tlogf("reflink dst patch verify failed");
    close(rfd);
    close(dfd);
    close(sfd);
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  if (memcmp(b, "ZZZ", 3) == 0)
  {
    tlogf("reflink src unexpectedly modified");
    close(rfd);
    close(dfd);
    close(sfd);
    kafs_test_stop_kafs(mnt, kafs_pid);
    kill(back_pid, SIGTERM);
    waitpid(back_pid, NULL, 0);
    return 1;
  }
  close(rfd);
  close(dfd);
  close(sfd);

  kafs_test_stop_kafs(mnt, kafs_pid);
  kill(back_pid, SIGTERM);
  waitpid(back_pid, NULL, 0);
  unlink(uds);
  unlink(img);
  rmdir(mnt);

  printf("e2e_hotplug OK\n");
  return 0;
}
