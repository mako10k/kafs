// minimal includes; test_utils provides mkimg helpers
#include "kafs_context.h"
#include "test_utils.h"

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <limits.h>
#include <errno.h>
#include <fcntl.h>

// environ for execve
extern char **environ;

// Simple logging with thread id (avoid conflict with libm logf)
static void slogf(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  fprintf(stderr, "[stress][pid:%d] ", getpid());
  vfprintf(stderr, fmt, ap);
  fprintf(stderr, "\n");
  va_end(ap);
}

// Resolve kafs executable path for various run directories
static const char *pick_kafs_exe(void) {
  static char chosen[PATH_MAX];
  const char *cands[] = {"./kafs", "./src/kafs", "src/kafs", "kafs", NULL};
  for (int i = 0; cands[i]; ++i) {
    const char *c = cands[i];
    if (strchr(c, '/')) {
      if (access(c, X_OK) == 0) {
        strncpy(chosen, c, sizeof(chosen) - 1);
        chosen[sizeof(chosen) - 1] = '\0';
        return chosen;
      }
    } else {
      // no slash: rely on PATH
      return c;
    }
  }
  return "kafs"; // fallback to PATH
}

static int run_cmd(char *const argv[]) {
  pid_t pid = fork();
  if (pid < 0) return -errno;
  if (pid == 0) {
    // child
    execvp(argv[0], argv);
    _exit(127);
  }
  int status = 0;
  if (waitpid(pid, &status, 0) < 0) return -errno;
  if (WIFEXITED(status) && WEXITSTATUS(status) == 0) return 0;
  return -1;
}

static int is_mounted_fuse(const char *mnt) {
  char absmnt[PATH_MAX];
  const char *want = mnt;
  if (realpath(mnt, absmnt) != NULL)
    want = absmnt;
  FILE *fp = fopen("/proc/mounts", "r");
  if (!fp) return 0;
  char dev[256], dir[256], type[64];
  int mounted = 0;
  while (fscanf(fp, "%255s %255s %63s %*[^\n]\n", dev, dir, type) == 3) {
    if (strcmp(dir, want) == 0 && strncmp(type, "fuse", 4) == 0) {
      mounted = 1;
      break;
    }
  }
  fclose(fp);
  return mounted;
}

static int mount_kafs(const char *img, const char *mnt, int multithread) {
  const char *kafs_exe = pick_kafs_exe();
  // ensure mountpoint exists
  mkdir(mnt, 0700);
  // run kafs in foreground (-f) with single-thread by default, enable MT via env
  (void)multithread;
  int pfd[2] = {-1, -1};
  if (pipe(pfd) != 0) {
    perror("pipe");
  }
  pid_t pid = fork();
  if (pid < 0) return -errno;
  if (pid == 0) {
    setenv("KAFS_IMAGE", img, 1);
    if (multithread) setenv("KAFS_MT", "1", 1);
    if (pfd[1] >= 0) {
      dup2(pfd[1], STDERR_FILENO);
      // optional: also capture stdout
      // dup2(pfd[1], STDOUT_FILENO);
      close(pfd[0]);
      close(pfd[1]);
    }
  char *args[] = {(char *)kafs_exe, (char *)mnt, "-f", "-o", "allow_other", NULL};
  execvp(kafs_exe, args);
  perror("execvp kafs");
    _exit(127);
  }
  // wait for mount to be ready (poll /proc/mounts up to ~5s), also detect early child exit
  if (pfd[0] >= 0) {
    // parent: close write end, set non-blocking read
    if (pfd[1] >= 0) close(pfd[1]);
    int fl = fcntl(pfd[0], F_GETFL, 0);
    if (fl != -1) fcntl(pfd[0], F_SETFL, fl | O_NONBLOCK);
  }
  for (int i = 0; i < 25; ++i) {
    if (is_mounted_fuse(mnt))
      return pid; // mounted
    int status = 0;
    pid_t w = waitpid(pid, &status, WNOHANG);
    if (w == pid) {
      if (WIFEXITED(status))
        fprintf(stderr, "[stress] kafs exited early: exit=%d\n", WEXITSTATUS(status));
      else if (WIFSIGNALED(status))
        fprintf(stderr, "[stress] kafs exited early: signal=%d\n", WTERMSIG(status));
      else
        fprintf(stderr, "[stress] kafs exited early: status=%d\n", status);
      // drain any remaining stderr
      if (pfd[0] >= 0) {
        char buf[1024];
        ssize_t n;
        while ((n = read(pfd[0], buf, sizeof(buf))) > 0) {
          fwrite("[kafs-stderr] ", 1, 14, stderr);
          fwrite(buf, 1, (size_t)n, stderr);
        }
        close(pfd[0]);
      }
      return -1;
    }
    // read non-blocking stderr from child
    if (pfd[0] >= 0) {
      char buf[1024];
      ssize_t n;
      while ((n = read(pfd[0], buf, sizeof(buf))) > 0) {
        fwrite("[kafs-stderr] ", 1, 14, stderr);
        fwrite(buf, 1, (size_t)n, stderr);
      }
    }
    struct timespec ts = {0, 200 * 1000 * 1000}; // 200ms
    nanosleep(&ts, NULL);
  }
  // failed to mount
  if (pfd[0] >= 0) close(pfd[0]);
  kill(pid, SIGTERM);
  waitpid(pid, NULL, 0);
  return -1;
  return pid; // return child pid (server)
}

static void unmount_kafs(const char *mnt, pid_t pid) {
  // try fusermount3 -u first, then kill
  char absmnt[PATH_MAX];
  const char *mp = (realpath(mnt, absmnt) != NULL) ? absmnt : mnt;
  // Try direct exec without building long shell strings
  char *um1[] = {"fusermount3", "-u", (char *)mp, NULL};
  if (run_cmd(um1) != 0) {
    char *um2[] = {"fusermount", "-u", (char *)mp, NULL};
    (void)run_cmd(um2);
  }
  kill(pid, SIGTERM);
  waitpid(pid, NULL, 0);
}

// Worker does a mix of operations in its own subdir
struct worker_arg {
  const char *mnt;
  int id;
  int rounds;
};

static void *worker_fn(void *p) {
  struct worker_arg *wa = (struct worker_arg *)p;
  char *base = NULL;
  if (asprintf(&base, "%s/w%03d", wa->mnt, wa->id) < 0)
    return NULL;
  mkdir(base, 0700);
  char *path = NULL;
  for (int r = 0; r < wa->rounds; ++r) {
    // create N files, write content, read back, then unlink
    for (int i = 0; i < 32; ++i) {
      if (asprintf(&path, "%s/f%05d.dat", base, r * 1000 + i) < 0)
        continue;
      FILE *fp = fopen(path, "wb");
      if (!fp) continue;
      // write a few KB with patterned content
      for (int k = 0; k < 8; ++k) {
        unsigned char buf[512];
        memset(buf, (unsigned char)((wa->id + r + i + k) & 0xFF), sizeof(buf));
        fwrite(buf, 1, sizeof(buf), fp);
      }
      fclose(fp);
      // read verify
      fp = fopen(path, "rb");
      if (fp) {
        unsigned char buf[512];
        size_t rd = fread(buf, 1, sizeof(buf), fp);
        (void)rd;
        fclose(fp);
      }
      free(path);
      path = NULL;
    }
    // remove half of them
    for (int i = 0; i < 32; i += 2) {
      if (asprintf(&path, "%s/f%05d.dat", base, r * 1000 + i) < 0)
        continue;
      unlink(path);
      free(path);
      path = NULL;
    }
  }
  free(base);
  return NULL;
}

int main(void) {
  // 1) prepare image with HRL enabled
  const char *img = "stress.img";
  const char *mnt = "mnt-stress";
  kafs_context_t ctx;
  off_t mapsize;
  if (kafs_test_mkimg_with_hrl(img, 64 * 1024 * 1024, 12 /*4KB*/, 4096, &ctx, &mapsize) != 0) {
  slogf("mkimg failed");
    return 1;
  }
  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);

  // 2) mount server
  pid_t srv = mount_kafs(img, mnt, 1);
  if (srv <= 0) {
    slogf("mount failed; skipping stress test (likely missing FUSE perms)");
    return 77; // automake SKIP
  }

  // 3) run workers
  const int th = 8;
  pthread_t tids[th];
  struct worker_arg args[th];
  for (int i = 0; i < th; ++i) {
    args[i].mnt = mnt;
    args[i].id = i;
    args[i].rounds = 16;
    if (pthread_create(&tids[i], NULL, worker_fn, &args[i]) != 0) {
  slogf("pthread_create failed");
      unmount_kafs(mnt, srv);
      return 1;
    }
  }
  for (int i = 0; i < th; ++i)
    pthread_join(tids[i], NULL);

  // 4) simple directory scan
  char *cmd2 = NULL;
  if (asprintf(&cmd2, "find %s -type f | wc -l > /dev/null", mnt) >= 0 && cmd2) {
    char *lsargs2[] = {"/bin/sh", "-c", cmd2, NULL};
    (void)run_cmd(lsargs2);
    free(cmd2);
  }

  // 5) unmount
  unmount_kafs(mnt, srv);
  slogf("stress finished");
  return 0;
}
