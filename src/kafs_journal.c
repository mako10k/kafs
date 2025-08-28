#include "kafs_journal.h"
#include "kafs_context.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <time.h>

#ifdef __has_include
#  if __has_include(<pthread.h>)
#    include <pthread.h>
#    define KAFS_JOURNAL_HAS_PTHREAD 1
#  else
#    define KAFS_JOURNAL_HAS_PTHREAD 0
#  endif
#else
#  define KAFS_JOURNAL_HAS_PTHREAD 1
#  include <pthread.h>
#endif

// Extend context with journal pointer via forward declaration; we avoid header coupling.
typedef struct kafs_context_ext {
  // mirror initial layout of kafs_context to keep ABI consistent within TU
  void *c_superblock;
  void *c_inotbl;
  void *c_blkmasktbl;
  unsigned int c_ino_search;
  unsigned int c_blo_search;
  int c_fd;
  void *c_hrl_index;
  unsigned int c_hrl_bucket_cnt;
  void *c_lock_hrl_buckets; 
  void *c_lock_hrl_global;  
  void *c_lock_bitmap;      
  void *c_lock_inode;       
  // not in header: attach journal pointer at end through separate symbol
} kafs_context_ext_t;

// We'll store journal instance in a static map keyed by ctx pointer (simple single-instance)
typedef struct journal_state {
  struct kafs_context *ctx;
  kafs_journal_t j;
} journal_state_t;

static journal_state_t g_state = {0};

static void timespec_now(struct timespec *ts) {
  clock_gettime(CLOCK_REALTIME, ts);
}

static int jlock(kafs_journal_t *j) {
#if KAFS_JOURNAL_HAS_PTHREAD
  if (!j->mtx) return 0;
  return pthread_mutex_lock((pthread_mutex_t *)j->mtx);
#else
  (void)j; return 0;
#endif
}
static int junlock(kafs_journal_t *j) {
#if KAFS_JOURNAL_HAS_PTHREAD
  if (!j->mtx) return 0;
  return pthread_mutex_unlock((pthread_mutex_t *)j->mtx);
#else
  (void)j; return 0;
#endif
}

static void jwritef(kafs_journal_t *j, const char *line) {
  if (j->fd < 0) return;
  size_t len = strlen(line);
  (void)write(j->fd, line, len);
  (void)write(j->fd, "\n", 1);
}

static void jprintf(kafs_journal_t *j, const char *fmt, ...) {
  if (j->fd < 0) return;
  char buf[1024];
  va_list ap; va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  jwritef(j, buf);
}

int kafs_journal_init(struct kafs_context *ctx, const char *image_path) {
  const char *env = getenv("KAFS_JOURNAL");
  if (!env || strcmp(env, "0") == 0) {
    g_state.ctx = ctx;
    g_state.j.enabled = 0;
    g_state.j.fd = -1;
    g_state.j.seq = 0;
    g_state.j.mtx = NULL;
    return 0;
  }
  char pathbuf[1024];
  const char *jpath = env;
  if (strcmp(env, "1") == 0) {
    // default sidecar: <image>.journal
    snprintf(pathbuf, sizeof(pathbuf), "%s.journal", image_path ? image_path : "kafs");
    jpath = pathbuf;
  }
  int fd = open(jpath, O_CREAT | O_APPEND | O_WRONLY, 0644);
  if (fd < 0) {
    perror("open journal");
    // don't fail filesystem; run without journal
    g_state.ctx = ctx;
    g_state.j.enabled = 0;
    g_state.j.fd = -1;
    g_state.j.seq = 0;
    g_state.j.mtx = NULL;
    return 0;
  }
#if KAFS_JOURNAL_HAS_PTHREAD
  pthread_mutex_t *m = malloc(sizeof(pthread_mutex_t));
  if (m) pthread_mutex_init(m, NULL);
#else
  void *m = NULL;
#endif
  g_state.ctx = ctx;
  g_state.j.enabled = 1;
  g_state.j.fd = fd;
  g_state.j.seq = 0;
  g_state.j.mtx = m;
  struct timespec ts; timespec_now(&ts);
  dprintf(fd, "# kafs journal start %ld.%09ld\n", (long)ts.tv_sec, ts.tv_nsec);
  fsync(fd);
  return 0;
}

void kafs_journal_shutdown(struct kafs_context *ctx) {
  (void)ctx;
  if (g_state.ctx != ctx) return;
  kafs_journal_t *j = &g_state.j;
  if (j->fd >= 0) {
    struct timespec ts; timespec_now(&ts);
    dprintf(j->fd, "# kafs journal end %ld.%09ld\n", (long)ts.tv_sec, ts.tv_nsec);
    fsync(j->fd);
    close(j->fd);
  }
#if KAFS_JOURNAL_HAS_PTHREAD
  if (j->mtx) { pthread_mutex_destroy((pthread_mutex_t *)j->mtx); free(j->mtx); }
#endif
  memset(&g_state, 0, sizeof(g_state));
}

uint64_t kafs_journal_begin(struct kafs_context *ctx, const char *op, const char *fmt, ...) {
  if (g_state.ctx != ctx || !g_state.j.enabled) return 0;
  kafs_journal_t *j = &g_state.j;
  jlock(j);
  uint64_t id = ++j->seq;
  struct timespec ts; timespec_now(&ts);
  dprintf(j->fd, "BEGIN %llu %s %ld.%09ld ", (unsigned long long)id, op, (long)ts.tv_sec, ts.tv_nsec);
  if (fmt && *fmt) {
    va_list ap; va_start(ap, fmt);
    vdprintf(j->fd, fmt, ap);
    va_end(ap);
  }
  dprintf(j->fd, "\n");
  fsync(j->fd);
  junlock(j);
  return id;
}

void kafs_journal_commit(struct kafs_context *ctx, uint64_t seq) {
  if (g_state.ctx != ctx || !g_state.j.enabled || seq == 0) return;
  kafs_journal_t *j = &g_state.j;
  jlock(j);
  struct timespec ts; timespec_now(&ts);
  dprintf(j->fd, "COMMIT %llu %ld.%09ld\n", (unsigned long long)seq, (long)ts.tv_sec, ts.tv_nsec);
  fsync(j->fd);
  junlock(j);
}

void kafs_journal_abort(struct kafs_context *ctx, uint64_t seq, const char *reason_fmt, ...) {
  if (g_state.ctx != ctx || !g_state.j.enabled || seq == 0) return;
  kafs_journal_t *j = &g_state.j;
  jlock(j);
  struct timespec ts; timespec_now(&ts);
  dprintf(j->fd, "ABORT %llu %ld.%09ld ", (unsigned long long)seq, (long)ts.tv_sec, ts.tv_nsec);
  if (reason_fmt && *reason_fmt) {
    va_list ap; va_start(ap, reason_fmt);
    vdprintf(j->fd, reason_fmt, ap);
    va_end(ap);
  }
  dprintf(j->fd, "\n");
  fsync(j->fd);
  junlock(j);
}

void kafs_journal_note(struct kafs_context *ctx, const char *op, const char *fmt, ...) {
  if (g_state.ctx != ctx || !g_state.j.enabled) return;
  kafs_journal_t *j = &g_state.j;
  jlock(j);
  struct timespec ts; timespec_now(&ts);
  dprintf(j->fd, "NOTE %s %ld.%09ld ", op, (long)ts.tv_sec, ts.tv_nsec);
  if (fmt && *fmt) {
    va_list ap; va_start(ap, fmt);
    vdprintf(j->fd, fmt, ap);
    va_end(ap);
  }
  dprintf(j->fd, "\n");
  fsync(j->fd);
  junlock(j);
}
