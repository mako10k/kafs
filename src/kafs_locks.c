#include "kafs_locks.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#ifdef __linux__
#include <execinfo.h>
#include <sys/syscall.h>
#endif
#include <time.h>

#if KAFS_HAS_PTHREAD
#include <pthread.h>

typedef struct
{
  pthread_mutex_t global;
  pthread_mutex_t bitmap;
  pthread_mutex_t *buckets;
  uint32_t bucket_cnt;
  // inode locks
  pthread_mutex_t *inode_mutexes;
  uint32_t inode_cnt;
  pthread_mutex_t inode_alloc;
} kafs_lock_state_t;

static uint32_t g_robust_unsupported_warned = 0;

typedef enum
{
  KAFS_LOCK_RANK_HRL_GLOBAL = 10,
  KAFS_LOCK_RANK_INODE_ALLOC = 20,
  KAFS_LOCK_RANK_INODE = 30,
  KAFS_LOCK_RANK_HRL_BUCKET = 40,
  KAFS_LOCK_RANK_BITMAP = 50,
} kafs_lock_rank_t;

static __thread int g_lock_rank_depth = 0;
static __thread int g_lock_rank_stack[64];

// Prevent thread cancellation while holding internal mutexes.
// FUSE worker cancellation during a locked critical section can leave stale owners.
static __thread int g_lock_cancel_depth = 0;
static __thread int g_lock_cancel_oldstate = PTHREAD_CANCEL_ENABLE;

static long kafs_lock_tid(void);
static void kafs_lock_dump_backtrace(void);

static inline void kafs_lock_rank_enter(kafs_lock_rank_t rank, const char *name)
{
  if (g_lock_rank_depth > 0)
  {
    int cur = g_lock_rank_stack[g_lock_rank_depth - 1];
    if ((int)rank < cur)
    {
      kafs_log(KAFS_LOG_ERR,
               "lock-order-violation: acquire=%s rank=%d while-holding-rank=%d tid=%ld\n",
               name ? name : "(null)", (int)rank, cur, kafs_lock_tid());
      kafs_lock_dump_backtrace();
      abort();
    }
  }
  if (g_lock_rank_depth >= (int)(sizeof(g_lock_rank_stack) / sizeof(g_lock_rank_stack[0])))
  {
    kafs_log(KAFS_LOG_ERR, "lock-rank-stack-overflow: tid=%ld\n", kafs_lock_tid());
    abort();
  }
  g_lock_rank_stack[g_lock_rank_depth++] = (int)rank;
}

static inline void kafs_lock_rank_leave(kafs_lock_rank_t rank, const char *name)
{
  if (g_lock_rank_depth <= 0)
  {
    kafs_log(KAFS_LOG_ERR, "lock-rank-stack-underflow: release=%s tid=%ld\n",
             name ? name : "(null)", kafs_lock_tid());
    abort();
  }
  int top = g_lock_rank_stack[g_lock_rank_depth - 1];
  if (top != (int)rank)
  {
    kafs_log(KAFS_LOG_ERR,
             "lock-rank-mismatch: release=%s rank=%d top=%d tid=%ld\n", name ? name : "(null)",
             (int)rank, top, kafs_lock_tid());
    kafs_lock_dump_backtrace();
    abort();
  }
  g_lock_rank_depth--;
}

static inline long kafs_mutex_owner_tid(const pthread_mutex_t *m)
{
#ifdef __linux__
  return (long)m->__data.__owner;
#else
  (void)m;
  return 0;
#endif
}

static inline int kafs_tid_alive(long tid)
{
  if (tid <= 0)
    return 1;
  if (tid == kafs_lock_tid())
    return 1;
  if (kill((pid_t)tid, 0) == 0)
    return 1;
  return errno != ESRCH;
}

static inline void kafs_timespec_add_ms(struct timespec *ts, long ms)
{
  ts->tv_sec += ms / 1000;
  ts->tv_nsec += (ms % 1000) * 1000000L;
  if (ts->tv_nsec >= 1000000000L)
  {
    ts->tv_sec += 1;
    ts->tv_nsec -= 1000000000L;
  }
}

static inline void kafs_lock_cancel_enter(void)
{
  if (g_lock_cancel_depth == 0)
  {
    int oldstate = PTHREAD_CANCEL_ENABLE;
    (void)pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &oldstate);
    g_lock_cancel_oldstate = oldstate;
  }
  g_lock_cancel_depth++;
}

static inline void kafs_lock_cancel_leave(void)
{
  if (g_lock_cancel_depth <= 0)
    return;
  g_lock_cancel_depth--;
  if (g_lock_cancel_depth == 0)
    (void)pthread_setcancelstate(g_lock_cancel_oldstate, NULL);
}

static long kafs_lock_tid(void)
{
#ifdef __linux__
  return syscall(SYS_gettid);
#else
  return (long)getpid();
#endif
}

static void kafs_lock_dump_backtrace(void)
{
#ifdef __linux__
  void *bt[24];
  int n = backtrace(bt, (int)(sizeof(bt) / sizeof(bt[0])));
  char **syms = backtrace_symbols(bt, n);
  if (!syms)
    return;
  for (int i = 0; i < n; ++i)
    kafs_log(KAFS_LOG_ERR, "lock-bt[%d]=%s\n", i, syms[i]);
  free(syms);
#endif
}

static void kafs_lock_panic(const char *op, const char *name, int rc)
{
  kafs_log(KAFS_LOG_ERR, "lock-%s failed: name=%s tid=%ld rc=%d (%s)\n", op,
           name ? name : "(null)", kafs_lock_tid(), rc, strerror(rc));
  kafs_lock_dump_backtrace();
  abort();
}

static void kafs_mutex_mark_consistent_or_panic(pthread_mutex_t *m, const char *name)
{
#if defined(PTHREAD_MUTEX_ROBUST) || defined(PTHREAD_MUTEX_ROBUST_NP)
  int crc = pthread_mutex_consistent(m);
  if (crc != 0)
    kafs_lock_panic("consistent", name, crc);
#else
  (void)m;
  (void)name;
#endif
}

static int kafs_mutex_init_checked(pthread_mutex_t *m, const char *name)
{
  pthread_mutexattr_t attr;
  int rc = pthread_mutexattr_init(&attr);
  if (rc != 0)
  {
    kafs_log(KAFS_LOG_ERR, "pthread_mutexattr_init failed: name=%s rc=%d (%s)\n",
             name ? name : "(null)", rc, strerror(rc));
    return -1;
  }

  rc = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
  if (rc != 0)
  {
    pthread_mutexattr_destroy(&attr);
    kafs_log(KAFS_LOG_ERR, "pthread_mutexattr_settype failed: name=%s rc=%d (%s)\n",
             name ? name : "(null)", rc, strerror(rc));
    return -1;
  }

#if defined(PTHREAD_MUTEX_ROBUST)
  rc = pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
  if (rc != 0)
  {
    pthread_mutexattr_destroy(&attr);
    kafs_log(KAFS_LOG_ERR, "pthread_mutexattr_setrobust failed: name=%s rc=%d (%s)\n",
             name ? name : "(null)", rc, strerror(rc));
    return -1;
  }
#elif defined(PTHREAD_MUTEX_ROBUST_NP)
  rc = pthread_mutexattr_setrobust_np(&attr, PTHREAD_MUTEX_ROBUST_NP);
  if (rc != 0)
  {
    pthread_mutexattr_destroy(&attr);
    kafs_log(KAFS_LOG_ERR, "pthread_mutexattr_setrobust_np failed: name=%s rc=%d (%s)\n",
             name ? name : "(null)", rc, strerror(rc));
    return -1;
  }
#else
  if (__atomic_exchange_n(&g_robust_unsupported_warned, 1u, __ATOMIC_RELAXED) == 0)
  {
    kafs_log(KAFS_LOG_WARNING,
             "robust mutex unsupported on this platform (logic trace only); further logs suppressed\n");
  }
#endif

  rc = pthread_mutex_init(m, &attr);
  pthread_mutexattr_destroy(&attr);
  if (rc != 0)
  {
    kafs_log(KAFS_LOG_ERR, "pthread_mutex_init failed: name=%s rc=%d (%s)\n",
             name ? name : "(null)", rc, strerror(rc));
    return -1;
  }
  return 0;
}

static inline uint64_t kafs_now_ns(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static inline void kafs_mutex_lock_stat(pthread_mutex_t *m, const char *name, kafs_lock_rank_t rank,
                                        uint64_t *acq, uint64_t *contended, uint64_t *wait_ns)
{
  __atomic_add_fetch(acq, 1u, __ATOMIC_RELAXED);
  int tr = pthread_mutex_trylock(m);
  if (tr == 0)
  {
    kafs_lock_rank_enter(rank, name);
    kafs_lock_cancel_enter();
    return;
  }
  if (tr == EOWNERDEAD)
  {
    kafs_log(KAFS_LOG_ERR,
             "lock-owner-dead: name=%s tid=%ld path=trylock (state recovered, investigate)\n",
             name ? name : "(null)", kafs_lock_tid());
    kafs_lock_dump_backtrace();
    kafs_mutex_mark_consistent_or_panic(m, name);
    __atomic_add_fetch(contended, 1u, __ATOMIC_RELAXED);
    kafs_lock_rank_enter(rank, name);
    kafs_lock_cancel_enter();
    return;
  }
  if (tr == ENOTRECOVERABLE)
    kafs_lock_panic("trylock", name, tr);
  if (tr == EBUSY)
  {
    uint64_t t0 = kafs_now_ns();
    __atomic_add_fetch(contended, 1u, __ATOMIC_RELAXED);
    for (;;)
    {
      struct timespec ts;
      clock_gettime(CLOCK_REALTIME, &ts);
      kafs_timespec_add_ms(&ts, 200);
      int rc = pthread_mutex_timedlock(m, &ts);
      if (rc == 0)
      {
        uint64_t t1 = kafs_now_ns();
        __atomic_add_fetch(wait_ns, t1 - t0, __ATOMIC_RELAXED);
        kafs_lock_rank_enter(rank, name);
        kafs_lock_cancel_enter();
        return;
      }
      if (rc == EOWNERDEAD)
      {
        uint64_t t1 = kafs_now_ns();
        __atomic_add_fetch(wait_ns, t1 - t0, __ATOMIC_RELAXED);
        kafs_log(KAFS_LOG_ERR,
                 "lock-owner-dead: name=%s tid=%ld path=timedlock (state recovered, investigate)\n",
                 name ? name : "(null)", kafs_lock_tid());
        kafs_lock_dump_backtrace();
        kafs_mutex_mark_consistent_or_panic(m, name);
        kafs_lock_rank_enter(rank, name);
        kafs_lock_cancel_enter();
        return;
      }
      if (rc == ETIMEDOUT)
      {
        long owner = kafs_mutex_owner_tid(m);
        if (owner > 0 && !kafs_tid_alive(owner))
        {
          kafs_log(KAFS_LOG_ERR,
                   "lock-stale-owner: name=%s owner=%ld waiter=%ld (hard-fail to avoid infinite hang)\n",
                   name ? name : "(null)", owner, kafs_lock_tid());
          kafs_lock_dump_backtrace();
          abort();
        }
        continue;
      }
      if (rc == ENOTRECOVERABLE)
        kafs_lock_panic("timedlock", name, rc);
      kafs_lock_panic("timedlock", name, rc);
    }
  }
  kafs_lock_panic("trylock", name, tr);
}

static inline void kafs_mutex_unlock_checked(pthread_mutex_t *m, const char *name,
                                             kafs_lock_rank_t rank)
{
  kafs_lock_rank_leave(rank, name);
  kafs_lock_cancel_leave();
  int rc = pthread_mutex_unlock(m);
  if (rc != 0)
    kafs_lock_panic("unlock", name, rc);
}

int kafs_ctx_locks_init(struct kafs_context *ctx)
{
  if (!ctx)
    return -1;
  kafs_lock_state_t *st = (kafs_lock_state_t *)calloc(1, sizeof(*st));
  if (!st)
    return -1;
  if (kafs_mutex_init_checked(&st->global, "hrl_global") != 0)
  {
    free(st);
    return -1;
  }
  if (kafs_mutex_init_checked(&st->bitmap, "bitmap") != 0)
  {
    pthread_mutex_destroy(&st->global);
    free(st);
    return -1;
  }
  st->bucket_cnt = ctx->c_hrl_bucket_cnt ? ctx->c_hrl_bucket_cnt : 1u;
  st->buckets = (pthread_mutex_t *)calloc(st->bucket_cnt, sizeof(pthread_mutex_t));
  if (!st->buckets)
  {
    pthread_mutex_destroy(&st->bitmap);
    pthread_mutex_destroy(&st->global);
    free(st);
    return -1;
  }
  for (uint32_t i = 0; i < st->bucket_cnt; ++i)
  {
    if (kafs_mutex_init_checked(&st->buckets[i], "hrl_bucket") != 0)
    {
      for (uint32_t j = 0; j < i; ++j)
        pthread_mutex_destroy(&st->buckets[j]);
      free(st->buckets);
      pthread_mutex_destroy(&st->bitmap);
      pthread_mutex_destroy(&st->global);
      free(st);
      return -1;
    }
  }
  // inode locks
  st->inode_cnt = (uint32_t)kafs_sb_inocnt_get(ctx->c_superblock);
  if (st->inode_cnt == 0)
    st->inode_cnt = 1;

  // open-count array (best-effort; only used for unlink/close reclamation)
  ctx->c_open_cnt = (uint32_t *)calloc(st->inode_cnt, sizeof(uint32_t));
  st->inode_mutexes = (pthread_mutex_t *)calloc(st->inode_cnt, sizeof(pthread_mutex_t));
  if (!st->inode_mutexes)
  {
    for (uint32_t i = 0; i < st->bucket_cnt; ++i)
      pthread_mutex_destroy(&st->buckets[i]);
    free(st->buckets);
    pthread_mutex_destroy(&st->bitmap);
    pthread_mutex_destroy(&st->global);
    free(st);
    return -1;
  }
  for (uint32_t i = 0; i < st->inode_cnt; ++i)
  {
    if (kafs_mutex_init_checked(&st->inode_mutexes[i], "inode") != 0)
    {
      for (uint32_t j = 0; j < i; ++j)
        pthread_mutex_destroy(&st->inode_mutexes[j]);
      free(st->inode_mutexes);
      for (uint32_t j = 0; j < st->bucket_cnt; ++j)
        pthread_mutex_destroy(&st->buckets[j]);
      free(st->buckets);
      pthread_mutex_destroy(&st->bitmap);
      pthread_mutex_destroy(&st->global);
      free(st);
      return -1;
    }
  }
  if (kafs_mutex_init_checked(&st->inode_alloc, "inode_alloc") != 0)
  {
    for (uint32_t i = 0; i < st->inode_cnt; ++i)
      pthread_mutex_destroy(&st->inode_mutexes[i]);
    free(st->inode_mutexes);
    for (uint32_t i = 0; i < st->bucket_cnt; ++i)
      pthread_mutex_destroy(&st->buckets[i]);
    free(st->buckets);
    pthread_mutex_destroy(&st->bitmap);
    pthread_mutex_destroy(&st->global);
    free(st);
    return -1;
  }
  ctx->c_lock_hrl_global = st;
  ctx->c_lock_hrl_buckets = st; // same state pointer
  ctx->c_lock_bitmap = st;
  ctx->c_lock_inode = st;
  return 0;
}

void kafs_ctx_locks_destroy(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_hrl_global)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_hrl_global;
  for (uint32_t i = 0; i < st->bucket_cnt; ++i)
    pthread_mutex_destroy(&st->buckets[i]);
  free(st->buckets);
  for (uint32_t i = 0; i < st->inode_cnt; ++i)
    pthread_mutex_destroy(&st->inode_mutexes[i]);
  free(st->inode_mutexes);
  pthread_mutex_destroy(&st->global);
  pthread_mutex_destroy(&st->bitmap);
  pthread_mutex_destroy(&st->inode_alloc);
  if (ctx->c_open_cnt)
  {
    free(ctx->c_open_cnt);
    ctx->c_open_cnt = NULL;
  }
  free(st);
  ctx->c_lock_hrl_buckets = NULL;
  ctx->c_lock_hrl_global = NULL;
  ctx->c_lock_bitmap = NULL;
  ctx->c_lock_inode = NULL;
}

void kafs_hrl_bucket_lock(struct kafs_context *ctx, uint32_t bucket)
{
  if (!ctx || !ctx->c_lock_hrl_buckets)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_hrl_buckets;
  kafs_mutex_lock_stat(&st->buckets[bucket % st->bucket_cnt], "hrl_bucket",
                       KAFS_LOCK_RANK_HRL_BUCKET,
                       &ctx->c_stat_lock_hrl_bucket_acquire,
                       &ctx->c_stat_lock_hrl_bucket_contended,
                       &ctx->c_stat_lock_hrl_bucket_wait_ns);
}

void kafs_hrl_bucket_unlock(struct kafs_context *ctx, uint32_t bucket)
{
  if (!ctx || !ctx->c_lock_hrl_buckets)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_hrl_buckets;
  kafs_mutex_unlock_checked(&st->buckets[bucket % st->bucket_cnt], "hrl_bucket",
                            KAFS_LOCK_RANK_HRL_BUCKET);
}

void kafs_hrl_global_lock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_hrl_global)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_hrl_global;
  kafs_mutex_lock_stat(&st->global, "hrl_global", KAFS_LOCK_RANK_HRL_GLOBAL,
                       &ctx->c_stat_lock_hrl_global_acquire,
                       &ctx->c_stat_lock_hrl_global_contended,
                       &ctx->c_stat_lock_hrl_global_wait_ns);
}

void kafs_hrl_global_unlock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_hrl_global)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_hrl_global;
  kafs_mutex_unlock_checked(&st->global, "hrl_global", KAFS_LOCK_RANK_HRL_GLOBAL);
}

void kafs_bitmap_lock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_bitmap)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_bitmap;
  kafs_mutex_lock_stat(&st->bitmap, "bitmap", KAFS_LOCK_RANK_BITMAP,
                       &ctx->c_stat_lock_bitmap_acquire,
                       &ctx->c_stat_lock_bitmap_contended, &ctx->c_stat_lock_bitmap_wait_ns);
}

void kafs_bitmap_unlock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_bitmap)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_bitmap;
  kafs_mutex_unlock_checked(&st->bitmap, "bitmap", KAFS_LOCK_RANK_BITMAP);
}

void kafs_inode_lock(struct kafs_context *ctx, uint32_t ino)
{
  if (!ctx || !ctx->c_lock_inode)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_inode;
  kafs_mutex_lock_stat(&st->inode_mutexes[ino % st->inode_cnt], "inode", KAFS_LOCK_RANK_INODE,
                       &ctx->c_stat_lock_inode_acquire,
                       &ctx->c_stat_lock_inode_contended, &ctx->c_stat_lock_inode_wait_ns);
}

void kafs_inode_unlock(struct kafs_context *ctx, uint32_t ino)
{
  if (!ctx || !ctx->c_lock_inode)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_inode;
  kafs_mutex_unlock_checked(&st->inode_mutexes[ino % st->inode_cnt], "inode",
                            KAFS_LOCK_RANK_INODE);
}

void kafs_inode_alloc_lock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_inode)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_inode;
  kafs_mutex_lock_stat(&st->inode_alloc, "inode_alloc", KAFS_LOCK_RANK_INODE_ALLOC,
                       &ctx->c_stat_lock_inode_alloc_acquire,
                       &ctx->c_stat_lock_inode_alloc_contended,
                       &ctx->c_stat_lock_inode_alloc_wait_ns);
}

void kafs_inode_alloc_unlock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_inode)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_inode;
  kafs_mutex_unlock_checked(&st->inode_alloc, "inode_alloc", KAFS_LOCK_RANK_INODE_ALLOC);
}

#else

int kafs_ctx_locks_init(struct kafs_context *ctx)
{
  if (!ctx)
    return 0;
  uint32_t cnt = (uint32_t)kafs_sb_inocnt_get(ctx->c_superblock);
  if (cnt == 0)
    cnt = 1;
  ctx->c_open_cnt = (uint32_t *)calloc(cnt, sizeof(uint32_t));
  return 0;
}
void kafs_ctx_locks_destroy(struct kafs_context *ctx)
{
  if (ctx && ctx->c_open_cnt)
  {
    free(ctx->c_open_cnt);
    ctx->c_open_cnt = NULL;
  }
}
void kafs_hrl_bucket_lock(struct kafs_context *ctx, uint32_t bucket)
{
  (void)ctx;
  (void)bucket;
}
void kafs_hrl_bucket_unlock(struct kafs_context *ctx, uint32_t bucket)
{
  (void)ctx;
  (void)bucket;
}
void kafs_hrl_global_lock(struct kafs_context *ctx) { (void)ctx; }
void kafs_hrl_global_unlock(struct kafs_context *ctx) { (void)ctx; }

void kafs_bitmap_lock(struct kafs_context *ctx) { (void)ctx; }
void kafs_bitmap_unlock(struct kafs_context *ctx) { (void)ctx; }

void kafs_inode_lock(struct kafs_context *ctx, uint32_t ino)
{
  (void)ctx;
  (void)ino;
}
void kafs_inode_unlock(struct kafs_context *ctx, uint32_t ino)
{
  (void)ctx;
  (void)ino;
}
void kafs_inode_alloc_lock(struct kafs_context *ctx) { (void)ctx; }
void kafs_inode_alloc_unlock(struct kafs_context *ctx) { (void)ctx; }

#endif
