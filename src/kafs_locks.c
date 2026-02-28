#include "kafs_locks.h"
#include <errno.h>
#include <stdlib.h>
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

static inline uint64_t kafs_now_ns(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static inline void kafs_mutex_lock_stat(pthread_mutex_t *m, uint64_t *acq, uint64_t *contended,
                                        uint64_t *wait_ns)
{
  __atomic_add_fetch(acq, 1u, __ATOMIC_RELAXED);
  int tr = pthread_mutex_trylock(m);
  if (tr == 0)
    return;
  if (tr == EBUSY)
  {
    uint64_t t0 = kafs_now_ns();
    pthread_mutex_lock(m);
    uint64_t t1 = kafs_now_ns();
    __atomic_add_fetch(contended, 1u, __ATOMIC_RELAXED);
    __atomic_add_fetch(wait_ns, t1 - t0, __ATOMIC_RELAXED);
    return;
  }
  pthread_mutex_lock(m);
}

int kafs_ctx_locks_init(struct kafs_context *ctx)
{
  if (!ctx)
    return -1;
  kafs_lock_state_t *st = (kafs_lock_state_t *)calloc(1, sizeof(*st));
  if (!st)
    return -1;
  pthread_mutex_init(&st->global, NULL);
  pthread_mutex_init(&st->bitmap, NULL);
  st->bucket_cnt = ctx->c_hrl_bucket_cnt ? ctx->c_hrl_bucket_cnt : 1u;
  st->buckets = (pthread_mutex_t *)calloc(st->bucket_cnt, sizeof(pthread_mutex_t));
  if (!st->buckets)
  {
    free(st);
    return -1;
  }
  for (uint32_t i = 0; i < st->bucket_cnt; ++i)
    pthread_mutex_init(&st->buckets[i], NULL);
  // inode locks
  st->inode_cnt = (uint32_t)kafs_sb_inocnt_get(ctx->c_superblock);
  if (st->inode_cnt == 0)
    st->inode_cnt = 1;

  // open-count array (best-effort; only used for unlink/close reclamation)
  ctx->c_open_cnt = (uint32_t *)calloc(st->inode_cnt, sizeof(uint32_t));
  st->inode_mutexes = (pthread_mutex_t *)calloc(st->inode_cnt, sizeof(pthread_mutex_t));
  if (!st->inode_mutexes)
  {
    free(st->buckets);
    free(st);
    return -1;
  }
  for (uint32_t i = 0; i < st->inode_cnt; ++i)
    pthread_mutex_init(&st->inode_mutexes[i], NULL);
  pthread_mutex_init(&st->inode_alloc, NULL);
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
  kafs_mutex_lock_stat(&st->buckets[bucket % st->bucket_cnt], &ctx->c_stat_lock_hrl_bucket_acquire,
                       &ctx->c_stat_lock_hrl_bucket_contended,
                       &ctx->c_stat_lock_hrl_bucket_wait_ns);
}

void kafs_hrl_bucket_unlock(struct kafs_context *ctx, uint32_t bucket)
{
  if (!ctx || !ctx->c_lock_hrl_buckets)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_hrl_buckets;
  pthread_mutex_unlock(&st->buckets[bucket % st->bucket_cnt]);
}

void kafs_hrl_global_lock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_hrl_global)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_hrl_global;
  kafs_mutex_lock_stat(&st->global, &ctx->c_stat_lock_hrl_global_acquire,
                       &ctx->c_stat_lock_hrl_global_contended,
                       &ctx->c_stat_lock_hrl_global_wait_ns);
}

void kafs_hrl_global_unlock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_hrl_global)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_hrl_global;
  pthread_mutex_unlock(&st->global);
}

void kafs_bitmap_lock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_bitmap)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_bitmap;
  kafs_mutex_lock_stat(&st->bitmap, &ctx->c_stat_lock_bitmap_acquire,
                       &ctx->c_stat_lock_bitmap_contended, &ctx->c_stat_lock_bitmap_wait_ns);
}

void kafs_bitmap_unlock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_bitmap)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_bitmap;
  pthread_mutex_unlock(&st->bitmap);
}

void kafs_inode_lock(struct kafs_context *ctx, uint32_t ino)
{
  if (!ctx || !ctx->c_lock_inode)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_inode;
  kafs_mutex_lock_stat(&st->inode_mutexes[ino % st->inode_cnt], &ctx->c_stat_lock_inode_acquire,
                       &ctx->c_stat_lock_inode_contended, &ctx->c_stat_lock_inode_wait_ns);
}

void kafs_inode_unlock(struct kafs_context *ctx, uint32_t ino)
{
  if (!ctx || !ctx->c_lock_inode)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_inode;
  pthread_mutex_unlock(&st->inode_mutexes[ino % st->inode_cnt]);
}

void kafs_inode_alloc_lock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_inode)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_inode;
  kafs_mutex_lock_stat(&st->inode_alloc, &ctx->c_stat_lock_inode_alloc_acquire,
                       &ctx->c_stat_lock_inode_alloc_contended,
                       &ctx->c_stat_lock_inode_alloc_wait_ns);
}

void kafs_inode_alloc_unlock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_inode)
    return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_inode;
  pthread_mutex_unlock(&st->inode_alloc);
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
