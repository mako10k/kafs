#include "kafs_locks.h"
#include <stdlib.h>

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
  pthread_mutex_lock(&st->buckets[bucket % st->bucket_cnt]);
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
  pthread_mutex_lock(&st->global);
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
  pthread_mutex_lock(&st->bitmap);
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
  pthread_mutex_lock(&st->inode_mutexes[ino % st->inode_cnt]);
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
  pthread_mutex_lock(&st->inode_alloc);
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
  (void)ctx;
  return 0;
}
void kafs_ctx_locks_destroy(struct kafs_context *ctx) { (void)ctx; }
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
