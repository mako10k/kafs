#include "kafs_locks.h"
#include <stdlib.h>

#if KAFS_HAS_PTHREAD
#include <pthread.h>

typedef struct {
  pthread_mutex_t global;
  pthread_mutex_t bitmap;
  pthread_mutex_t *buckets;
  uint32_t bucket_cnt;
} kafs_lock_state_t;

int kafs_ctx_locks_init(struct kafs_context *ctx)
{
  if (!ctx) return -1;
  kafs_lock_state_t *st = (kafs_lock_state_t *)calloc(1, sizeof(*st));
  if (!st) return -1;
  pthread_mutex_init(&st->global, NULL);
  pthread_mutex_init(&st->bitmap, NULL);
  st->bucket_cnt = ctx->c_hrl_bucket_cnt ? ctx->c_hrl_bucket_cnt : 1u;
  st->buckets = (pthread_mutex_t *)calloc(st->bucket_cnt, sizeof(pthread_mutex_t));
  if (!st->buckets) { free(st); return -1; }
  for (uint32_t i = 0; i < st->bucket_cnt; ++i) pthread_mutex_init(&st->buckets[i], NULL);
  ctx->c_lock_hrl_global = st;
  ctx->c_lock_hrl_buckets = st; // same state pointer
  ctx->c_lock_bitmap = st;
  return 0;
}

void kafs_ctx_locks_destroy(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_hrl_global) return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_hrl_global;
  for (uint32_t i = 0; i < st->bucket_cnt; ++i) pthread_mutex_destroy(&st->buckets[i]);
  free(st->buckets);
  pthread_mutex_destroy(&st->global);
  pthread_mutex_destroy(&st->bitmap);
  free(st);
  ctx->c_lock_hrl_buckets = NULL;
  ctx->c_lock_hrl_global = NULL;
  ctx->c_lock_bitmap = NULL;
}

void kafs_hrl_bucket_lock(struct kafs_context *ctx, uint32_t bucket)
{
  if (!ctx || !ctx->c_lock_hrl_buckets) return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_hrl_buckets;
  pthread_mutex_lock(&st->buckets[bucket % st->bucket_cnt]);
}

void kafs_hrl_bucket_unlock(struct kafs_context *ctx, uint32_t bucket)
{
  if (!ctx || !ctx->c_lock_hrl_buckets) return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_hrl_buckets;
  pthread_mutex_unlock(&st->buckets[bucket % st->bucket_cnt]);
}

void kafs_hrl_global_lock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_hrl_global) return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_hrl_global;
  pthread_mutex_lock(&st->global);
}

void kafs_hrl_global_unlock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_hrl_global) return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_hrl_global;
  pthread_mutex_unlock(&st->global);
}

void kafs_bitmap_lock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_bitmap) return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_bitmap;
  pthread_mutex_lock(&st->bitmap);
}

void kafs_bitmap_unlock(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_lock_bitmap) return;
  kafs_lock_state_t *st = (kafs_lock_state_t *)ctx->c_lock_bitmap;
  pthread_mutex_unlock(&st->bitmap);
}

#else

int kafs_ctx_locks_init(struct kafs_context *ctx) { (void)ctx; return 0; }
void kafs_ctx_locks_destroy(struct kafs_context *ctx) { (void)ctx; }
void kafs_hrl_bucket_lock(struct kafs_context *ctx, uint32_t bucket) { (void)ctx; (void)bucket; }
void kafs_hrl_bucket_unlock(struct kafs_context *ctx, uint32_t bucket) { (void)ctx; (void)bucket; }
void kafs_hrl_global_lock(struct kafs_context *ctx) { (void)ctx; }
void kafs_hrl_global_unlock(struct kafs_context *ctx) { (void)ctx; }

void kafs_bitmap_lock(struct kafs_context *ctx) { (void)ctx; }
void kafs_bitmap_unlock(struct kafs_context *ctx) { (void)ctx; }

#endif
