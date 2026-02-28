#pragma once
#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_locks.h"
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <fuse.h>

/// ブロック番号のうち存在しないことを表す値
#define KAFS_BLO_NONE ((kafs_blkcnt_t)0)
/// ブロックマスクのビット数
#define KAFS_BLKMASK_BITS (sizeof(kafs_blkmask_t) << 3)
/// ブロックマスクのビット数の log2 の値
#define KAFS_BLKMASK_LOG_BITS (__builtin_ctz(sizeof(kafs_blkmask_t)) + 3)
/// ブロックマスクのビット数のマスク値
#define KAFS_BLKMASK_MASK_BITS (KAFS_BLKMASK_BITS - 1)

static kafs_blkcnt_t kafs_get_free_blkmask(kafs_blkmask_t bm)
{
  if (sizeof(kafs_blkmask_t) <= sizeof(unsigned int))
    return __builtin_ctz(bm);
  if (sizeof(kafs_blkmask_t) <= sizeof(unsigned long))
    return __builtin_ctzl(bm);
  return __builtin_ctzll(bm);
}

static inline uint64_t kafs_blk_now_ns(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static int kafs_meta_bitmap_words_enabled(const struct kafs_context *ctx)
{
  if (!ctx)
    return KAFS_FALSE;
  if (!ctx->c_meta_delta_enabled)
    return KAFS_FALSE;
  if (!ctx->c_meta_bitmap_words_enabled)
    return KAFS_FALSE;
  if (!ctx->c_meta_bitmap_words || !ctx->c_meta_bitmap_dirty)
    return KAFS_FALSE;
  return KAFS_TRUE;
}

static const kafs_blkmask_t *kafs_meta_bitmap_tbl_const(const struct kafs_context *ctx)
{
  if (kafs_meta_bitmap_words_enabled(ctx) && ctx->c_meta_bitmap_wordcnt > 0)
    return ctx->c_meta_bitmap_words;
  return ctx->c_blkmasktbl;
}

static kafs_blkmask_t *kafs_meta_bitmap_tbl_mut(struct kafs_context *ctx)
{
  if (kafs_meta_bitmap_words_enabled(ctx) && ctx->c_meta_bitmap_wordcnt > 0)
    return ctx->c_meta_bitmap_words;
  return ctx->c_blkmasktbl;
}

static void kafs_meta_bitmap_mark_dirty(struct kafs_context *ctx, kafs_blkcnt_t blod)
{
  if (!kafs_meta_bitmap_words_enabled(ctx))
    return;
  if ((size_t)blod >= ctx->c_meta_bitmap_wordcnt)
    return;
  if (!ctx->c_meta_bitmap_dirty[blod])
  {
    ctx->c_meta_bitmap_dirty[blod] = 1u;
    ctx->c_meta_bitmap_dirty_count++;
  }
}

/// @brief 指定されたブロック番号の利用状況を取得する
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @return 0: 未使用, != 0: 使用中
static int kafs_blk_get_usage(const struct kafs_context *ctx, kafs_blkcnt_t blo)
{
  assert(ctx != NULL);
  assert(blo < kafs_sb_blkcnt_get(ctx->c_superblock));
  kafs_blkcnt_t blod = blo >> KAFS_BLKMASK_LOG_BITS;
  kafs_blkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS;
  const kafs_blkmask_t *blkmasktbl = kafs_meta_bitmap_tbl_const(ctx);
  int ret = (blkmasktbl[blod] & ((kafs_blkmask_t)1 << blor)) != 0;
  kafs_log(KAFS_LOG_DEBUG, "%s(blo=%" PRIuFAST32 ") returns %s\n", __func__, blo,
           ret ? "true" : "false");
  return ret;
}

// MTセーフに読みたい場合のラッパ（ビットマップロックを取得）
__attribute_maybe_unused__ static int kafs_blk_get_usage_locked(struct kafs_context *ctx,
                                                                kafs_blkcnt_t blo)
{
  kafs_bitmap_lock(ctx);
  int ret = kafs_blk_get_usage((const struct kafs_context *)ctx, blo);
  kafs_bitmap_unlock(ctx);
  return ret;
}

static int kafs_alloc_v3_summary_enabled(const struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_superblock)
    return KAFS_FALSE;
  if (kafs_sb_format_version_get(ctx->c_superblock) != KAFS_FORMAT_VERSION)
    return KAFS_FALSE;
  if (kafs_sb_allocator_version_get(ctx->c_superblock) < 2u)
    return KAFS_FALSE;
  if ((kafs_sb_feature_flags_get(ctx->c_superblock) & KAFS_FEATURE_ALLOC_V2) == 0)
    return KAFS_FALSE;
  if (kafs_sb_allocator_size_get(ctx->c_superblock) == 0u)
    return KAFS_FALSE;
  return KAFS_TRUE;
}

static int kafs_alloc_v3_summary_sync_one(struct kafs_context *ctx, kafs_blkcnt_t blo)
{
  if (!ctx || !ctx->c_superblock)
    return -EINVAL;
  if (!kafs_alloc_v3_summary_enabled(ctx))
    return 0;
  if (ctx->c_alloc_v3_summary_dirty)
    return 0;

  kafs_blkcnt_t blocnt = kafs_sb_blkcnt_get(ctx->c_superblock);
  size_t l0_bytes = ((size_t)blocnt + 7u) >> 3;
  size_t l1_bytes = (l0_bytes + 7u) >> 3;
  size_t l2_bytes = (l1_bytes + 7u) >> 3;

  uint64_t off = kafs_sb_allocator_offset_get(ctx->c_superblock);
  uint64_t size = kafs_sb_allocator_size_get(ctx->c_superblock);
  uint64_t need = (uint64_t)l1_bytes + (uint64_t)l2_bytes;
  if (size < need)
    return -EINVAL;
  if (off > (uint64_t)ctx->c_mapsize || need > (uint64_t)ctx->c_mapsize - off)
    return -EINVAL;

  size_t l0_idx = ((size_t)blo) >> 3;
  if (l0_idx >= l0_bytes)
    return -EINVAL;
  size_t l1_idx = l0_idx >> 3;
  if (l1_idx >= l1_bytes)
    return -EINVAL;
  size_t l2_idx = l1_idx >> 3;
  if (l2_idx >= l2_bytes)
    return -EINVAL;

  uint8_t *base = (uint8_t *)ctx->c_superblock + (size_t)off;
  uint8_t *l1 = base;
  uint8_t *l2 = base + l1_bytes;
  const uint8_t *l0 = (const uint8_t *)kafs_meta_bitmap_tbl_const(ctx);
  uint8_t l1_mask = (uint8_t)(1u << (l0_idx & 7u));
  if (l0[l0_idx] == 0xFFu)
    l1[l1_idx] &= (uint8_t)~l1_mask;
  else
    l1[l1_idx] |= l1_mask;

  uint8_t l2_mask = (uint8_t)(1u << (l1_idx & 7u));
  if (l1[l1_idx] == 0u)
    l2[l2_idx] &= (uint8_t)~l2_mask;
  else
    l2[l2_idx] |= l2_mask;

  return 0;
}

/// @brief 指定されたブロックの利用状況をセットする
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @param usage KAFS_FALSE: フラグをクリア, KAFS_TRUE: フラグをセット
/// @return 0: 成功, < 0: 失敗 (-errno)
// Internal helper without taking bitmap lock (callers must hold bitmap lock)
static int kafs_blk_set_usage_nolock(struct kafs_context *ctx, kafs_blkcnt_t blo, kafs_bool_t usage)
{
  kafs_log(KAFS_LOG_DEBUG, "%s(blo=%" PRIuFAST32 ", usage=%s)\n", __func__, blo,
           usage ? "true" : "false");
  assert(ctx != NULL);
  kafs_ssuperblock_t *sb = ctx->c_superblock;
  assert(blo < kafs_sb_blkcnt_get(sb));
  kafs_blkcnt_t blod = blo >> KAFS_BLKMASK_LOG_BITS;
  kafs_blkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS;
  kafs_blkmask_t bit = (kafs_blkmask_t)1 << blor;
  kafs_blkmask_t *blkmasktbl = kafs_meta_bitmap_tbl_mut(ctx);
  kafs_blkmask_t word = blkmasktbl[blod];
  int was_used = (word & bit) != 0;
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_calls, 1u, __ATOMIC_RELAXED);
  if (usage == KAFS_TRUE)
  {
    __atomic_add_fetch(&ctx->c_stat_blk_set_usage_alloc_calls, 1u, __ATOMIC_RELAXED);
    if (!was_used)
    {
      uint64_t t_bit0 = kafs_blk_now_ns();
      word |= bit;
      blkmasktbl[blod] = word;
      kafs_meta_bitmap_mark_dirty(ctx, blod);
      uint64_t t_bit1 = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_bit_update, t_bit1 - t_bit0,
                         __ATOMIC_RELAXED);

      uint64_t t_free0 = kafs_blk_now_ns();
      if (ctx->c_meta_delta_enabled)
      {
        ctx->c_meta_delta_free_blocks -= 1;
      }
      else
      {
        kafs_blkcnt_t blkcnt_free = kafs_sb_blkcnt_free_get(sb);
        if (blkcnt_free > 0)
          kafs_sb_blkcnt_free_set(sb, blkcnt_free - 1);
      }
      uint64_t t_free1 = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_freecnt_update, t_free1 - t_free0,
                         __ATOMIC_RELAXED);

      uint64_t t_wtime0 = kafs_blk_now_ns();
      if (ctx->c_meta_delta_enabled)
      {
        ctx->c_meta_delta_wtime_dirty = 1u;
      }
      else
      {
        kafs_sb_wtime_set(sb, kafs_now());
      }
      uint64_t t_wtime1 = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_wtime_update, t_wtime1 - t_wtime0,
                         __ATOMIC_RELAXED);

      if (kafs_alloc_v3_summary_sync_one(ctx, blo) < 0)
        ctx->c_alloc_v3_summary_dirty = 1u;
    }
  }
  else
  {
    __atomic_add_fetch(&ctx->c_stat_blk_set_usage_free_calls, 1u, __ATOMIC_RELAXED);
    if (was_used)
    {
      uint64_t t_bit0 = kafs_blk_now_ns();
      word &= (kafs_blkmask_t)~bit;
      blkmasktbl[blod] = word;
      kafs_meta_bitmap_mark_dirty(ctx, blod);
      uint64_t t_bit1 = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_bit_update, t_bit1 - t_bit0,
                         __ATOMIC_RELAXED);

      uint64_t t_free0 = kafs_blk_now_ns();
      if (ctx->c_meta_delta_enabled)
      {
        ctx->c_meta_delta_free_blocks += 1;
      }
      else
      {
        kafs_blkcnt_t blkcnt_free = kafs_sb_blkcnt_free_get(sb);
        kafs_sb_blkcnt_free_set(sb, blkcnt_free + 1);
      }
      uint64_t t_free1 = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_freecnt_update, t_free1 - t_free0,
                         __ATOMIC_RELAXED);

      uint64_t t_wtime0 = kafs_blk_now_ns();
      if (ctx->c_meta_delta_enabled)
      {
        ctx->c_meta_delta_wtime_dirty = 1u;
      }
      else
      {
        kafs_sb_wtime_set(sb, kafs_now());
      }
      uint64_t t_wtime1 = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_wtime_update, t_wtime1 - t_wtime0,
                         __ATOMIC_RELAXED);

      if (kafs_alloc_v3_summary_sync_one(ctx, blo) < 0)
        ctx->c_alloc_v3_summary_dirty = 1u;
    }
  }
  return KAFS_SUCCESS;
}

// Fast claim helper for allocation path (caller must hold bitmap lock).
// Returns 1 when claimed, 0 when already used.
static int kafs_blk_try_claim_nolock(struct kafs_context *ctx, kafs_blkcnt_t blo)
{
  assert(ctx != NULL);
  kafs_ssuperblock_t *sb = ctx->c_superblock;
  assert(blo < kafs_sb_blkcnt_get(sb));

  kafs_blkcnt_t blod = blo >> KAFS_BLKMASK_LOG_BITS;
  kafs_blkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS;
  kafs_blkmask_t bit = (kafs_blkmask_t)1 << blor;

  kafs_blkmask_t *blkmasktbl = kafs_meta_bitmap_tbl_mut(ctx);
  kafs_blkmask_t word = blkmasktbl[blod];
  if ((word & bit) != 0)
    return 0;

  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_calls, 1u, __ATOMIC_RELAXED);
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_alloc_calls, 1u, __ATOMIC_RELAXED);

  uint64_t t_bit0 = kafs_blk_now_ns();
  word |= bit;
  blkmasktbl[blod] = word;
  kafs_meta_bitmap_mark_dirty(ctx, blod);
  uint64_t t_bit1 = kafs_blk_now_ns();
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_bit_update, t_bit1 - t_bit0, __ATOMIC_RELAXED);

  uint64_t t_free0 = kafs_blk_now_ns();
  if (ctx->c_meta_delta_enabled)
  {
    ctx->c_meta_delta_free_blocks -= 1;
  }
  else
  {
    kafs_blkcnt_t blkcnt_free = kafs_sb_blkcnt_free_get(sb);
    if (blkcnt_free > 0)
      kafs_sb_blkcnt_free_set(sb, blkcnt_free - 1);
  }
  uint64_t t_free1 = kafs_blk_now_ns();
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_freecnt_update, t_free1 - t_free0,
                     __ATOMIC_RELAXED);

  uint64_t t_wtime0 = kafs_blk_now_ns();
  if (ctx->c_meta_delta_enabled)
  {
    ctx->c_meta_delta_wtime_dirty = 1u;
  }
  else
  {
    kafs_sb_wtime_set(sb, kafs_now());
  }
  uint64_t t_wtime1 = kafs_blk_now_ns();
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_wtime_update, t_wtime1 - t_wtime0,
                     __ATOMIC_RELAXED);

  if (kafs_alloc_v3_summary_sync_one(ctx, blo) < 0)
    ctx->c_alloc_v3_summary_dirty = 1u;

  return 1;
}

// Public wrapper that takes bitmap lock
__attribute_maybe_unused__ static int kafs_blk_set_usage(struct kafs_context *ctx,
                                                         kafs_blkcnt_t blo, kafs_bool_t usage)
{
  kafs_bitmap_lock(ctx);
  int rc = kafs_blk_set_usage_nolock(ctx, blo, usage);
  kafs_bitmap_unlock(ctx);
  return rc;
}

/// @brief v3 allocator backend を使用する条件を満たすか判定する
/// @param ctx コンテキスト
/// @return KAFS_TRUE: v3 backend, KAFS_FALSE: legacy backend
static int kafs_blk_alloc_backend_is_v3(struct kafs_context *ctx)
{
  return kafs_alloc_v3_summary_enabled(ctx);
}

/// @brief 未使用のブロック番号を取得し、使用中フラグをつける（legacy）
/// @param ctx コンテキスト
/// @param pblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int kafs_blk_alloc_legacy(struct kafs_context *ctx, kafs_blkcnt_t *pblo)
{
  assert(ctx != NULL);
  assert(pblo != NULL);
  assert(*pblo == KAFS_BLO_NONE);

  __atomic_add_fetch(&ctx->c_stat_blk_alloc_calls, 1u, __ATOMIC_RELAXED);

  const kafs_blkmask_t *blkmasktbl = kafs_meta_bitmap_tbl_const(ctx);

  kafs_blkcnt_t blocnt = kafs_sb_blkcnt_get(ctx->c_superblock);
  kafs_blkcnt_t fdb = kafs_sb_first_data_block_get(ctx->c_superblock);
  if (fdb >= blocnt)
    return -ENOSPC;

  kafs_blkcnt_t blo_search = ctx->c_blo_search;
  if (blo_search < fdb)
    blo_search = fdb;
  kafs_blkcnt_t blo = blo_search + 1;
  if (blo < fdb)
    blo = fdb;

  uint64_t t_scan_start = kafs_blk_now_ns();

  while (blo_search != blo)
  {
    if (blo >= blocnt)
      blo = fdb;

    kafs_blkcnt_t blod = blo >> KAFS_BLKMASK_LOG_BITS;
    kafs_blkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS;
    kafs_blkmask_t blkmask = ~blkmasktbl[blod];

    kafs_blkcnt_t fdb_d = fdb >> KAFS_BLKMASK_LOG_BITS;
    kafs_blkcnt_t fdb_r = fdb & KAFS_BLKMASK_MASK_BITS;
    if (blod == fdb_d && fdb_r != 0)
      blkmask &= (kafs_blkmask_t)(~(((kafs_blkmask_t)1u << fdb_r) - 1u));

    if (blkmask != 0)
    {
      kafs_blkcnt_t blor_found = kafs_get_free_blkmask(blkmask);
      kafs_blkcnt_t blo_found = (blod << KAFS_BLKMASK_LOG_BITS) + blor_found;
      if (blo_found >= fdb && blo_found < blocnt)
      {
        uint64_t t_scan_stop = kafs_blk_now_ns();
        __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_scan, t_scan_stop - t_scan_start,
                           __ATOMIC_RELAXED);

        uint64_t t_claim0 = kafs_blk_now_ns();
        kafs_bitmap_lock(ctx);
        uint64_t t_set0 = kafs_blk_now_ns();
        int claimed = kafs_blk_try_claim_nolock(ctx, blo_found);
        uint64_t t_set1 = kafs_blk_now_ns();
        __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_set_usage, t_set1 - t_set0,
                           __ATOMIC_RELAXED);
        if (claimed)
        {
          ctx->c_blo_search = blo_found;
          *pblo = blo_found;
          kafs_bitmap_unlock(ctx);
          uint64_t t_claim1 = kafs_blk_now_ns();
          __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_claim, t_claim1 - t_claim0,
                             __ATOMIC_RELAXED);
          return KAFS_SUCCESS;
        }
        kafs_bitmap_unlock(ctx);
        uint64_t t_claim1 = kafs_blk_now_ns();
        __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_claim, t_claim1 - t_claim0,
                           __ATOMIC_RELAXED);
        __atomic_add_fetch(&ctx->c_stat_blk_alloc_claim_retries, 1u, __ATOMIC_RELAXED);
        t_scan_start = kafs_blk_now_ns();
      }
    }
    blo += KAFS_BLKMASK_BITS - blor;
  }

  uint64_t t_scan_end = kafs_blk_now_ns();
  __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_scan, t_scan_end - t_scan_start, __ATOMIC_RELAXED);

  return -ENOSPC;
}

static int kafs_alloc_v3_layout(struct kafs_context *ctx, uint8_t **l1, size_t *l1_bytes,
                                uint8_t **l2, size_t *l2_bytes, size_t *l0_bytes)
{
  if (!ctx || !ctx->c_superblock || !l1 || !l1_bytes || !l2 || !l2_bytes || !l0_bytes)
    return -EINVAL;

  kafs_blkcnt_t blocnt = kafs_sb_blkcnt_get(ctx->c_superblock);
  size_t l0_sz = ((size_t)blocnt + 7u) >> 3;
  size_t l1_sz = (l0_sz + 7u) >> 3;
  size_t l2_sz = (l1_sz + 7u) >> 3;
  uint64_t off = kafs_sb_allocator_offset_get(ctx->c_superblock);
  uint64_t sz = kafs_sb_allocator_size_get(ctx->c_superblock);
  uint64_t need = (uint64_t)l1_sz + (uint64_t)l2_sz;
  if (sz < need)
    return -EINVAL;
  if (off > (uint64_t)ctx->c_mapsize || need > (uint64_t)ctx->c_mapsize - off)
    return -EINVAL;

  uint8_t *base = (uint8_t *)ctx->c_superblock + (size_t)off;
  *l1 = base;
  *l1_bytes = l1_sz;
  *l2 = base + l1_sz;
  *l2_bytes = l2_sz;
  *l0_bytes = l0_sz;
  return 0;
}

static int kafs_alloc_v3_rebuild_summary_if_dirty(struct kafs_context *ctx)
{
  if (!ctx)
    return -EINVAL;
  if (!ctx->c_alloc_v3_summary_dirty)
    return 0;

  uint8_t *l1 = NULL;
  uint8_t *l2 = NULL;
  const uint8_t *l0 = (const uint8_t *)kafs_meta_bitmap_tbl_const(ctx);
  size_t l1_bytes = 0;
  size_t l2_bytes = 0;
  size_t l0_bytes = 0;
  int rc = kafs_alloc_v3_layout(ctx, &l1, &l1_bytes, &l2, &l2_bytes, &l0_bytes);
  if (rc < 0)
    return rc;

  memset(l1, 0, l1_bytes);
  memset(l2, 0, l2_bytes);
  for (size_t i = 0; i < l0_bytes; ++i)
  {
    if (l0[i] != 0xFFu)
      l1[i >> 3] |= (uint8_t)(1u << (i & 7u));
  }
  for (size_t i = 0; i < l1_bytes; ++i)
  {
    if (l1[i] != 0u)
      l2[i >> 3] |= (uint8_t)(1u << (i & 7u));
  }
  ctx->c_alloc_v3_summary_dirty = 0u;
  return 0;
}

static int kafs_alloc_v3_find_in_range(struct kafs_context *ctx, kafs_blkcnt_t start,
                                       kafs_blkcnt_t end, kafs_blkcnt_t *out_blo)
{
  if (!ctx || !out_blo || start > end)
    return 0;

  uint8_t *l1 = NULL;
  uint8_t *l2 = NULL;
  const uint8_t *l0 = (const uint8_t *)kafs_meta_bitmap_tbl_const(ctx);
  size_t l1_bytes = 0;
  size_t l2_bytes = 0;
  size_t l0_bytes = 0;
  if (kafs_alloc_v3_layout(ctx, &l1, &l1_bytes, &l2, &l2_bytes, &l0_bytes) < 0)
    return 0;

  size_t l0_start_byte = ((size_t)start) >> 3;
  size_t l0_end_byte = ((size_t)end) >> 3;
  size_t l1_start = l0_start_byte >> 3;
  size_t l1_end = l0_end_byte >> 3;

  for (size_t l1_idx = l1_start; l1_idx <= l1_end && l1_idx < l1_bytes; ++l1_idx)
  {
    size_t l2_idx = l1_idx >> 3;
    if (l2_idx >= l2_bytes)
      continue;
    if ((l2[l2_idx] & (uint8_t)(1u << (l1_idx & 7u))) == 0)
      continue;

    uint8_t l1_bits = l1[l1_idx];
    if (l1_bits == 0u)
      continue;

    size_t l0_min = l1_idx << 3;
    size_t l0_max = l0_min + 7u;
    if (l0_min < l0_start_byte)
      l1_bits &= (uint8_t)(0xFFu << (l0_start_byte - l0_min));
    if (l0_max > l0_end_byte)
    {
      size_t keep = l0_end_byte - l0_min + 1u;
      if (keep < 8u)
        l1_bits &= (uint8_t)((1u << keep) - 1u);
    }
    if (l1_bits == 0u)
      continue;

    while (l1_bits)
    {
      unsigned b = (unsigned)__builtin_ctz((unsigned)l1_bits);
      l1_bits &= (uint8_t)(l1_bits - 1u);

      size_t l0_idx = l0_min + b;
      if (l0_idx >= l0_bytes)
        continue;

      uint8_t free_bits = (uint8_t)~l0[l0_idx];
      if (l0_idx == l0_start_byte)
        free_bits &= (uint8_t)(0xFFu << (((size_t)start) & 7u));
      if (l0_idx == l0_end_byte)
      {
        size_t end_bit = ((size_t)end) & 7u;
        if (end_bit < 7u)
          free_bits &= (uint8_t)((1u << (end_bit + 1u)) - 1u);
      }
      if (free_bits == 0u)
        continue;

      unsigned bit = (unsigned)__builtin_ctz((unsigned)free_bits);
      kafs_blkcnt_t blo = (kafs_blkcnt_t)((l0_idx << 3) + bit);
      if (blo < start || blo > end)
        continue;
      *out_blo = blo;
      return 1;
    }
  }
  return 0;
}

static int kafs_blk_alloc_v3(struct kafs_context *ctx, kafs_blkcnt_t *pblo)
{
  assert(ctx != NULL);
  assert(pblo != NULL);
  assert(*pblo == KAFS_BLO_NONE);

  __atomic_add_fetch(&ctx->c_stat_blk_alloc_calls, 1u, __ATOMIC_RELAXED);

  kafs_blkcnt_t blocnt = kafs_sb_blkcnt_get(ctx->c_superblock);
  kafs_blkcnt_t fdb = kafs_sb_first_data_block_get(ctx->c_superblock);
  if (fdb >= blocnt)
    return -ENOSPC;

  kafs_blkcnt_t search_start = ctx->c_blo_search + 1;
  if (search_start < fdb || search_start >= blocnt)
    search_start = fdb;

  uint64_t t_scan_start = kafs_blk_now_ns();
  for (;;)
  {
    if (kafs_alloc_v3_rebuild_summary_if_dirty(ctx) < 0)
      return kafs_blk_alloc_legacy(ctx, pblo);

    kafs_blkcnt_t candidate = KAFS_BLO_NONE;
    int found = kafs_alloc_v3_find_in_range(ctx, search_start, blocnt - 1, &candidate);
    if (!found && search_start > fdb)
      found = kafs_alloc_v3_find_in_range(ctx, fdb, search_start - 1, &candidate);
    if (!found)
    {
      uint64_t t_scan_end = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_scan, t_scan_end - t_scan_start,
                         __ATOMIC_RELAXED);
      return -ENOSPC;
    }

    uint64_t t_scan_stop = kafs_blk_now_ns();
    __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_scan, t_scan_stop - t_scan_start,
                       __ATOMIC_RELAXED);

    uint64_t t_claim0 = kafs_blk_now_ns();
    kafs_bitmap_lock(ctx);
    uint64_t t_set0 = kafs_blk_now_ns();
    int claimed = kafs_blk_try_claim_nolock(ctx, candidate);
    uint64_t t_set1 = kafs_blk_now_ns();
    __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_set_usage, t_set1 - t_set0, __ATOMIC_RELAXED);
    if (claimed)
    {
      ctx->c_blo_search = candidate;
      *pblo = candidate;
      kafs_bitmap_unlock(ctx);
      uint64_t t_claim1 = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_claim, t_claim1 - t_claim0, __ATOMIC_RELAXED);
      return KAFS_SUCCESS;
    }
    kafs_bitmap_unlock(ctx);
    uint64_t t_claim1 = kafs_blk_now_ns();
    __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_claim, t_claim1 - t_claim0, __ATOMIC_RELAXED);
    __atomic_add_fetch(&ctx->c_stat_blk_alloc_claim_retries, 1u, __ATOMIC_RELAXED);

    search_start = candidate + 1;
    if (search_start < fdb || search_start >= blocnt)
      search_start = fdb;
    t_scan_start = kafs_blk_now_ns();
  }
}

static int kafs_blk_alloc(struct kafs_context *ctx, kafs_blkcnt_t *pblo)
{
  if (kafs_blk_alloc_backend_is_v3(ctx))
    return kafs_blk_alloc_v3(ctx, pblo);
  return kafs_blk_alloc_legacy(ctx, pblo);
}
