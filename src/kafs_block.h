#pragma once
#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_locks.h"
#include <assert.h>
#include <stdio.h>
#include <errno.h>
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
  int ret = (ctx->c_blkmasktbl[blod] & ((kafs_blkmask_t)1 << blor)) != 0;
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
  int was_used = (ctx->c_blkmasktbl[blod] & bit) != 0;
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_calls, 1u, __ATOMIC_RELAXED);
  if (usage == KAFS_TRUE)
  {
    __atomic_add_fetch(&ctx->c_stat_blk_set_usage_alloc_calls, 1u, __ATOMIC_RELAXED);
    if (!was_used)
    {
      uint64_t t_bit0 = kafs_blk_now_ns();
      ctx->c_blkmasktbl[blod] |= bit;
      uint64_t t_bit1 = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_bit_update, t_bit1 - t_bit0,
                         __ATOMIC_RELAXED);

      uint64_t t_free0 = kafs_blk_now_ns();
      kafs_blkcnt_t blkcnt_free = kafs_sb_blkcnt_free_get(sb);
      // 0 下回りは防ぐ（MT競合時の二重減算を避ける）
      if (blkcnt_free > 0)
        kafs_sb_blkcnt_free_set(sb, blkcnt_free - 1);
      uint64_t t_free1 = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_freecnt_update, t_free1 - t_free0,
                         __ATOMIC_RELAXED);

      uint64_t t_wtime0 = kafs_blk_now_ns();
      kafs_sb_wtime_set(sb, kafs_now());
      uint64_t t_wtime1 = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_wtime_update, t_wtime1 - t_wtime0,
                         __ATOMIC_RELAXED);
    }
  }
  else
  {
    __atomic_add_fetch(&ctx->c_stat_blk_set_usage_free_calls, 1u, __ATOMIC_RELAXED);
    if (was_used)
    {
      uint64_t t_bit0 = kafs_blk_now_ns();
      ctx->c_blkmasktbl[blod] &= ~bit;
      uint64_t t_bit1 = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_bit_update, t_bit1 - t_bit0,
                         __ATOMIC_RELAXED);

      uint64_t t_free0 = kafs_blk_now_ns();
      kafs_blkcnt_t blkcnt_free = kafs_sb_blkcnt_free_get(sb);
      kafs_sb_blkcnt_free_set(sb, blkcnt_free + 1);
      uint64_t t_free1 = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_freecnt_update, t_free1 - t_free0,
                         __ATOMIC_RELAXED);

      uint64_t t_wtime0 = kafs_blk_now_ns();
      kafs_sb_wtime_set(sb, kafs_now());
      uint64_t t_wtime1 = kafs_blk_now_ns();
      __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_wtime_update, t_wtime1 - t_wtime0,
                         __ATOMIC_RELAXED);
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

  if ((ctx->c_blkmasktbl[blod] & bit) != 0)
    return 0;

  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_calls, 1u, __ATOMIC_RELAXED);
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_alloc_calls, 1u, __ATOMIC_RELAXED);

  uint64_t t_bit0 = kafs_blk_now_ns();
  ctx->c_blkmasktbl[blod] |= bit;
  uint64_t t_bit1 = kafs_blk_now_ns();
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_bit_update, t_bit1 - t_bit0, __ATOMIC_RELAXED);

  uint64_t t_free0 = kafs_blk_now_ns();
  kafs_blkcnt_t blkcnt_free = kafs_sb_blkcnt_free_get(sb);
  if (blkcnt_free > 0)
    kafs_sb_blkcnt_free_set(sb, blkcnt_free - 1);
  uint64_t t_free1 = kafs_blk_now_ns();
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_freecnt_update, t_free1 - t_free0,
                     __ATOMIC_RELAXED);

  uint64_t t_wtime0 = kafs_blk_now_ns();
  kafs_sb_wtime_set(sb, kafs_now());
  uint64_t t_wtime1 = kafs_blk_now_ns();
  __atomic_add_fetch(&ctx->c_stat_blk_set_usage_ns_wtime_update, t_wtime1 - t_wtime0,
                     __ATOMIC_RELAXED);

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

/// @brief 未使用のブロック番号を取得し、使用中フラグをつける
/// @param ctx コンテキスト
/// @param pblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int kafs_blk_alloc(struct kafs_context *ctx, kafs_blkcnt_t *pblo)
{
  assert(ctx != NULL);
  assert(pblo != NULL);
  assert(*pblo == KAFS_BLO_NONE);

  __atomic_add_fetch(&ctx->c_stat_blk_alloc_calls, 1u, __ATOMIC_RELAXED);

  const kafs_blkmask_t *blkmasktbl = ctx->c_blkmasktbl;
  kafs_blkcnt_t blocnt = kafs_sb_blkcnt_get(ctx->c_superblock);
  kafs_blkcnt_t fdb = kafs_sb_first_data_block_get(ctx->c_superblock);
  if (fdb >= blocnt)
    return -ENOSPC;

  // Fast path: scan without holding the bitmap lock to keep the critical section short.
  // We lock only when attempting to claim a candidate block.
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
    kafs_blkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS; // next scan starts at bit blor
    kafs_blkmask_t blkmask = ~blkmasktbl[blod];

    // never allocate blocks below first_data_block
    kafs_blkcnt_t fdb_d = fdb >> KAFS_BLKMASK_LOG_BITS;
    kafs_blkcnt_t fdb_r = fdb & KAFS_BLKMASK_MASK_BITS;
    if (blod == fdb_d && fdb_r != 0)
      blkmask &= (kafs_blkmask_t)(~(((kafs_blkmask_t)1u << fdb_r) - 1u));

    // cppcheck-suppress knownConditionTrueFalse
    if (blkmask != 0)
    {
      kafs_blkcnt_t blor_found = kafs_get_free_blkmask(blkmask);
      kafs_blkcnt_t blo_found = (blod << KAFS_BLKMASK_LOG_BITS) + blor_found;
      if (blo_found >= fdb && blo_found < blocnt)
      {
        uint64_t t_scan_stop = kafs_blk_now_ns();
        __atomic_add_fetch(&ctx->c_stat_blk_alloc_ns_scan, t_scan_stop - t_scan_start,
                           __ATOMIC_RELAXED);

        // Claim under lock (recheck because scan is lock-free)
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
