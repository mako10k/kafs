#pragma once
#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include <assert.h>
#include <stdio.h>
#include <errno.h>

/// ブロック番号のうち存在しないことを表す値
#define KAFS_BLO_NONE ((kafs_blkcnt_t)0)
/// ブロックマスクのビット数
#define KAFS_BLKMASK_BITS (sizeof(kafs_blkmask_t) << 3)
/// ブロックマスクのビット数の log2 の値
#define KAFS_BLKMASK_LOG_BITS (sizeof(kafs_blkmask_t) + 3)
/// ブロックマスクのビット数のマスク値
#define KAFS_BLKMASK_MASK_BITS (KAFS_BLKMASK_BITS - 1)

static kafs_blkcnt_t
kafs_get_free_blkmask (kafs_blkmask_t bm)
{
  if (sizeof (kafs_blkmask_t) <= sizeof (unsigned int))
    return __builtin_ctz (bm);
  if (sizeof (kafs_blkmask_t) <= sizeof (unsigned long))
    return __builtin_ctzl (bm);
  return __builtin_ctzll (bm);
}

/// @brief 指定されたブロック番号の利用状況を取得する
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @return 0: 未使用, != 0: 使用中
static int
kafs_blk_get_usage (const struct kafs_context *ctx, kafs_blkcnt_t blo)
{
  assert (ctx != NULL);
  assert (blo < kafs_sb_load_blkcnt (ctx->c_superblock));
  kafs_blkcnt_t blod = blo >> KAFS_BLKMASK_LOG_BITS;
  kafs_blkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS;
  return (ctx->c_blkmasktbl[blod] & (1 << blor)) != 0;
}

/// @brief 指定されたブロックの利用状況をセットする
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @param val 0: フラグをクリア, != 0: フラグをセット
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_blk_set_usage (struct kafs_context *ctx, kafs_blkcnt_t blo, kafs_hbool_t usage)
{
  assert (ctx != NULL);
  kafs_ssuperblock_t *sb = ctx->c_superblock;
  assert (blo < kafs_sb_load_blkcnt (sb));
  kafs_blkcnt_t blod = blo >> KAFS_BLKMASK_LOG_BITS;
  kafs_blkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS;
  if (usage == KAFS_TRUE)
    {
      assert (!kafs_blk_get_usage (ctx, blo));
      ctx->c_blkmasktbl[blod] |= 1 << blor;
      kafs_blkcnt_t blkcnt_free = kafs_sb_load_blkcnt_free (sb);
      assert (blkcnt_free > 0);
      kafs_sb_save_blkcnt_free (sb, blkcnt_free - 1);
      kafs_sb_save_wtime (sb, kafs_now ());
    }
  else
    {
      assert (kafs_blk_get_usage (ctx, blo));
      ctx->c_blkmasktbl[blod] &= ~(1 << blor);
      kafs_blkcnt_t blkcnt_free = kafs_sb_load_blkcnt_free (sb);
      kafs_blkcnt_t r_blkcnt = kafs_sb_load_r_blkcnt (sb);
      assert (blkcnt_free < r_blkcnt - 1);
      kafs_sb_save_blkcnt_free (sb, blkcnt_free + 1);
      kafs_sb_save_wtime (sb, kafs_now ());
    }
  return KAFS_SUCCESS;
}

/// @brief 未使用のブロック番号を取得し、使用中フラグをつける
/// @param ctx コンテキスト
/// @param pblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_alloc_blk (struct kafs_context *ctx, kafs_blkcnt_t * pblo)
{
  assert (ctx != NULL);
  assert (pblo != NULL);
  assert (*pblo == KAFS_BLO_NONE);
  kafs_blkcnt_t blo_search = ctx->c_blo_search;
  kafs_blkcnt_t blo = blo_search + 1;
  kafs_blkmask_t *blkmasktbl = ctx->c_blkmasktbl;
  kafs_blkmask_t blocnt = kafs_sb_load_blkcnt (ctx->c_superblock);
  while (blo_search != blo)
    {
      if (blo >= blocnt)
	blo = 0;
      kafs_blkcnt_t blod = blo >> KAFS_BLKMASK_LOG_BITS;
      kafs_blkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS;	// ToDo: 2周目以降は常に0
      kafs_blkmask_t blkmask = ~blkmasktbl[blod];
      if (blkmask != 0)
	{
	  kafs_blkcnt_t blor_found = kafs_get_free_blkmask (blkmask);
	  kafs_blkcnt_t blo_found = (blod << KAFS_BLKMASK_LOG_BITS) + blor_found;
	  if (blo_found < blocnt)
	    {
	      ctx->c_blo_search = blo_found;
	      *pblo = blo_found;
	      KAFS_CALL (kafs_blk_set_usage, ctx, blo_found, 1);
	      return KAFS_SUCCESS;
	    }
	}
      blo += KAFS_BLKMASK_BITS - blor;
    }
  return -ENOSPC;
}
