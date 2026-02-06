#pragma once
#include "kafs.h"
#include "kafs_context.h"
// HRL API（実装は kafs_hrl.c）

// 予備：強ハッシュ（現状未使用）
#define KAFS_HRL_STRONG_LEN 32u
typedef struct
{
  uint64_t fast;
  unsigned char strong[KAFS_HRL_STRONG_LEN];
} kafs_hr_digest_t;

// HRL エントリレイアウト（メタデータ領域内）
typedef struct
{
  uint32_t refcnt;     // 0: free
  uint32_t next_plus1; // 0: end, else (index+1)
  uint32_t blo;        // 物理ブロック番号
  uint32_t _pad;       // 8Bアライン用
  uint64_t fast;       // 高速ハッシュ（FNV-1a 64）
} kafs_hrl_entry_t;

// 初期化/オープン/クローズ
int kafs_hrl_format(kafs_context_t *ctx);
int kafs_hrl_open(kafs_context_t *ctx);
int kafs_hrl_close(kafs_context_t *ctx);

// 参照操作
int kafs_hrl_lookup(kafs_context_t *ctx, const kafs_hr_digest_t *dg, kafs_hrid_t *out_hrid);
int kafs_hrl_put(kafs_context_t *ctx, const void *block_data, kafs_hrid_t *out_hrid,
                 int *out_is_new, kafs_blkcnt_t *out_blo);
int kafs_hrl_inc_ref(kafs_context_t *ctx, kafs_hrid_t hrid);
int kafs_hrl_dec_ref(kafs_context_t *ctx, kafs_hrid_t hrid);
int kafs_hrl_read_block(kafs_context_t *ctx, kafs_hrid_t hrid, void *out_buf);
int kafs_hrl_write_block(kafs_context_t *ctx, const void *buf, kafs_hrid_t *out_hrid,
                         int *out_is_new);

// 物理ブロックからの参照追加（HRL管理外なら -ENOENT）
int kafs_hrl_inc_ref_by_blo(kafs_context_t *ctx, kafs_blkcnt_t blo);

// 物理ブロックからの参照解除（HRL管理外なら直接解放）
int kafs_hrl_dec_ref_by_blo(kafs_context_t *ctx, kafs_blkcnt_t blo);
