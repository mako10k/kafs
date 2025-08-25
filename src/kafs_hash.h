#pragma once
#include "kafs.h"
#include "kafs_context.h"
#include <errno.h>

// Hash Reference Layer (HRL) — header-only skeleton API
// 目的: ブロック重複排除のため、内容アドレス化層を提供する。
// ひな型段階では関数は未実装(-ENOSYS)を返すか、何もしない。

/// 強ハッシュ長（BLAKE3-256想定）
#define KAFS_HRL_STRONG_LEN 32u

/// HRL ダイジェスト（高速+強）
typedef struct {
  uint64_t fast;                     // 例: xxh64
  unsigned char strong[KAFS_HRL_STRONG_LEN]; // 例: BLAKE3-256
} kafs_hr_digest_t;

// フォーマット時に HRL 領域を初期化（ひな型: 何もしない）
static inline int kafs_hrl_format(kafs_context_t *ctx) {
  (void)ctx;
  return 0;
}

// マウント時に HRL を開く（ひな型: 何もしない）
static inline int kafs_hrl_open(kafs_context_t *ctx) {
  (void)ctx;
  return 0;
}

// アンマウント時に HRL を閉じる（ひな型: 何もしない）
static inline int kafs_hrl_close(kafs_context_t *ctx) {
  (void)ctx;
  return 0;
}

// ダイジェストから HRID を検索（未発見: -ENOENT）
static inline int kafs_hrl_lookup(kafs_context_t *ctx, const kafs_hr_digest_t *dg, kafs_hrid_t *out_hrid) {
  (void)ctx; (void)dg; (void)out_hrid;
  return -ENOENT;
}

// 新規にブロックを登録し HRID を得る（ひな型: 未実装）
static inline int kafs_hrl_put(kafs_context_t *ctx, const void *block_data, kafs_hrid_t *out_hrid, int *out_is_new) {
  (void)ctx; (void)block_data; (void)out_hrid; (void)out_is_new;
  return -ENOSYS;
}

// HRID の参照カウント +1 / -1（ひな型: 未実装）
static inline int kafs_hrl_inc_ref(kafs_context_t *ctx, kafs_hrid_t hrid) {
  (void)ctx; (void)hrid;
  return -ENOSYS;
}
static inline int kafs_hrl_dec_ref(kafs_context_t *ctx, kafs_hrid_t hrid) {
  (void)ctx; (void)hrid;
  return -ENOSYS;
}

// HRID に紐づくブロック読み出し/書き込み（ひな型: 未実装）
static inline int kafs_hrl_read_block(kafs_context_t *ctx, kafs_hrid_t hrid, void *out_buf) {
  (void)ctx; (void)hrid; (void)out_buf;
  return -ENOSYS;
}
static inline int kafs_hrl_write_block(kafs_context_t *ctx, const void *buf, kafs_hrid_t *out_hrid, int *out_is_new) {
  (void)ctx; (void)buf; (void)out_hrid; (void)out_is_new;
  return -ENOSYS;
}
