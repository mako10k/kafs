#pragma once

#include "kafs.h"
#include "kafs_context.h"

#include <sys/types.h>

// 共通テストユーティリティ: 画像作成（HRLあり/なし）

// enable_hrl != 0 の場合、HRL領域をレイアウトしフォーマットします。
// 戻り値: 0=成功、負値=エラー
int kafs_test_mkimg(const char *path, size_t bytes, unsigned log_bs, unsigned inodes,
                    int enable_hrl, kafs_context_t *out_ctx, off_t *out_mapsize);

static inline int kafs_test_mkimg_with_hrl(const char *path, size_t bytes, unsigned log_bs,
                                           unsigned inodes, kafs_context_t *out_ctx,
                                           off_t *out_mapsize)
{
  return kafs_test_mkimg(path, bytes, log_bs, inodes, 1, out_ctx, out_mapsize);
}

static inline int kafs_test_mkimg_no_hrl(const char *path, size_t bytes, unsigned log_bs,
                                         unsigned inodes, kafs_context_t *out_ctx,
                                         off_t *out_mapsize)
{
  return kafs_test_mkimg(path, bytes, log_bs, inodes, 0, out_ctx, out_mapsize);
}
