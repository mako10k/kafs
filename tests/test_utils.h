#pragma once

#include "kafs.h"
#include "kafs_context.h"

#include <sys/types.h>

// 共通テストユーティリティ: 画像作成（HRLあり/なし）

// enable_hrl != 0 の場合、HRL領域をレイアウトしフォーマットします。
// 戻り値: 0=成功、負値=エラー
int kafs_test_mkimg(const char *path, size_t bytes, unsigned log_bs, unsigned inodes,
                    int enable_hrl, kafs_context_t *out_ctx, off_t *out_mapsize);

// Stop a running kafs instance and unmount (best-effort).
void kafs_test_stop_kafs(const char *mnt, pid_t kafs_pid);

// Create a unique workdir under ${TMPDIR:-/tmp} and chdir into it.
// This keeps tests from polluting the repository tree.
// Returns 0 on success, negative errno on failure.
int kafs_test_enter_tmpdir(const char *tag);

// Returns the path to the kafs binary used by tests.
// If KAFS_TEST_KAFS is set, it is used; otherwise falls back to "./kafs".
const char *kafs_test_kafs_bin(void);

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
