#pragma once

#include "kafs.h"
#include "kafs_context.h"
#include "kafs_inode.h"

#include <sys/types.h>

// 共通テストユーティリティ: 画像作成（HRLあり/なし）

// enable_hrl != 0 の場合、HRL領域をレイアウトしフォーマットします。
// 戻り値: 0=成功、負値=エラー
int kafs_test_mkimg(const char *path, size_t bytes, unsigned log_bs, unsigned inodes,
                    int enable_hrl, kafs_context_t *out_ctx, off_t *out_mapsize);

// Stop a running kafs instance and unmount (best-effort).
void kafs_test_stop_kafs(const char *mnt, pid_t kafs_pid);

typedef struct kafs_test_mount_options
{
  const char *debug;
  const char *log_path;
  int multithread;
  int timeout_ms;
} kafs_test_mount_options_t;

// Start kafs in the foreground for mount-based tests.
// Returns the child pid on success, negative/zero on failure.
pid_t kafs_test_start_kafs(const char *img, const char *mnt,
                           const kafs_test_mount_options_t *options);

// Dump a previously captured kafs log file to stderr if it exists.
void kafs_test_dump_log(const char *log_path, const char *reason);

// Create a unique workdir under ${TMPDIR:-/tmp} and chdir into it.
// This keeps tests from polluting the repository tree.
// Returns 0 on success, negative errno on failure.
int kafs_test_enter_tmpdir(const char *tag);

// Look up a root-directory entry in a mapped image and return its inode number.
// Supports both legacy dirent layout and v4 directory records when the root dir fits inline.
int kafs_test_lookup_root_dirent_ino(void *base, off_t mapsize, const char *name,
                                     kafs_inocnt_t *out_ino, kafs_sinode_t **out_inotbl,
                                     kafs_sinode_t **out_root);

// Returns the path to the kafs binary used by tests.
// If KAFS_TEST_KAFS is set, it is used; otherwise falls back to "./kafs".
const char *kafs_test_kafs_bin(void);

// If KAFS_TEST_MKFS is set, it is used; otherwise resolves mkfs.kafs near the test binary.
const char *kafs_test_mkfs_bin(void);

// If KAFS_TEST_KAFSCTL is set, it is used; otherwise falls back to "./kafsctl".
const char *kafs_test_kafsctl_bin(void);

// If KAFS_TEST_FSCK is set, it is used; otherwise resolves fsck.kafs near the test binary.
const char *kafs_test_fsck_bin(void);

// If KAFS_TEST_KAFS_INFO is set, it is used; otherwise resolves kafs-info near the test binary.
const char *kafs_test_kafs_info_bin(void);

// If KAFS_TEST_KAFSDUMP is set, it is used; otherwise resolves kafsdump near the test binary.
const char *kafs_test_kafsdump_bin(void);

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
