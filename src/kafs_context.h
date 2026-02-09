#pragma once
#include "kafs.h"
#include "kafs_superblock.h"
#include "kafs_inode.h"
#include "kafs_hotplug.h"
#include <pthread.h>
#include <sys/un.h>

/// @brief コンテキスト
struct kafs_context
{
  /// @brief 画像全体の mmap ベース（MAP_SHARED）
  void *c_img_base;
  /// @brief 画像全体の mmap サイズ（バイト）
  size_t c_img_size;
  /// @brief スーパーブロック情報
  kafs_ssuperblock_t *c_superblock;
  /// @brief inode 情報
  kafs_sinode_t *c_inotbl;
  /// @brief ブロックマスク
  kafs_blkmask_t *c_blkmasktbl;
  /// @brief 前回の inode 検索情報
  kafs_inocnt_t c_ino_search;
  /// @brief 前回のブロック検索情報
  kafs_blkcnt_t c_blo_search;
  /// @brief ファイル記述子
  int c_fd;
  /// @brief mmap サイズ（メタデータ領域）
  size_t c_mapsize;
  /// @brief HRL バケットテーブル先頭（メタデータ mmap 内）
  void *c_hrl_index;
  /// @brief HRL バケット数（テーブルサイズ / バケットサイズ）
  uint32_t c_hrl_bucket_cnt;
  // --- Concurrency (optional locks) ---
  void *c_lock_hrl_buckets; // opaque pointer to mutex array
  void *c_lock_hrl_global;  // opaque pointer to global HRL mutex
  void *c_lock_bitmap;      // opaque pointer to bitmap/allocator mutex
  void *c_lock_inode;       // opaque pointer to inode locks (array + alloc)
  // --- Journal ---
  void *c_journal; // opaque pointer to journal state (kafs_journal_t*)

  // --- Runtime stats (best-effort) ---
  uint64_t c_stat_hrl_put_calls;
  uint64_t c_stat_hrl_put_hits;
  uint64_t c_stat_hrl_put_misses;
  uint64_t c_stat_hrl_put_fallback_legacy;

  // --- Runtime inode open counts (in-memory only) ---
  uint32_t *c_open_cnt; // sized to superblock inocnt (allocated at mount)

  // --- Mount context ---
  const char *c_mountpoint; // mountpoint path (from argv)

  // --- Hotplug RPC (front) ---
  int c_hotplug_fd;
  int c_hotplug_active;
  uint64_t c_hotplug_session_id;
  uint32_t c_hotplug_epoch;
  uint32_t c_hotplug_data_mode;
  int c_hotplug_state;
  int c_hotplug_last_error;
  uint32_t c_hotplug_wait_queue_len;
  uint32_t c_hotplug_wait_queue_limit;
  uint32_t c_hotplug_wait_timeout_ms;
  uint16_t c_hotplug_front_major;
  uint16_t c_hotplug_front_minor;
  uint32_t c_hotplug_front_features;
  uint16_t c_hotplug_back_major;
  uint16_t c_hotplug_back_minor;
  uint32_t c_hotplug_back_features;
  uint32_t c_hotplug_compat_result;
  int32_t c_hotplug_compat_reason;
  uint32_t c_hotplug_env_count;
  kafs_hotplug_env_entry_t c_hotplug_env[KAFS_HOTPLUG_ENV_MAX];
  // Proxy/supervisor (front) state
  pid_t c_back_pid;
  pthread_mutex_t c_hotplug_lock;
  int c_hotplug_lock_init;
  pthread_mutex_t c_hotplug_wait_lock;
  pthread_cond_t c_hotplug_wait_cond;
  int c_hotplug_wait_lock_init;
  int c_hotplug_connecting;
  char c_hotplug_uds_path[sizeof(((struct sockaddr_un *)0)->sun_path)];
};

// Lock helpers (no-op when locks not enabled in build)
int kafs_ctx_locks_init(struct kafs_context *ctx);
void kafs_ctx_locks_destroy(struct kafs_context *ctx);

typedef struct kafs_context kafs_context_t;
