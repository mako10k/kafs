#pragma once
#include "kafs_config.h"
#include "kafs.h"
#include "kafs_superblock.h"
#include "kafs_inode.h"
#include "kafs_hotplug.h"
#include <pthread.h>
#include <stdlib.h>
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
  /// @brief HRL 空きスロット free-list の先頭（0: none, else index+1）
  uint32_t c_hrl_free_head_plus1;
  /// @brief HRL 再利用可能スロット数（best-effort）
  uint32_t c_hrl_free_slot_count;
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
  uint64_t c_stat_hrl_put_ns_hash;
  uint64_t c_stat_hrl_put_ns_find;
  uint64_t c_stat_hrl_put_ns_cmp_content;
  uint64_t c_stat_hrl_put_ns_slot_alloc;
  uint64_t c_stat_hrl_put_ns_blk_alloc;
  uint64_t c_stat_hrl_put_ns_blk_write;
  uint64_t c_stat_hrl_put_chain_steps;
  uint64_t c_stat_hrl_put_cmp_calls;
  uint64_t c_stat_hrl_rescue_attempts;
  uint64_t c_stat_hrl_rescue_hits;
  uint64_t c_stat_hrl_rescue_evicts;

  uint64_t c_stat_lock_hrl_bucket_acquire;
  uint64_t c_stat_lock_hrl_bucket_contended;
  uint64_t c_stat_lock_hrl_bucket_wait_ns;
  uint64_t c_stat_lock_hrl_global_acquire;
  uint64_t c_stat_lock_hrl_global_contended;
  uint64_t c_stat_lock_hrl_global_wait_ns;
  uint64_t c_stat_lock_bitmap_acquire;
  uint64_t c_stat_lock_bitmap_contended;
  uint64_t c_stat_lock_bitmap_wait_ns;
  uint64_t c_stat_lock_inode_acquire;
  uint64_t c_stat_lock_inode_contended;
  uint64_t c_stat_lock_inode_wait_ns;
  uint64_t c_stat_lock_inode_alloc_acquire;
  uint64_t c_stat_lock_inode_alloc_contended;
  uint64_t c_stat_lock_inode_alloc_wait_ns;

  uint64_t c_stat_access_calls;
  uint64_t c_stat_access_path_walk_calls;
  uint64_t c_stat_access_fh_fastpath_hits;
  uint64_t c_stat_access_path_components;
  uint64_t c_stat_dir_snapshot_calls;
  uint64_t c_stat_dir_snapshot_bytes;
  uint64_t c_stat_dir_snapshot_meta_load_calls;
  uint64_t c_stat_dirent_view_next_calls;

  uint64_t c_stat_pwrite_calls;
  uint64_t c_stat_pwrite_bytes;
  uint64_t c_stat_pwrite_ns_iblk_read;
  uint64_t c_stat_pwrite_ns_iblk_write;
  uint64_t c_stat_pwrite_iblk_write_sample_seq;
  uint32_t c_stat_pwrite_iblk_write_sample_count;
  uint32_t c_stat_pwrite_iblk_write_sample_cap;
  uint64_t c_stat_pwrite_iblk_write_samples[1024];
  uint64_t c_stat_iblk_write_ns_hrl_put;
  uint64_t c_stat_iblk_write_ns_legacy_blk_write;
  uint64_t c_stat_iblk_write_ns_dec_ref;

  uint64_t c_stat_blk_alloc_calls;
  uint64_t c_stat_blk_alloc_claim_retries;
  uint64_t c_stat_blk_alloc_ns_scan;
  uint64_t c_stat_blk_alloc_ns_claim;
  uint64_t c_stat_blk_alloc_ns_set_usage;

  uint64_t c_stat_blk_set_usage_calls;
  uint64_t c_stat_blk_set_usage_alloc_calls;
  uint64_t c_stat_blk_set_usage_free_calls;
  uint64_t c_stat_blk_set_usage_ns_bit_update;
  uint64_t c_stat_blk_set_usage_ns_freecnt_update;
  uint64_t c_stat_blk_set_usage_ns_wtime_update;

  // copy_file_range fast path stats (best-effort)
  uint64_t c_stat_copy_share_attempt_blocks;
  uint64_t c_stat_copy_share_done_blocks;
  uint64_t c_stat_copy_share_fallback_blocks;
  uint64_t c_stat_copy_share_skip_unaligned;
  uint64_t c_stat_copy_share_skip_dst_inline;
  uint64_t c_stat_bg_dedup_replacements;
  uint64_t c_stat_bg_dedup_evicts;
  uint64_t c_stat_bg_dedup_retries;
  uint64_t c_stat_bg_dedup_steps;
  uint64_t c_stat_bg_dedup_scanned_blocks;
  uint64_t c_stat_bg_dedup_direct_candidates;
  uint64_t c_stat_bg_dedup_direct_hits;
  uint64_t c_stat_bg_dedup_index_evicts;
  uint64_t c_stat_bg_dedup_cooldowns;
  uint64_t c_stat_pending_worker_start_calls;
  uint64_t c_stat_pending_worker_start_failures;
  int32_t c_stat_pending_worker_start_last_error;
  int32_t c_stat_pending_worker_lwp_tid;
  uint64_t c_stat_pending_worker_main_entries;
  uint64_t c_stat_pending_worker_main_exits;
  uint64_t c_stat_pending_resolved;
  uint64_t c_stat_pending_old_block_freed;
  uint64_t c_stat_trim_issued;
  uint64_t c_stat_trim_failed;

  // --- Optional runtime TRIM behavior ---
  uint32_t c_trim_on_free;

  // --- Allocator v3 runtime state ---
  uint32_t c_alloc_v3_summary_dirty;

  // --- Phase2 meta delta (runtime batching) ---
  uint32_t c_meta_delta_enabled;
  int64_t c_meta_delta_free_blocks;
  uint32_t c_meta_delta_wtime_dirty;
  kafs_time_t c_meta_delta_last_wtime;
  uint32_t c_meta_bitmap_words_enabled;
  kafs_blkmask_t *c_meta_bitmap_words;
  uint8_t *c_meta_bitmap_dirty;
  size_t c_meta_bitmap_wordcnt;
  size_t c_meta_bitmap_dirty_count;

  // --- Phase3 pending log runtime state ---
  uint32_t c_pendinglog_enabled;
  void *c_pendinglog_base;
  size_t c_pendinglog_size;
  uint32_t c_pendinglog_capacity;
  uint32_t c_pendinglog_capacity_min;
  uint32_t c_pendinglog_capacity_max;
  pthread_t c_pending_worker_tid;
  pthread_mutex_t c_pending_worker_lock;
  pthread_cond_t c_pending_worker_cond;
  int c_pending_worker_lock_init;
  int c_pending_worker_running;
  int c_pending_worker_stop;
  uint32_t c_pending_worker_backoff_ms;
  uint32_t c_pending_worker_prio_mode;
  int32_t c_pending_worker_nice;
  uint32_t c_pending_worker_prio_base_mode;
  int32_t c_pending_worker_nice_base;
  uint32_t c_pending_worker_auto_boosted;
  uint32_t c_pending_ttl_soft_ms;
  uint32_t c_pending_ttl_hard_ms;
  uint64_t c_pending_oldest_age_ms;
  uint32_t c_pending_ttl_over_soft;
  uint32_t c_pending_ttl_over_hard;
  int32_t c_pending_worker_prio_apply_error;
  uint32_t c_pending_worker_prio_dirty;

  // --- Logical delete / tombstone GC runtime state ---
  pthread_t c_tombstone_gc_worker_tid;
  pthread_mutex_t c_tombstone_gc_worker_lock;
  pthread_cond_t c_tombstone_gc_worker_cond;
  int c_tombstone_gc_worker_lock_init;
  int c_tombstone_gc_worker_running;
  int c_tombstone_gc_worker_stop;
  uint32_t c_tombstone_gc_cursor;

  // --- Idle background dedup scan runtime config ---
  uint32_t c_bg_dedup_enabled;
  uint32_t c_bg_dedup_interval_ms;
  uint32_t c_bg_dedup_quiet_interval_ms;
  uint32_t c_bg_dedup_pressure_interval_ms;
  uint32_t c_bg_dedup_start_used_pct;
  uint32_t c_bg_dedup_pressure_used_pct;
  pthread_t c_bg_dedup_worker_tid;
  pthread_mutex_t c_bg_dedup_worker_lock;
  pthread_cond_t c_bg_dedup_worker_cond;
  int c_bg_dedup_worker_lock_init;
  int c_bg_dedup_worker_running;
  int c_bg_dedup_worker_stop;
  uint32_t c_bg_dedup_worker_prio_mode;
  int32_t c_bg_dedup_worker_nice;
  uint32_t c_bg_dedup_worker_prio_base_mode;
  int32_t c_bg_dedup_worker_nice_base;
  uint32_t c_bg_dedup_worker_auto_boosted;
  int32_t c_bg_dedup_worker_prio_apply_error;
  uint32_t c_bg_dedup_worker_prio_dirty;
  uint32_t c_bg_dedup_mode;
  uint32_t c_bg_dedup_telemetry_valid;
  uint64_t c_bg_dedup_last_scanned_blocks;
  uint64_t c_bg_dedup_last_direct_candidates;
  uint64_t c_bg_dedup_last_replacements;
  uint32_t c_bg_dedup_idle_skip_streak;
  uint64_t c_bg_dedup_cold_start_due_ns;

  // --- Idle background dedup scan state ---
  uint32_t c_bg_dedup_ino_cursor;
  uint32_t c_bg_dedup_iblk_cursor;
  uint32_t c_bg_dedup_anchor_ino;
  uint32_t c_bg_dedup_anchor_iblk;
  uint32_t c_bg_dedup_anchor_valid;
  uint32_t c_bg_dedup_anchor_advance_count;
  uint64_t c_bg_dedup_cooldown_until_ns;
  uint32_t c_bg_dedup_prng;

  uint32_t c_bg_dedup_idx_count;
  uint32_t c_bg_dedup_idx_next_insert;
  uint64_t c_bg_dedup_idx_fast[4096];
  uint32_t c_bg_dedup_idx_blo[4096];

  uint32_t c_bg_dedup_recent_pos;
  uint64_t c_bg_dedup_recent_fast[64];
  uint32_t c_bg_dedup_recent_blo[64];

  // --- ENOSPC rescue cache for foreground writes (best-effort) ---
  uint32_t c_hrl_rescue_recent_pos;
  uint64_t c_hrl_rescue_recent_fast[64];
  uint32_t c_hrl_rescue_recent_blo[64];

  // fsync/fdatasync behavior policy
  uint32_t c_fsync_policy;

  // --- Runtime inode open counts (in-memory only) ---
  uint32_t *c_open_cnt;  // sized to superblock inocnt (allocated at mount)
  uint32_t *c_ino_epoch; // sized to superblock inocnt (optimistic guard for pending worker)

  // --- Debug create->first-pwrite correlation (allocated only when debug enabled) ---
  uint64_t c_diag_create_seq_next;
  uint64_t *c_diag_create_seq;
  uint16_t *c_diag_create_mode;
  uint8_t *c_diag_create_first_write_seen;
  char *c_diag_create_paths;
  int c_diag_log_fd;

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

static inline uint32_t kafs_ctx_inode_format(const kafs_context_t *ctx)
{
  assert(ctx != NULL);
  assert(ctx->c_superblock != NULL);
  return kafs_u32_stoh(ctx->c_superblock->s_format_version);
}

static inline size_t kafs_ctx_inode_bytes(const kafs_context_t *ctx)
{
  return kafs_inode_bytes_for_format(kafs_ctx_inode_format(ctx));
}

static inline kafs_sinode_t *kafs_ctx_inode(kafs_context_t *ctx, kafs_inocnt_t ino)
{
  assert(ctx != NULL);
  return (kafs_sinode_t *)kafs_inode_ptr_in_table(ctx->c_inotbl, kafs_ctx_inode_format(ctx), ino);
}

static inline const kafs_sinode_t *kafs_ctx_inode_const(const kafs_context_t *ctx,
                                                        kafs_inocnt_t ino)
{
  assert(ctx != NULL);
  return (const kafs_sinode_t *)kafs_inode_ptr_const_in_table(ctx->c_inotbl,
                                                              kafs_ctx_inode_format(ctx), ino);
}

static inline kafs_inocnt_t kafs_ctx_ino_no(const kafs_context_t *ctx, const kafs_sinode_t *inoent)
{
  assert(ctx != NULL);
  assert(inoent != NULL);
  assert(ctx->c_superblock != NULL);
  assert(ctx->c_inotbl != NULL);
  size_t inode_bytes = kafs_ctx_inode_bytes(ctx);
  uint64_t table_bytes = kafs_inode_table_bytes_for_format(
      kafs_ctx_inode_format(ctx), kafs_inocnt_stoh(ctx->c_superblock->s_inocnt));
  uintptr_t base = (uintptr_t)ctx->c_inotbl;
  uintptr_t addr = (uintptr_t)inoent;
  uintptr_t end = base + (uintptr_t)table_bytes;

  if (inode_bytes == 0)
    abort();
  if (table_bytes == 0 || end < base)
    abort();
  if (addr < base || addr >= end)
    abort();

  uintptr_t diff = addr - base;
  if ((diff % inode_bytes) != 0)
    abort();
  return (kafs_inocnt_t)(diff / inode_bytes);
}

static inline kafs_sinode_taildesc_v5_t *kafs_ctx_inode_taildesc_v5(kafs_context_t *ctx,
                                                                    kafs_sinode_t *inoent)
{
  assert(ctx != NULL);
  assert(inoent != NULL);
  if (kafs_ctx_inode_format(ctx) != KAFS_FORMAT_VERSION_V5)
    return NULL;
  return &((kafs_sinode_v5_t *)inoent)->i_taildesc;
}

static inline const kafs_sinode_taildesc_v5_t *
kafs_ctx_inode_taildesc_v5_const(const kafs_context_t *ctx, const kafs_sinode_t *inoent)
{
  assert(ctx != NULL);
  assert(inoent != NULL);
  if (kafs_ctx_inode_format(ctx) != KAFS_FORMAT_VERSION_V5)
    return NULL;
  return &((const kafs_sinode_v5_t *)inoent)->i_taildesc;
}

static inline void kafs_ctx_inode_zero(kafs_context_t *ctx, kafs_sinode_t *inoent)
{
  assert(ctx != NULL);
  kafs_inode_zero_for_format(inoent, kafs_ctx_inode_format(ctx));
}
