#pragma once
#include <stdint.h>
#include <sys/ioctl.h>

// Userspace -> KAFS (FUSE ioctl)
// Note: FUSE may truncate the ioctl request to 32 bits.

#define KAFS_IOCTL_MAGIC 'k'

struct kafs_stats
{
  uint32_t struct_size;
  uint32_t version;

  uint32_t blksize;
  uint32_t reserved0;

  // Filesystem (from superblock)
  uint64_t fs_blocks_total;
  uint64_t fs_blocks_free;
  uint64_t fs_inodes_total;
  uint64_t fs_inodes_free;

  // HRL snapshot (computed by scanning entries)
  uint64_t hrl_entries_total;
  uint64_t hrl_entries_used;       // refcnt>0
  uint64_t hrl_entries_duplicated; // refcnt>1
  uint64_t hrl_refcnt_sum;         // sum(refcnt) over refcnt>0

  // Runtime counters (best-effort)
  uint64_t hrl_put_calls;
  uint64_t hrl_put_hits;            // existing block used (is_new==0)
  uint64_t hrl_put_misses;          // new block stored (is_new==1)
  uint64_t hrl_put_fallback_legacy; // HRL path failed and legacy allocation used
  uint64_t hrl_put_ns_hash;
  uint64_t hrl_put_ns_find;
  uint64_t hrl_put_ns_cmp_content;
  uint64_t hrl_put_ns_slot_alloc;
  uint64_t hrl_put_ns_blk_alloc;
  uint64_t hrl_put_ns_blk_write;
  uint64_t hrl_put_chain_steps;
  uint64_t hrl_put_cmp_calls;

  // Lock contention snapshot (best-effort, runtime counters)
  uint64_t lock_hrl_bucket_acquire;
  uint64_t lock_hrl_bucket_contended;
  uint64_t lock_hrl_bucket_wait_ns;
  uint64_t lock_hrl_global_acquire;
  uint64_t lock_hrl_global_contended;
  uint64_t lock_hrl_global_wait_ns;
  uint64_t lock_bitmap_acquire;
  uint64_t lock_bitmap_contended;
  uint64_t lock_bitmap_wait_ns;
  uint64_t lock_inode_acquire;
  uint64_t lock_inode_contended;
  uint64_t lock_inode_wait_ns;
  uint64_t lock_inode_alloc_acquire;
  uint64_t lock_inode_alloc_contended;
  uint64_t lock_inode_alloc_wait_ns;

  uint64_t pwrite_calls;
  uint64_t pwrite_bytes;
  uint64_t pwrite_ns_iblk_read;
  uint64_t pwrite_ns_iblk_write;
  uint64_t pwrite_iblk_write_sample_count;
  uint64_t pwrite_iblk_write_sample_cap;
  uint64_t pwrite_iblk_write_p50_ns;
  uint64_t pwrite_iblk_write_p95_ns;
  uint64_t pwrite_iblk_write_p99_ns;
  uint64_t iblk_write_ns_hrl_put;
  uint64_t iblk_write_ns_legacy_blk_write;
  uint64_t iblk_write_ns_dec_ref;

  uint64_t blk_alloc_calls;
  uint64_t blk_alloc_claim_retries;
  uint64_t blk_alloc_ns_scan;
  uint64_t blk_alloc_ns_claim;
  uint64_t blk_alloc_ns_set_usage;

  uint64_t blk_set_usage_calls;
  uint64_t blk_set_usage_alloc_calls;
  uint64_t blk_set_usage_free_calls;
  uint64_t blk_set_usage_ns_bit_update;
  uint64_t blk_set_usage_ns_freecnt_update;
  uint64_t blk_set_usage_ns_wtime_update;
};

typedef struct kafs_stats kafs_stats_t;

#define KAFS_IOCTL_GET_STATS _IOR(KAFS_IOCTL_MAGIC, 1, struct kafs_stats)

// Path-based copy/reflink via kafsctl ("正統派": no /proc fd resolution).
// src/dst are KAFS-internal absolute paths (begin with '/').
#ifndef KAFS_IOCTL_PATH_MAX
#define KAFS_IOCTL_PATH_MAX 4096
#endif

struct kafs_ioctl_copy
{
  uint32_t struct_size;
  uint32_t flags;
  char src[KAFS_IOCTL_PATH_MAX];
  char dst[KAFS_IOCTL_PATH_MAX];
};

typedef struct kafs_ioctl_copy kafs_ioctl_copy_t;

#define KAFS_IOCTL_COPY_F_REFLINK 1u
#define KAFS_IOCTL_COPY _IOW(KAFS_IOCTL_MAGIC, 2, struct kafs_ioctl_copy)

// Hotplug status (kafsctl)
#define KAFS_HOTPLUG_STATUS_VERSION 3u

enum
{
  KAFS_HOTPLUG_STATE_DISABLED = 0,
  KAFS_HOTPLUG_STATE_WAITING = 1,
  KAFS_HOTPLUG_STATE_CONNECTED = 2,
  KAFS_HOTPLUG_STATE_ERROR = 3
};

enum
{
  KAFS_HOTPLUG_COMPAT_UNKNOWN = 0,
  KAFS_HOTPLUG_COMPAT_OK = 1,
  KAFS_HOTPLUG_COMPAT_WARN = 2,
  KAFS_HOTPLUG_COMPAT_REJECT = 3
};

struct kafs_hotplug_status
{
  uint32_t struct_size;
  uint32_t version;
  uint32_t state;
  uint32_t data_mode;
  uint64_t session_id;
  uint32_t epoch;
  int32_t last_error;
  uint32_t wait_queue_len;
  uint32_t wait_timeout_ms;
  uint32_t wait_queue_limit;
  uint16_t front_major;
  uint16_t front_minor;
  uint32_t front_features;
  uint16_t back_major;
  uint16_t back_minor;
  uint32_t back_features;
  uint32_t compat_result;
  int32_t compat_reason;
};

typedef struct kafs_hotplug_status kafs_hotplug_status_t;


