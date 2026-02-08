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

#define KAFS_IOCTL_GET_HOTPLUG_STATUS _IOR(KAFS_IOCTL_MAGIC, 3, struct kafs_hotplug_status)

// Hotplug control (kafsctl)
#define KAFS_HOTPLUG_ENV_KEY_MAX 64
#define KAFS_HOTPLUG_ENV_VALUE_MAX 256
#define KAFS_HOTPLUG_ENV_MAX 32

struct kafs_hotplug_timeout
{
  uint32_t struct_size;
  uint32_t timeout_ms;
};

typedef struct kafs_hotplug_timeout kafs_hotplug_timeout_t;

struct kafs_hotplug_env_entry
{
  char key[KAFS_HOTPLUG_ENV_KEY_MAX];
  char value[KAFS_HOTPLUG_ENV_VALUE_MAX];
};

typedef struct kafs_hotplug_env_entry kafs_hotplug_env_entry_t;

struct kafs_hotplug_env
{
  uint32_t struct_size;
  uint32_t count;
  kafs_hotplug_env_entry_t entries[KAFS_HOTPLUG_ENV_MAX];
};

typedef struct kafs_hotplug_env kafs_hotplug_env_t;

struct kafs_hotplug_env_update
{
  uint32_t struct_size;
  char key[KAFS_HOTPLUG_ENV_KEY_MAX];
  char value[KAFS_HOTPLUG_ENV_VALUE_MAX];
};

typedef struct kafs_hotplug_env_update kafs_hotplug_env_update_t;

#define KAFS_IOCTL_HOTPLUG_RESTART _IO(KAFS_IOCTL_MAGIC, 4)
#define KAFS_IOCTL_SET_HOTPLUG_TIMEOUT _IOW(KAFS_IOCTL_MAGIC, 5, struct kafs_hotplug_timeout)
#define KAFS_IOCTL_GET_HOTPLUG_ENV _IOR(KAFS_IOCTL_MAGIC, 6, struct kafs_hotplug_env)
#define KAFS_IOCTL_SET_HOTPLUG_ENV _IOW(KAFS_IOCTL_MAGIC, 7, struct kafs_hotplug_env_update)
#define KAFS_IOCTL_UNSET_HOTPLUG_ENV _IOW(KAFS_IOCTL_MAGIC, 8, struct kafs_hotplug_env_update)
