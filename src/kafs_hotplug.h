#pragma once
#include "kafs_config.h"
#include <stdint.h>

#define KAFS_HOTPLUG_ENV_KEY_MAX 64
#define KAFS_HOTPLUG_ENV_VALUE_MAX 256
#define KAFS_HOTPLUG_ENV_MAX 32

#define KAFS_HOTPLUG_STATUS_VERSION 5u

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

typedef struct
{
  char key[KAFS_HOTPLUG_ENV_KEY_MAX];
  char value[KAFS_HOTPLUG_ENV_VALUE_MAX];
} kafs_hotplug_env_entry_t;

typedef struct kafs_hotplug_status
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
  uint32_t pending_worker_prio_mode;
  int32_t pending_worker_nice;
  int32_t pending_worker_prio_apply_error;
  uint32_t fsync_policy;
  uint32_t pending_ttl_soft_ms;
  uint32_t pending_ttl_hard_ms;
  uint64_t pending_oldest_age_ms;
  uint32_t pending_ttl_over_soft;
  uint32_t pending_ttl_over_hard;
} kafs_hotplug_status_t;
