#pragma once

#include "kafs.h"
#include "kafs_superblock.h"

#include <stdio.h>

typedef enum kafs_v6_runtime_mode
{
  KAFS_V6_RUNTIME_MODE_NONE = 0,
  KAFS_V6_RUNTIME_MODE_INSPECTION,
  KAFS_V6_RUNTIME_MODE_CONTROLLED_WRITE,
} kafs_v6_runtime_mode_t;

typedef enum kafs_v6_runtime_validation_reason
{
  KAFS_V6_RUNTIME_VALID = 0,
  KAFS_V6_RUNTIME_INVALID_NO_MODE,
  KAFS_V6_RUNTIME_INVALID_LEGACY_MODE_TOKEN,
  KAFS_V6_RUNTIME_INVALID_HOTPLUG,
  KAFS_V6_RUNTIME_INVALID_INSPECTION_NEEDS_RO,
  KAFS_V6_RUNTIME_INVALID_INSPECTION_WRITEBACK_CACHE,
  KAFS_V6_RUNTIME_INVALID_CONTROLLED_WITH_INSPECTION,
  KAFS_V6_RUNTIME_INVALID_CONTROLLED_RO,
  KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_RW,
  KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_NO_WRITEBACK,
  KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_NO_TRIM,
  KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_BG_OFF,
  KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_FSYNC_FULL,
  KAFS_V6_RUNTIME_INVALID_CONTROLLED_UNSAFE_WRITEBACK,
  KAFS_V6_RUNTIME_INVALID_CONTROLLED_UNSAFE_TRIM,
  KAFS_V6_RUNTIME_INVALID_CONTROLLED_UNSAFE_BG_DEDUP,
  KAFS_V6_RUNTIME_INVALID_CONTROLLED_UNSAFE_FSYNC,
} kafs_v6_runtime_validation_reason_t;

typedef struct kafs_v6_runtime_request
{
  kafs_v6_runtime_mode_t mode;
  int inspection_token_seen;
  int legacy_mode_token_seen;
  int hotplug_requested;
  int mount_read_only_requested;
  int mount_read_only_seen;
  int mount_read_write_requested;
  int no_writeback_cache_requested;
  int writeback_cache_enabled;
  int writeback_cache_explicit;
  int no_trim_on_free_requested;
  int trim_on_free_enabled;
  int bg_dedup_scan_off_requested;
  int bg_dedup_scan_enabled;
  int bg_dedup_scan_explicit;
  int fsync_policy_full_requested;
  int fsync_policy_other_requested;
  uint32_t fsync_policy;
} kafs_v6_runtime_request_t;

void kafs_v6_runtime_request_init(kafs_v6_runtime_request_t *req);
int kafs_v6_runtime_validate_kafs_request(const kafs_v6_runtime_request_t *req,
                                          kafs_v6_runtime_validation_reason_t *reason_out);
int kafs_v6_runtime_validate_entrypoint_request(const kafs_v6_runtime_request_t *req,
                                                kafs_v6_runtime_validation_reason_t *reason_out);
int kafs_v6_runtime_check_image_format(const char *image_path, uint32_t expected_format, FILE *err,
                                       const char *tool_name);
