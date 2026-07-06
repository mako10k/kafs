#pragma once

#include "kafs.h"
#include "kafs_context.h"

#include <stdio.h>

/*
 * Internal kafs-v6 adapter.
 *
 * This is not a v5/v6 compatibility layer. It translates the dedicated
 * kafs-v6 entrypoint state into the shared FUSE runtime entrypoints that still
 * live in kafs.c under KAFS_V6_ENTRYPOINT. Keep it linked only into kafs-v6.
 */

typedef enum kafs_v6_entrypoint_adapter_mode
{
  KAFS_V6_ENTRYPOINT_ADAPTER_MODE_NONE = 0,
  KAFS_V6_ENTRYPOINT_ADAPTER_MODE_INSPECTION,
  KAFS_V6_ENTRYPOINT_ADAPTER_MODE_CONTROLLED_WRITE,
} kafs_v6_entrypoint_adapter_mode_t;

typedef struct kafs_v6_entrypoint_adapter_options
{
  kafs_v6_entrypoint_adapter_mode_t mode;
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
  int trim_on_free_explicit;
  int bg_dedup_scan_off_requested;
  int bg_dedup_scan_enabled;
  int fsync_policy_full_requested;
  int fsync_policy_other_requested;
  uint32_t fsync_policy;
} kafs_v6_entrypoint_adapter_options_t;

typedef struct kafs_v6_entrypoint_adapter_fuse_options
{
  kafs_bool_t writeback_cache_enabled;
  kafs_bool_t writeback_cache_explicit;
  kafs_bool_t trim_on_free_enabled;
  kafs_bool_t trim_on_free_explicit;
} kafs_v6_entrypoint_adapter_fuse_options_t;

int kafs_v6_entrypoint_adapter_validate_options(const kafs_v6_entrypoint_adapter_options_t *opts,
                                                FILE *err);
int kafs_v6_entrypoint_adapter_open_context(kafs_context_t *ctx, const char *image_path,
                                            kafs_v6_entrypoint_adapter_mode_t mode, FILE *err);
int kafs_v6_entrypoint_adapter_mount_main(const char *image_path, const char *mountpoint,
                                          int argc_extra, char **argv_extra,
                                          const kafs_v6_entrypoint_adapter_options_t *opts,
                                          FILE *err);

/*
 * Implemented by kafs.c under KAFS_V6_ENTRYPOINT. The adapter owns v6
 * entrypoint preparation; kafs.c owns the shared FUSE operation table and
 * cleanup path.
 */
int kafs_v6_entrypoint_adapter_run_shared_fuse(
    kafs_context_t *ctx, int argc_fuse, char **argv_fuse,
    const kafs_v6_entrypoint_adapter_fuse_options_t *opts);
