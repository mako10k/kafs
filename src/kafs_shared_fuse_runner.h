#pragma once

#include "kafs.h"
#include "kafs_context.h"

/*
 * Internal shared FUSE runner boundary.
 *
 * This is implemented by kafs_shared_fuse_runtime.c for targets that compile
 * the shared FUSE operation table. It is not an installed ABI or a
 * format-compatibility adapter; format-specific admission must complete before
 * handing an initialized context to this common runner.
 */

typedef struct kafs_shared_fuse_runtime_options
{
  kafs_bool_t writeback_cache_enabled;
  kafs_bool_t writeback_cache_explicit;
  kafs_bool_t trim_on_free_enabled;
  kafs_bool_t trim_on_free_explicit;
} kafs_shared_fuse_runtime_options_t;

typedef struct kafs_shared_fuse_run_request
{
  kafs_context_t *ctx;
  int argc_fuse;
  char **argv_fuse;
  char *hotplug_uds_path;
  const kafs_shared_fuse_runtime_options_t *runtime_options;
} kafs_shared_fuse_run_request_t;

int kafs_shared_fuse_run_request(const kafs_shared_fuse_run_request_t *request);
