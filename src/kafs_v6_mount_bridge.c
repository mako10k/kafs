#include "kafs_v6_mount_bridge.h"

#include "kafs_v6_runtime.h"

#include <unistd.h>

static kafs_v6_runtime_mode_t kafs_v6_mount_bridge_runtime_mode(kafs_v6_mount_bridge_mode_t mode)
{
  switch (mode)
  {
  case KAFS_V6_MOUNT_BRIDGE_MODE_INSPECTION:
    return KAFS_V6_RUNTIME_MODE_INSPECTION;
  case KAFS_V6_MOUNT_BRIDGE_MODE_CONTROLLED_WRITE:
    return KAFS_V6_RUNTIME_MODE_CONTROLLED_WRITE;
  case KAFS_V6_MOUNT_BRIDGE_MODE_NONE:
  default:
    return KAFS_V6_RUNTIME_MODE_NONE;
  }
}

static void kafs_v6_mount_bridge_close_context_fd(kafs_context_t *ctx)
{
  if (!ctx)
    return;
  if (ctx->c_fd >= 0)
    close(ctx->c_fd);
  ctx->c_fd = -1;
}

static void kafs_v6_mount_bridge_request_from_options(kafs_v6_runtime_request_t *req,
                                                      const kafs_v6_mount_bridge_options_t *opts)
{
  kafs_v6_runtime_request_init(req);
  if (!opts)
    return;

  req->mode = kafs_v6_mount_bridge_runtime_mode(opts->mode);
  req->legacy_mode_token_seen = opts->legacy_mode_token_seen != 0;
  req->hotplug_requested = opts->hotplug_requested != 0;
  req->mount_read_only_requested = opts->mount_read_only_requested != 0;
  req->mount_read_only_seen = opts->mount_read_only_seen != 0;
  req->mount_read_write_requested = opts->mount_read_write_requested != 0;
  req->no_writeback_cache_requested = opts->no_writeback_cache_requested != 0;
  req->writeback_cache_enabled = opts->writeback_cache_enabled != 0;
  req->writeback_cache_explicit = opts->writeback_cache_explicit != 0;
  req->no_trim_on_free_requested = opts->no_trim_on_free_requested != 0;
  req->trim_on_free_enabled = opts->trim_on_free_enabled != 0;
  req->bg_dedup_scan_off_requested = opts->bg_dedup_scan_off_requested != 0;
  req->bg_dedup_scan_enabled = opts->bg_dedup_scan_enabled != 0;
  req->fsync_policy_full_requested = opts->fsync_policy_full_requested != 0;
  req->fsync_policy_other_requested = opts->fsync_policy_other_requested != 0;
  req->fsync_policy = opts->fsync_policy;
}

int kafs_v6_mount_bridge_validate_options(const kafs_v6_mount_bridge_options_t *opts, FILE *err)
{
  kafs_v6_runtime_request_t req;
  kafs_v6_mount_bridge_request_from_options(&req, opts);
  return kafs_v6_runtime_report_entrypoint_request(&req, err);
}

int kafs_v6_mount_bridge_open_context(kafs_context_t *ctx, const char *image_path,
                                      kafs_v6_mount_bridge_mode_t mode, FILE *err)
{
  kafs_v6_runtime_mode_t runtime_mode = kafs_v6_mount_bridge_runtime_mode(mode);
  kafs_ssuperblock_t sbdisk;
  if (kafs_v6_runtime_open_context_image(ctx, image_path, runtime_mode, &sbdisk, err) != 0)
    return 2;

  kafs_inocnt_t inocnt = 0;
  kafs_blkcnt_t r_blkcnt = 0;
  int rc = kafs_v6_runtime_admit_mount_context(ctx, &sbdisk, runtime_mode, &inocnt, &r_blkcnt, err);
  if (rc != 0)
  {
    kafs_v6_mount_bridge_close_context_fd(ctx);
    return 2;
  }

  rc = kafs_v6_runtime_init_mount_services(ctx, image_path, runtime_mode, inocnt, r_blkcnt, err);
  if (rc != 0)
  {
    kafs_ctx_unmap_image(ctx);
    kafs_v6_mount_bridge_close_context_fd(ctx);
    return 2;
  }

  return 0;
}
