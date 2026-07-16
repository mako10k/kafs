#include "kafs_v7_entrypoint_adapter.h"

#include "kafs_inode.h"
#include "kafs_rpc.h"
#include "kafs_shared_fuse_runner.h"
#include "kafs_v7_mount_options.h"
#include "kafs_v7_runtime.h"

#include <fcntl.h>
#include <limits.h>
#include <string.h>
#include <unistd.h>

#define KAFS_V7_ENTRYPOINT_PENDING_TTL_SOFT_MS_DEFAULT 5000u
#define KAFS_V7_ENTRYPOINT_PENDING_TTL_HARD_MS_DEFAULT 30000u
#define KAFS_V7_ENTRYPOINT_HOTPLUG_WAIT_TIMEOUT_MS_DEFAULT 2000u
#define KAFS_V7_ENTRYPOINT_HOTPLUG_WAIT_QUEUE_LIMIT_DEFAULT 64u
#define KAFS_V7_ENTRYPOINT_BG_DEDUP_INTERVAL_MS_DEFAULT 2000u
#define KAFS_V7_ENTRYPOINT_BG_DEDUP_QUIET_INTERVAL_MS_DEFAULT 5000u
#define KAFS_V7_ENTRYPOINT_BG_DEDUP_PRESSURE_INTERVAL_MS_DEFAULT 100u
#define KAFS_V7_ENTRYPOINT_BG_DEDUP_START_USED_PCT_DEFAULT 85u
#define KAFS_V7_ENTRYPOINT_BG_DEDUP_PRESSURE_USED_PCT_DEFAULT 95u
#define KAFS_V7_ENTRYPOINT_BG_DEDUP_MODE_COLD 1u

#ifndef KAFS_V7_TOOL_NAME
#define KAFS_V7_TOOL_NAME "kafs-v7"
#endif

static kafs_v7_runtime_mode_t
kafs_v7_entrypoint_adapter_runtime_mode(kafs_v7_entrypoint_adapter_mode_t mode)
{
  switch (mode)
  {
  case KAFS_V7_ENTRYPOINT_ADAPTER_MODE_INSPECTION:
    return KAFS_V7_RUNTIME_MODE_INSPECTION;
  case KAFS_V7_ENTRYPOINT_ADAPTER_MODE_CONTROLLED_WRITE:
    return KAFS_V7_RUNTIME_MODE_CONTROLLED_WRITE;
  case KAFS_V7_ENTRYPOINT_ADAPTER_MODE_NONE:
  default:
    return KAFS_V7_RUNTIME_MODE_NONE;
  }
}

static void kafs_v7_entrypoint_adapter_close_context_fd(kafs_context_t *ctx)
{
  if (!ctx)
    return;
  if (ctx->c_fd >= 0)
    close(ctx->c_fd);
  ctx->c_fd = -1;
}

static void
kafs_v7_entrypoint_adapter_request_from_options(kafs_v7_runtime_request_t *req,
                                                const kafs_v7_entrypoint_adapter_options_t *opts)
{
  kafs_v7_runtime_request_init(req);
  if (!opts)
    return;

  req->mode = kafs_v7_entrypoint_adapter_runtime_mode(opts->mode);
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

static void kafs_v7_entrypoint_adapter_set_mountpoint(kafs_context_t *ctx, const char *mount_arg,
                                                      char *mnt_abs, size_t mnt_abs_size)
{
  ctx->c_mountpoint = mount_arg;
  if (!mount_arg || mount_arg[0] == '\0')
    return;

  if (mount_arg[0] == '/')
  {
    int n = snprintf(mnt_abs, mnt_abs_size, "%s", mount_arg);
    if (n >= 0 && (size_t)n < mnt_abs_size)
      ctx->c_mountpoint = mnt_abs;
    return;
  }

  char cwd[PATH_MAX];
  if (!getcwd(cwd, sizeof(cwd)))
    return;

  int n = snprintf(mnt_abs, mnt_abs_size, "%s/%s", cwd, mount_arg);
  if (n >= 0 && (size_t)n < mnt_abs_size)
    ctx->c_mountpoint = mnt_abs;
}

static void
kafs_v7_entrypoint_adapter_init_context(kafs_context_t *ctx,
                                        const kafs_v7_entrypoint_adapter_options_t *opts,
                                        const char *mount_arg, char *mnt_abs, size_t mnt_abs_size)
{
  memset(ctx, 0, sizeof(*ctx));
  kafs_v7_entrypoint_adapter_set_mountpoint(ctx, mount_arg, mnt_abs, mnt_abs_size);

  ctx->c_fd = -1;
  ctx->c_hotplug_fd = -1;
  ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_DISABLED;
  ctx->c_hotplug_wait_queue_limit = KAFS_V7_ENTRYPOINT_HOTPLUG_WAIT_QUEUE_LIMIT_DEFAULT;
  ctx->c_hotplug_wait_timeout_ms = KAFS_V7_ENTRYPOINT_HOTPLUG_WAIT_TIMEOUT_MS_DEFAULT;
  ctx->c_hotplug_data_mode = KAFS_RPC_DATA_INLINE;
  ctx->c_hotplug_front_major = KAFS_RPC_HELLO_MAJOR;
  ctx->c_hotplug_front_minor = KAFS_RPC_HELLO_MINOR;
  ctx->c_hotplug_front_features = KAFS_RPC_HELLO_FEATURES;
  ctx->c_hotplug_compat_result = KAFS_HOTPLUG_COMPAT_UNKNOWN;

  ctx->c_pending_worker_prio_mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
  ctx->c_pending_worker_prio_base_mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
  ctx->c_pending_worker_prio_dirty = 1;
  ctx->c_pending_ttl_soft_ms = KAFS_V7_ENTRYPOINT_PENDING_TTL_SOFT_MS_DEFAULT;
  ctx->c_pending_ttl_hard_ms = KAFS_V7_ENTRYPOINT_PENDING_TTL_HARD_MS_DEFAULT;
  ctx->c_tombstone_gc_cursor = KAFS_INO_ROOTDIR + 1u;

  ctx->c_bg_dedup_enabled = opts && opts->bg_dedup_scan_enabled ? 1u : 0u;
  ctx->c_bg_dedup_interval_ms = KAFS_V7_ENTRYPOINT_BG_DEDUP_INTERVAL_MS_DEFAULT;
  ctx->c_bg_dedup_quiet_interval_ms = KAFS_V7_ENTRYPOINT_BG_DEDUP_QUIET_INTERVAL_MS_DEFAULT;
  ctx->c_bg_dedup_pressure_interval_ms = KAFS_V7_ENTRYPOINT_BG_DEDUP_PRESSURE_INTERVAL_MS_DEFAULT;
  ctx->c_bg_dedup_start_used_pct = KAFS_V7_ENTRYPOINT_BG_DEDUP_START_USED_PCT_DEFAULT;
  ctx->c_bg_dedup_pressure_used_pct = KAFS_V7_ENTRYPOINT_BG_DEDUP_PRESSURE_USED_PCT_DEFAULT;
  ctx->c_bg_dedup_worker_prio_mode = KAFS_PENDING_WORKER_PRIO_IDLE;
  ctx->c_bg_dedup_worker_nice = 19;
  ctx->c_bg_dedup_worker_prio_base_mode = KAFS_PENDING_WORKER_PRIO_IDLE;
  ctx->c_bg_dedup_worker_nice_base = 19;
  ctx->c_bg_dedup_worker_prio_dirty = 1;
  ctx->c_bg_dedup_mode = KAFS_V7_ENTRYPOINT_BG_DEDUP_MODE_COLD;

  ctx->c_fsync_policy = opts ? opts->fsync_policy : KAFS_FSYNC_POLICY_JOURNAL_ONLY;
  ctx->c_sd_card_profile = KAFS_SD_CARD_PROFILE_NONE;
  ctx->c_atime_policy = KAFS_ATIME_POLICY_NO_RUNTIME_UPDATES;
}

static int kafs_v7_entrypoint_adapter_lock_runtime_image(kafs_context_t *ctx,
                                                         const char *image_path)
{
  struct flock lk = {0};
  lk.l_type = ctx && ctx->c_runtime_read_only ? F_RDLCK : F_WRLCK;
  lk.l_whence = SEEK_SET;
  lk.l_start = 0;
  lk.l_len = 0;
  if (!ctx || ctx->c_fd < 0 || fcntl(ctx->c_fd, F_SETLK, &lk) == -1)
  {
    perror("fcntl(F_SETLK)");
    fprintf(stderr, "image '%s' is busy (already mounted?).\n", image_path);
    return 2;
  }
  return 0;
}

static int kafs_v7_entrypoint_adapter_has_single_arg(char **argv_clean, int argc_clean)
{
  for (int i = 0; i < argc_clean; ++i)
  {
    if (strcmp(argv_clean[i], "-s") == 0)
      return 1;
  }
  return 0;
}

static void kafs_v7_entrypoint_adapter_apply_fuse_single_arg(kafs_v7_mount_thread_options_t *thread,
                                                             int saw_single, char **argv_fuse,
                                                             int *argc_fuse)
{
  if (!thread->enable_mt && !saw_single)
    argv_fuse[(*argc_fuse)++] = "-s";
  if (thread->enable_mt && saw_single)
    thread->enable_mt = KAFS_FALSE;
}

static void kafs_v7_entrypoint_adapter_apply_fuse_debug_arg(char **argv_fuse, int *argc_fuse)
{
  if (kafs_debug_level() >= 3)
    argv_fuse[(*argc_fuse)++] = "-d";
}

static void
kafs_v7_entrypoint_adapter_apply_fuse_mt_arg(const kafs_v7_mount_thread_options_t *thread,
                                             char **argv_fuse, int *argc_fuse, char *mt_opt_buf,
                                             size_t mt_opt_buf_size)
{
  if (!thread->enable_mt || thread->saw_max_threads)
    return;

  unsigned mt_cnt = kafs_v7_mount_options_thread_count(thread);
  snprintf(mt_opt_buf, mt_opt_buf_size, "max_threads=%u", mt_cnt);
  argv_fuse[(*argc_fuse)++] = "-o";
  argv_fuse[(*argc_fuse)++] = mt_opt_buf;
  kafs_log(KAFS_LOG_INFO, "kafs: enabling multithread with -o %s\n", mt_opt_buf);
}

static int kafs_v7_entrypoint_adapter_append_readonly_arg(const kafs_context_t *ctx,
                                                          char **argv_fuse, int argc_fuse)
{
  if (!ctx || !ctx->c_runtime_read_only)
    return argc_fuse;
  argv_fuse[argc_fuse++] = "-o";
  argv_fuse[argc_fuse++] = "ro";
  argv_fuse[argc_fuse] = NULL;
  return argc_fuse;
}

static void kafs_v7_entrypoint_adapter_build_fuse_argv(char **argv_clean, int argc_clean,
                                                       kafs_v7_mount_thread_options_t *thread,
                                                       char **argv_fuse, int *argc_fuse,
                                                       char *mt_opt_buf, size_t mt_opt_buf_size)
{
  memcpy(argv_fuse, argv_clean, sizeof(argv_fuse[0]) * (size_t)argc_clean);
  int saw_single = kafs_v7_entrypoint_adapter_has_single_arg(argv_clean, argc_clean);
  *argc_fuse = argc_clean;
  kafs_v7_entrypoint_adapter_apply_fuse_single_arg(thread, saw_single, argv_fuse, argc_fuse);
  kafs_v7_entrypoint_adapter_apply_fuse_debug_arg(argv_fuse, argc_fuse);
  kafs_v7_entrypoint_adapter_apply_fuse_mt_arg(thread, argv_fuse, argc_fuse, mt_opt_buf,
                                               mt_opt_buf_size);
  argv_fuse[*argc_fuse] = NULL;
}

static void kafs_v7_entrypoint_adapter_shared_options_from_mount(
    kafs_shared_fuse_runtime_options_t *out, const kafs_context_t *ctx,
    const kafs_v7_entrypoint_adapter_options_t *opts)
{
  memset(out, 0, sizeof(*out));
  out->writeback_cache_enabled = opts->writeback_cache_enabled ? KAFS_TRUE : KAFS_FALSE;
  out->writeback_cache_explicit =
      (opts->writeback_cache_explicit || opts->no_writeback_cache_requested) ? KAFS_TRUE
                                                                             : KAFS_FALSE;
  out->trim_on_free_enabled = opts->trim_on_free_enabled ? KAFS_TRUE : KAFS_FALSE;
  out->trim_on_free_explicit =
      (opts->trim_on_free_explicit || opts->no_trim_on_free_requested) ? KAFS_TRUE : KAFS_FALSE;

  if (ctx && ctx->c_runtime_read_only)
  {
    out->writeback_cache_enabled = KAFS_FALSE;
    out->writeback_cache_explicit = KAFS_TRUE;
    out->trim_on_free_enabled = KAFS_FALSE;
    out->trim_on_free_explicit = KAFS_TRUE;
  }
}

int kafs_v7_entrypoint_adapter_validate_options(const kafs_v7_entrypoint_adapter_options_t *opts,
                                                FILE *err)
{
  kafs_v7_runtime_request_t req;
  kafs_v7_entrypoint_adapter_request_from_options(&req, opts);
  return kafs_v7_runtime_report_entrypoint_request(&req, err);
}

int kafs_v7_entrypoint_adapter_open_context(kafs_context_t *ctx, const char *image_path,
                                            kafs_v7_entrypoint_adapter_mode_t mode, FILE *err)
{
  kafs_v7_runtime_mode_t runtime_mode = kafs_v7_entrypoint_adapter_runtime_mode(mode);
  kafs_ssuperblock_t sbdisk;
  if (kafs_v7_runtime_open_context_image(ctx, image_path, runtime_mode, &sbdisk, err) != 0)
    return 2;
  ctx->c_runtime_read_only = runtime_mode == KAFS_V7_RUNTIME_MODE_INSPECTION ? 1u : 0u;
  if (kafs_v7_entrypoint_adapter_lock_runtime_image(ctx, image_path) != 0)
  {
    kafs_v7_entrypoint_adapter_close_context_fd(ctx);
    return 2;
  }

  kafs_inocnt_t inocnt = 0;
  kafs_blkcnt_t r_blkcnt = 0;
  int rc = kafs_v7_runtime_admit_mount_context(ctx, &sbdisk, runtime_mode, &inocnt, &r_blkcnt, err);
  if (rc != 0)
  {
    kafs_v7_entrypoint_adapter_close_context_fd(ctx);
    return 2;
  }

  rc = kafs_v7_runtime_init_mount_services(ctx, image_path, runtime_mode, inocnt, r_blkcnt, err);
  if (rc != 0)
  {
    kafs_ctx_unmap_image(ctx);
    kafs_v7_entrypoint_adapter_close_context_fd(ctx);
    return 2;
  }

  return 0;
}

int kafs_v7_entrypoint_adapter_mount_main(const char *image_path, const char *mountpoint,
                                          int argc_extra, char **argv_extra,
                                          const kafs_v7_entrypoint_adapter_options_t *opts,
                                          FILE *err)
{
  if (!err)
    err = stderr;
  if (!image_path || !mountpoint || argc_extra < 0 || !opts)
  {
    fprintf(err, "%s mount requires an image path, mountpoint, and adapter options.\n",
            KAFS_V7_TOOL_NAME);
    return 2;
  }
  if (kafs_v7_entrypoint_adapter_validate_options(opts, err) != 0)
    return 2;

  char *argv_clean[argc_extra + 3];
  char *owned[argc_extra + 3];
  int argc_clean = 0;
  int owned_count = 0;
  kafs_v7_mount_thread_options_t thread = {0};
  if (kafs_v7_mount_options_filter_fuse_args(mountpoint, argc_extra, argv_extra, argv_clean,
                                             &argc_clean, owned, &owned_count, &thread, err) != 0)
  {
    kafs_v7_mount_options_free_owned(owned, owned_count);
    return 2;
  }

  static kafs_context_t ctx;
  static char mnt_abs[PATH_MAX];
  kafs_v7_entrypoint_adapter_init_context(&ctx, opts, argv_clean[1], mnt_abs, sizeof(mnt_abs));

  if (kafs_v7_entrypoint_adapter_open_context(&ctx, image_path, opts->mode, err) != 0)
  {
    kafs_v7_mount_options_free_owned(owned, owned_count);
    return 2;
  }
  char *argv_fuse[argc_clean + 10];
  char mt_opt_buf[64];
  int argc_fuse = 0;
  kafs_v7_entrypoint_adapter_build_fuse_argv(argv_clean, argc_clean, &thread, argv_fuse, &argc_fuse,
                                             mt_opt_buf, sizeof(mt_opt_buf));
  argc_fuse = kafs_v7_entrypoint_adapter_append_readonly_arg(&ctx, argv_fuse, argc_fuse);

  kafs_shared_fuse_runtime_options_t fuse_opts;
  kafs_v7_entrypoint_adapter_shared_options_from_mount(&fuse_opts, &ctx, opts);
  kafs_shared_fuse_run_request_t fuse_request = {
      .ctx = &ctx,
      .argc_fuse = argc_fuse,
      .argv_fuse = argv_fuse,
      .hotplug_uds_path = NULL,
      .runtime_options = &fuse_opts,
  };
  int rc = kafs_shared_fuse_run_request(&fuse_request);
  kafs_v7_entrypoint_adapter_close_context_fd(&ctx);
  kafs_v7_mount_options_free_owned(owned, owned_count);
  return rc;
}
