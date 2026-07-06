#include "kafs_v6_entrypoint_adapter.h"

#include "kafs_inode.h"
#include "kafs_rpc.h"
#include "kafs_v6_runtime.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define KAFS_V6_ENTRYPOINT_PENDING_TTL_SOFT_MS_DEFAULT 5000u
#define KAFS_V6_ENTRYPOINT_PENDING_TTL_HARD_MS_DEFAULT 30000u
#define KAFS_V6_ENTRYPOINT_HOTPLUG_WAIT_TIMEOUT_MS_DEFAULT 2000u
#define KAFS_V6_ENTRYPOINT_HOTPLUG_WAIT_QUEUE_LIMIT_DEFAULT 64u
#define KAFS_V6_ENTRYPOINT_BG_DEDUP_INTERVAL_MS_DEFAULT 2000u
#define KAFS_V6_ENTRYPOINT_BG_DEDUP_QUIET_INTERVAL_MS_DEFAULT 5000u
#define KAFS_V6_ENTRYPOINT_BG_DEDUP_PRESSURE_INTERVAL_MS_DEFAULT 100u
#define KAFS_V6_ENTRYPOINT_BG_DEDUP_START_USED_PCT_DEFAULT 85u
#define KAFS_V6_ENTRYPOINT_BG_DEDUP_PRESSURE_USED_PCT_DEFAULT 95u
#define KAFS_V6_ENTRYPOINT_BG_DEDUP_MODE_COLD 1u

typedef struct kafs_v6_entrypoint_adapter_mount_filter
{
  kafs_bool_t enable_mt;
  int saw_max_threads;
  unsigned mt_cnt_override;
  int mt_cnt_override_set;
} kafs_v6_entrypoint_adapter_mount_filter_t;

static kafs_v6_runtime_mode_t
kafs_v6_entrypoint_adapter_runtime_mode(kafs_v6_entrypoint_adapter_mode_t mode)
{
  switch (mode)
  {
  case KAFS_V6_ENTRYPOINT_ADAPTER_MODE_INSPECTION:
    return KAFS_V6_RUNTIME_MODE_INSPECTION;
  case KAFS_V6_ENTRYPOINT_ADAPTER_MODE_CONTROLLED_WRITE:
    return KAFS_V6_RUNTIME_MODE_CONTROLLED_WRITE;
  case KAFS_V6_ENTRYPOINT_ADAPTER_MODE_NONE:
  default:
    return KAFS_V6_RUNTIME_MODE_NONE;
  }
}

static void kafs_v6_entrypoint_adapter_close_context_fd(kafs_context_t *ctx)
{
  if (!ctx)
    return;
  if (ctx->c_fd >= 0)
    close(ctx->c_fd);
  ctx->c_fd = -1;
}

static void
kafs_v6_entrypoint_adapter_request_from_options(kafs_v6_runtime_request_t *req,
                                                const kafs_v6_entrypoint_adapter_options_t *opts)
{
  kafs_v6_runtime_request_init(req);
  if (!opts)
    return;

  req->mode = kafs_v6_entrypoint_adapter_runtime_mode(opts->mode);
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

static int kafs_v6_entrypoint_adapter_starts_with(const char *s, const char *prefix)
{
  size_t len = strlen(prefix);
  return strncmp(s, prefix, len) == 0;
}

static int
kafs_v6_entrypoint_adapter_parse_mt_token(const char *tok,
                                          kafs_v6_entrypoint_adapter_mount_filter_t *filter)
{
  if (kafs_v6_entrypoint_adapter_starts_with(tok, "max_threads=") ||
      strcmp(tok, "max_threads") == 0)
  {
    filter->saw_max_threads = 1;
    return 0;
  }

  if (strcmp(tok, "multi_thread") == 0 || strcmp(tok, "multi-thread") == 0 ||
      strcmp(tok, "multithread") == 0)
  {
    filter->enable_mt = KAFS_TRUE;
    return 1;
  }

  const char *vstr = NULL;
  if (kafs_v6_entrypoint_adapter_starts_with(tok, "multi_thread="))
    vstr = tok + strlen("multi_thread=");
  else if (kafs_v6_entrypoint_adapter_starts_with(tok, "multi-thread="))
    vstr = tok + strlen("multi-thread=");
  else if (kafs_v6_entrypoint_adapter_starts_with(tok, "multithread="))
    vstr = tok + strlen("multithread=");
  if (!vstr)
    return 0;

  char *endp = NULL;
  unsigned long value = strtoul(vstr, &endp, 10);
  if (!endp || *endp != '\0')
  {
    fprintf(stderr, "invalid -o multi_thread=N: '%s'\n", vstr);
    return 2;
  }
  if (value < 1)
    value = 1;
  if (value > 100000)
    value = 100000;
  filter->enable_mt = KAFS_TRUE;
  filter->mt_cnt_override = (unsigned)value;
  filter->mt_cnt_override_set = 1;
  return 1;
}

static int kafs_v6_entrypoint_adapter_is_internal_mount_token(
    const char *tok, kafs_v6_entrypoint_adapter_mount_filter_t *filter)
{
  int rc = kafs_v6_entrypoint_adapter_parse_mt_token(tok, filter);
  if (rc != 0)
    return rc;

  if (strcmp(tok, "no_writeback_cache") == 0 || strcmp(tok, "no-writeback-cache") == 0 ||
      strcmp(tok, "writeback_cache") == 0 || strcmp(tok, "writeback-cache") == 0 ||
      strcmp(tok, "no_trim_on_free") == 0 || strcmp(tok, "no-trim-on-free") == 0 ||
      strcmp(tok, "trim_on_free") == 0 || strcmp(tok, "trim-on-free") == 0 ||
      strcmp(tok, "hotplug") == 0 || strcmp(tok, "sd_card_profile") == 0 ||
      strcmp(tok, "sd-card-profile") == 0 || strcmp(tok, "v6_inspection_mount") == 0 ||
      strcmp(tok, "v6-inspection-mount") == 0 || strcmp(tok, "v6_write_mount") == 0 ||
      strcmp(tok, "v6-write-mount") == 0 || strcmp(tok, "bg_dedup_scan") == 0 ||
      strcmp(tok, "dedup_scan") == 0 || strcmp(tok, "no_bg_dedup_scan") == 0 ||
      strcmp(tok, "no-bg-dedup-scan") == 0 || strcmp(tok, "no_dedup_scan") == 0 ||
      strcmp(tok, "no-dedup-scan") == 0)
    return 1;

  static const char *const prefixes[] = {
      "hotplug=",
      "hotplug_uds=",
      "hotplug-uds=",
      "hotplug_back_bin=",
      "hotplug-back-bin=",
      "sd_card_profile=",
      "sd-card-profile=",
      "pending_worker_prio=",
      "dedup_worker_prio=",
      "pending_worker_nice=",
      "dedup_worker_nice=",
      "pending_ttl_soft_ms=",
      "pending_ttl_hard_ms=",
      "pendinglog_cap_initial=",
      "pending_cap_initial=",
      "pendinglog_cap_min=",
      "pending_cap_min=",
      "pendinglog_cap_max=",
      "pending_cap_max=",
      "fsync_policy=",
      "bg_dedup_scan=",
      "dedup_scan=",
      "bg_dedup_interval_ms=",
      "dedup_interval_ms=",
      "bg_dedup_quiet_interval_ms=",
      "dedup_quiet_interval_ms=",
      "bg_dedup_pressure_interval_ms=",
      "dedup_pressure_interval_ms=",
      "bg_dedup_start_used_pct=",
      "dedup_start_used_pct=",
      "bg_dedup_pressure_used_pct=",
      "dedup_pressure_used_pct=",
      "bg_dedup_worker_prio=",
      "dedup_scan_worker_prio=",
      "bg_dedup_worker_nice=",
      "dedup_scan_worker_nice=",
  };
  for (size_t i = 0; i < sizeof(prefixes) / sizeof(prefixes[0]); ++i)
  {
    if (kafs_v6_entrypoint_adapter_starts_with(tok, prefixes[i]))
      return 1;
  }

  return 0;
}

static int kafs_v6_entrypoint_adapter_append_filtered_token(char *filtered, size_t filtered_size,
                                                            size_t *used, const char *tok)
{
  size_t tok_len = strlen(tok);
  size_t extra = tok_len + (*used ? 1u : 0u);
  if (extra >= filtered_size || *used > filtered_size - 1u - extra)
  {
    fprintf(stderr, "kafs-v6: filtered FUSE option list is too long.\n");
    return 2;
  }
  if (*used)
    filtered[(*used)++] = ',';
  memcpy(filtered + *used, tok, tok_len);
  *used += tok_len;
  filtered[*used] = '\0';
  return 0;
}

static int
kafs_v6_entrypoint_adapter_filter_o_list(const char *oval, char *filtered, size_t filtered_size,
                                         kafs_v6_entrypoint_adapter_mount_filter_t *filter)
{
  char *dup = strdup(oval);
  if (!dup)
  {
    perror("strdup");
    return 2;
  }

  filtered[0] = '\0';
  size_t used = 0;
  char *saveptr = NULL;
  for (char *tok = strtok_r(dup, ",", &saveptr); tok; tok = strtok_r(NULL, ",", &saveptr))
  {
    while (*tok == ' ' || *tok == '\t')
      tok++;
    if (*tok == '\0')
    {
      fprintf(stderr, "kafs-v6: empty token in -o option list.\n");
      free(dup);
      return 2;
    }

    int internal = kafs_v6_entrypoint_adapter_is_internal_mount_token(tok, filter);
    if (internal == 2)
    {
      free(dup);
      return 2;
    }
    if (internal == 1)
      continue;

    if (kafs_v6_entrypoint_adapter_append_filtered_token(filtered, filtered_size, &used, tok) != 0)
    {
      free(dup);
      return 2;
    }
  }

  free(dup);
  return 0;
}

static int kafs_v6_entrypoint_adapter_append_o_arg(char **argv_clean, int *argc_clean, char **owned,
                                                   int *owned_count, const char *filtered,
                                                   int compact)
{
  char *kept = NULL;
  if (compact)
  {
    kept = (char *)malloc(strlen(filtered) + 3u);
    if (!kept)
    {
      perror("malloc");
      return 2;
    }
    kept[0] = '-';
    kept[1] = 'o';
    strcpy(kept + 2, filtered);
    argv_clean[(*argc_clean)++] = kept;
  }
  else
  {
    kept = strdup(filtered);
    if (!kept)
    {
      perror("strdup");
      return 2;
    }
    argv_clean[(*argc_clean)++] = "-o";
    argv_clean[(*argc_clean)++] = kept;
  }

  owned[(*owned_count)++] = kept;
  return 0;
}

static int kafs_v6_entrypoint_adapter_filter_fuse_args(
    const char *mountpoint, int argc_extra, char **argv_extra, char **argv_clean, int *argc_clean,
    char **owned, int *owned_count, kafs_v6_entrypoint_adapter_mount_filter_t *filter)
{
  *argc_clean = 0;
  *owned_count = 0;
  argv_clean[(*argc_clean)++] = "kafs-v6";
  argv_clean[(*argc_clean)++] = (char *)mountpoint;

  for (int i = 0; i < argc_extra; ++i)
  {
    char *arg = argv_extra[i];
    const char *oval = NULL;
    int compact = 0;
    if (strcmp(arg, "-o") == 0)
    {
      if (i + 1 >= argc_extra)
      {
        fprintf(stderr, "kafs-v6: -o requires an option list.\n");
        return 2;
      }
      oval = argv_extra[++i];
    }
    else if (strncmp(arg, "-o", 2) == 0 && arg[2] != '\0')
    {
      oval = arg + 2;
      compact = 1;
    }

    if (!oval)
    {
      argv_clean[(*argc_clean)++] = arg;
      continue;
    }

    char filtered[strlen(oval) + 1u];
    if (kafs_v6_entrypoint_adapter_filter_o_list(oval, filtered, sizeof(filtered), filter) != 0)
      return 2;
    if (filtered[0] != '\0' &&
        kafs_v6_entrypoint_adapter_append_o_arg(argv_clean, argc_clean, owned, owned_count,
                                                filtered, compact) != 0)
      return 2;
  }
  return 0;
}

static void kafs_v6_entrypoint_adapter_free_owned(char **owned, int owned_count)
{
  for (int i = 0; i < owned_count; ++i)
    free(owned[i]);
}

static void kafs_v6_entrypoint_adapter_set_mountpoint(kafs_context_t *ctx, const char *mount_arg,
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
kafs_v6_entrypoint_adapter_init_context(kafs_context_t *ctx,
                                        const kafs_v6_entrypoint_adapter_options_t *opts,
                                        const char *mount_arg, char *mnt_abs, size_t mnt_abs_size)
{
  memset(ctx, 0, sizeof(*ctx));
  kafs_v6_entrypoint_adapter_set_mountpoint(ctx, mount_arg, mnt_abs, mnt_abs_size);

  ctx->c_fd = -1;
  ctx->c_hotplug_fd = -1;
  ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_DISABLED;
  ctx->c_hotplug_wait_queue_limit = KAFS_V6_ENTRYPOINT_HOTPLUG_WAIT_QUEUE_LIMIT_DEFAULT;
  ctx->c_hotplug_wait_timeout_ms = KAFS_V6_ENTRYPOINT_HOTPLUG_WAIT_TIMEOUT_MS_DEFAULT;
  ctx->c_hotplug_data_mode = KAFS_RPC_DATA_INLINE;
  ctx->c_hotplug_front_major = KAFS_RPC_HELLO_MAJOR;
  ctx->c_hotplug_front_minor = KAFS_RPC_HELLO_MINOR;
  ctx->c_hotplug_front_features = KAFS_RPC_HELLO_FEATURES;
  ctx->c_hotplug_compat_result = KAFS_HOTPLUG_COMPAT_UNKNOWN;

  ctx->c_pending_worker_prio_mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
  ctx->c_pending_worker_prio_base_mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
  ctx->c_pending_worker_prio_dirty = 1;
  ctx->c_pending_ttl_soft_ms = KAFS_V6_ENTRYPOINT_PENDING_TTL_SOFT_MS_DEFAULT;
  ctx->c_pending_ttl_hard_ms = KAFS_V6_ENTRYPOINT_PENDING_TTL_HARD_MS_DEFAULT;
  ctx->c_tombstone_gc_cursor = KAFS_INO_ROOTDIR + 1u;

  ctx->c_bg_dedup_enabled = opts && opts->bg_dedup_scan_enabled ? 1u : 0u;
  ctx->c_bg_dedup_interval_ms = KAFS_V6_ENTRYPOINT_BG_DEDUP_INTERVAL_MS_DEFAULT;
  ctx->c_bg_dedup_quiet_interval_ms = KAFS_V6_ENTRYPOINT_BG_DEDUP_QUIET_INTERVAL_MS_DEFAULT;
  ctx->c_bg_dedup_pressure_interval_ms = KAFS_V6_ENTRYPOINT_BG_DEDUP_PRESSURE_INTERVAL_MS_DEFAULT;
  ctx->c_bg_dedup_start_used_pct = KAFS_V6_ENTRYPOINT_BG_DEDUP_START_USED_PCT_DEFAULT;
  ctx->c_bg_dedup_pressure_used_pct = KAFS_V6_ENTRYPOINT_BG_DEDUP_PRESSURE_USED_PCT_DEFAULT;
  ctx->c_bg_dedup_worker_prio_mode = KAFS_PENDING_WORKER_PRIO_IDLE;
  ctx->c_bg_dedup_worker_nice = 19;
  ctx->c_bg_dedup_worker_prio_base_mode = KAFS_PENDING_WORKER_PRIO_IDLE;
  ctx->c_bg_dedup_worker_nice_base = 19;
  ctx->c_bg_dedup_worker_prio_dirty = 1;
  ctx->c_bg_dedup_mode = KAFS_V6_ENTRYPOINT_BG_DEDUP_MODE_COLD;

  ctx->c_fsync_policy = opts ? opts->fsync_policy : KAFS_FSYNC_POLICY_JOURNAL_ONLY;
  ctx->c_sd_card_profile = KAFS_SD_CARD_PROFILE_NONE;
  ctx->c_atime_policy = KAFS_ATIME_POLICY_NO_RUNTIME_UPDATES;
}

static int kafs_v6_entrypoint_adapter_lock_runtime_image(kafs_context_t *ctx,
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

static int kafs_v6_entrypoint_adapter_has_single_arg(char **argv_clean, int argc_clean)
{
  for (int i = 0; i < argc_clean; ++i)
  {
    if (strcmp(argv_clean[i], "-s") == 0)
      return 1;
  }
  return 0;
}

static unsigned
kafs_v6_entrypoint_adapter_mt_thread_count(const kafs_v6_entrypoint_adapter_mount_filter_t *filter)
{
  unsigned mt_cnt = 8;
  if (filter->mt_cnt_override_set)
    mt_cnt = filter->mt_cnt_override;
  else
  {
    const char *mt_env = getenv("KAFS_MAX_THREADS");
    if (mt_env && *mt_env)
    {
      char *endp = NULL;
      unsigned long parsed = strtoul(mt_env, &endp, 10);
      if (endp && *endp == '\0')
        mt_cnt = (unsigned)parsed;
    }
    if (mt_cnt < 1)
      mt_cnt = 1;
    if (mt_cnt > 100000)
      mt_cnt = 100000;
  }
  return mt_cnt;
}

static void
kafs_v6_entrypoint_adapter_apply_fuse_single_arg(kafs_v6_entrypoint_adapter_mount_filter_t *filter,
                                                 int saw_single, char **argv_fuse, int *argc_fuse)
{
  if (!filter->enable_mt && !saw_single)
    argv_fuse[(*argc_fuse)++] = "-s";
  if (filter->enable_mt && saw_single)
    filter->enable_mt = KAFS_FALSE;
}

static void kafs_v6_entrypoint_adapter_apply_fuse_debug_arg(char **argv_fuse, int *argc_fuse)
{
  if (kafs_debug_level() >= 3)
    argv_fuse[(*argc_fuse)++] = "-d";
}

static void kafs_v6_entrypoint_adapter_apply_fuse_mt_arg(
    const kafs_v6_entrypoint_adapter_mount_filter_t *filter, char **argv_fuse, int *argc_fuse,
    char *mt_opt_buf, size_t mt_opt_buf_size)
{
  if (!filter->enable_mt || filter->saw_max_threads)
    return;

  unsigned mt_cnt = kafs_v6_entrypoint_adapter_mt_thread_count(filter);
  snprintf(mt_opt_buf, mt_opt_buf_size, "max_threads=%u", mt_cnt);
  argv_fuse[(*argc_fuse)++] = "-o";
  argv_fuse[(*argc_fuse)++] = mt_opt_buf;
  kafs_log(KAFS_LOG_INFO, "kafs: enabling multithread with -o %s\n", mt_opt_buf);
}

static int kafs_v6_entrypoint_adapter_append_readonly_arg(const kafs_context_t *ctx,
                                                          char **argv_fuse, int argc_fuse)
{
  if (!ctx || !ctx->c_runtime_read_only)
    return argc_fuse;
  argv_fuse[argc_fuse++] = "-o";
  argv_fuse[argc_fuse++] = "ro";
  argv_fuse[argc_fuse] = NULL;
  return argc_fuse;
}

static void kafs_v6_entrypoint_adapter_build_fuse_argv(
    char **argv_clean, int argc_clean, kafs_v6_entrypoint_adapter_mount_filter_t *filter,
    char **argv_fuse, int *argc_fuse, char *mt_opt_buf, size_t mt_opt_buf_size)
{
  memcpy(argv_fuse, argv_clean, sizeof(argv_fuse[0]) * (size_t)argc_clean);
  int saw_single = kafs_v6_entrypoint_adapter_has_single_arg(argv_clean, argc_clean);
  *argc_fuse = argc_clean;
  kafs_v6_entrypoint_adapter_apply_fuse_single_arg(filter, saw_single, argv_fuse, argc_fuse);
  kafs_v6_entrypoint_adapter_apply_fuse_debug_arg(argv_fuse, argc_fuse);
  kafs_v6_entrypoint_adapter_apply_fuse_mt_arg(filter, argv_fuse, argc_fuse, mt_opt_buf,
                                               mt_opt_buf_size);
  argv_fuse[*argc_fuse] = NULL;
}

static void
kafs_v6_entrypoint_adapter_fuse_options_from_mount(kafs_v6_entrypoint_adapter_fuse_options_t *out,
                                                   const kafs_context_t *ctx,
                                                   const kafs_v6_entrypoint_adapter_options_t *opts)
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

int kafs_v6_entrypoint_adapter_validate_options(const kafs_v6_entrypoint_adapter_options_t *opts,
                                                FILE *err)
{
  kafs_v6_runtime_request_t req;
  kafs_v6_entrypoint_adapter_request_from_options(&req, opts);
  return kafs_v6_runtime_report_entrypoint_request(&req, err);
}

int kafs_v6_entrypoint_adapter_open_context(kafs_context_t *ctx, const char *image_path,
                                            kafs_v6_entrypoint_adapter_mode_t mode, FILE *err)
{
  kafs_v6_runtime_mode_t runtime_mode = kafs_v6_entrypoint_adapter_runtime_mode(mode);
  kafs_ssuperblock_t sbdisk;
  if (kafs_v6_runtime_open_context_image(ctx, image_path, runtime_mode, &sbdisk, err) != 0)
    return 2;

  kafs_inocnt_t inocnt = 0;
  kafs_blkcnt_t r_blkcnt = 0;
  int rc = kafs_v6_runtime_admit_mount_context(ctx, &sbdisk, runtime_mode, &inocnt, &r_blkcnt, err);
  if (rc != 0)
  {
    kafs_v6_entrypoint_adapter_close_context_fd(ctx);
    return 2;
  }

  rc = kafs_v6_runtime_init_mount_services(ctx, image_path, runtime_mode, inocnt, r_blkcnt, err);
  if (rc != 0)
  {
    kafs_ctx_unmap_image(ctx);
    kafs_v6_entrypoint_adapter_close_context_fd(ctx);
    return 2;
  }

  return 0;
}

int kafs_v6_entrypoint_adapter_mount_main(const char *image_path, const char *mountpoint,
                                          int argc_extra, char **argv_extra,
                                          const kafs_v6_entrypoint_adapter_options_t *opts,
                                          FILE *err)
{
  if (!err)
    err = stderr;
  if (!image_path || !mountpoint || argc_extra < 0 || !opts)
  {
    fprintf(err, "kafs-v6 mount requires an image path, mountpoint, and adapter options.\n");
    return 2;
  }
  if (kafs_v6_entrypoint_adapter_validate_options(opts, err) != 0)
    return 2;

  char *argv_clean[argc_extra + 3];
  char *owned[argc_extra + 3];
  int argc_clean = 0;
  int owned_count = 0;
  kafs_v6_entrypoint_adapter_mount_filter_t filter = {0};
  if (kafs_v6_entrypoint_adapter_filter_fuse_args(mountpoint, argc_extra, argv_extra, argv_clean,
                                                  &argc_clean, owned, &owned_count, &filter) != 0)
  {
    kafs_v6_entrypoint_adapter_free_owned(owned, owned_count);
    return 2;
  }

  static kafs_context_t ctx;
  static char mnt_abs[PATH_MAX];
  kafs_v6_entrypoint_adapter_init_context(&ctx, opts, argv_clean[1], mnt_abs, sizeof(mnt_abs));

  if (kafs_v6_entrypoint_adapter_open_context(&ctx, image_path, opts->mode, err) != 0)
  {
    kafs_v6_entrypoint_adapter_free_owned(owned, owned_count);
    return 2;
  }
  if (kafs_v6_entrypoint_adapter_lock_runtime_image(&ctx, image_path) != 0)
  {
    kafs_ctx_unmap_image(&ctx);
    kafs_v6_entrypoint_adapter_close_context_fd(&ctx);
    kafs_v6_entrypoint_adapter_free_owned(owned, owned_count);
    return 2;
  }

  char *argv_fuse[argc_clean + 10];
  char mt_opt_buf[64];
  int argc_fuse = 0;
  kafs_v6_entrypoint_adapter_build_fuse_argv(argv_clean, argc_clean, &filter, argv_fuse, &argc_fuse,
                                             mt_opt_buf, sizeof(mt_opt_buf));
  argc_fuse = kafs_v6_entrypoint_adapter_append_readonly_arg(&ctx, argv_fuse, argc_fuse);

  kafs_v6_entrypoint_adapter_fuse_options_t fuse_opts;
  kafs_v6_entrypoint_adapter_fuse_options_from_mount(&fuse_opts, &ctx, opts);
  int rc = kafs_v6_entrypoint_adapter_run_shared_fuse(&ctx, argc_fuse, argv_fuse, &fuse_opts);
  kafs_v6_entrypoint_adapter_free_owned(owned, owned_count);
  return rc;
}
