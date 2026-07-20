#include "kafs_v6_mount_options.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

static int kafs_v6_mount_options_record_mode_token(kafs_v6_runtime_request_t *req, const char *tok)
{
  if (strcmp(tok, "ro") == 0)
  {
    req->mount_read_only_requested = 1;
    req->mount_read_only_seen = 1;
    return 1;
  }
  if (strcmp(tok, "rw") == 0)
  {
    req->mount_read_write_requested = 1;
    return 1;
  }
  return 0;
}

static int kafs_v6_mount_options_record_cache_token(kafs_v6_runtime_request_t *req, const char *tok)
{
  static const char *const no_writeback[] = {"no_writeback_cache", "no-writeback-cache"};
  static const char *const writeback[] = {"writeback_cache", "writeback-cache"};
  static const char *const no_trim[] = {"no_trim_on_free", "no-trim-on-free"};
  static const char *const trim[] = {"trim_on_free", "trim-on-free"};

  if (kafs_mount_options_eq_any(tok, no_writeback, KAFS_MOUNT_OPTIONS_ARRAY_COUNT(no_writeback)))
  {
    req->no_writeback_cache_requested = 1;
    return 1;
  }
  if (kafs_mount_options_eq_any(tok, writeback, KAFS_MOUNT_OPTIONS_ARRAY_COUNT(writeback)))
  {
    req->writeback_cache_enabled = 1;
    req->writeback_cache_explicit = 1;
    return 1;
  }
  if (kafs_mount_options_eq_any(tok, no_trim, KAFS_MOUNT_OPTIONS_ARRAY_COUNT(no_trim)))
  {
    req->no_trim_on_free_requested = 1;
    return 1;
  }
  if (kafs_mount_options_eq_any(tok, trim, KAFS_MOUNT_OPTIONS_ARRAY_COUNT(trim)))
  {
    req->trim_on_free_enabled = 1;
    return 1;
  }
  return 0;
}

static int kafs_v6_mount_options_record_bg_dedup_token(kafs_v6_runtime_request_t *req,
                                                       const char *tok)
{
  static const char *const off_tokens[] = {
      "bg_dedup_scan=off", "dedup_scan=off", "no_bg_dedup_scan",
      "no-bg-dedup-scan",  "no_dedup_scan",  "no-dedup-scan",
  };
  static const char *const on_tokens[] = {
      "bg_dedup_scan",
      "dedup_scan",
      "bg_dedup_scan=on",
      "dedup_scan=on",
  };

  if (kafs_mount_options_eq_any(tok, off_tokens, KAFS_MOUNT_OPTIONS_ARRAY_COUNT(off_tokens)))
  {
    req->bg_dedup_scan_off_requested = 1;
    return 1;
  }
  if (kafs_mount_options_eq_any(tok, on_tokens, KAFS_MOUNT_OPTIONS_ARRAY_COUNT(on_tokens)))
  {
    req->bg_dedup_scan_enabled = 1;
    req->bg_dedup_scan_explicit = 1;
    return 1;
  }
  return 0;
}

static int kafs_v6_mount_options_record_fsync_token(kafs_v6_runtime_request_t *req, const char *tok)
{
  if (strcmp(tok, "fsync_policy=full") == 0)
  {
    req->fsync_policy_full_requested = 1;
    req->fsync_policy = KAFS_FSYNC_POLICY_FULL;
    return 1;
  }
  if (kafs_mount_options_starts_with(tok, "fsync_policy="))
  {
    req->fsync_policy_other_requested = 1;
    return 1;
  }
  return 0;
}

static int kafs_v6_mount_options_record_legacy_mode_token(kafs_v6_runtime_request_t *req,
                                                          const char *tok)
{
  static const char *const legacy_tokens[] = {
      "v6_inspection_mount",
      "v6-inspection-mount",
      "v6_write_mount",
      "v6-write-mount",
  };

  if (kafs_mount_options_eq_any(tok, legacy_tokens, KAFS_MOUNT_OPTIONS_ARRAY_COUNT(legacy_tokens)))
  {
    req->legacy_mode_token_seen = 1;
    return 1;
  }
  return 0;
}

static int kafs_v6_mount_options_record_hotplug_token(kafs_v6_runtime_request_t *req,
                                                      const char *tok)
{
  static const char *const hotplug_prefixes[] = {
      "hotplug=", "hotplug_uds=", "hotplug-uds=", "hotplug_back_bin=", "hotplug-back-bin=",
  };

  if (strcmp(tok, "hotplug") == 0 ||
      kafs_mount_options_starts_with_any(tok, hotplug_prefixes,
                                         KAFS_MOUNT_OPTIONS_ARRAY_COUNT(hotplug_prefixes)))
  {
    req->hotplug_requested = 1;
    return 1;
  }
  return 0;
}

int kafs_v6_mount_options_record_runtime_token(kafs_v6_runtime_request_t *req, const char *tok)
{
  if (!req || !tok)
    return -EINVAL;

  if (kafs_v6_mount_options_record_mode_token(req, tok) ||
      kafs_v6_mount_options_record_cache_token(req, tok) ||
      kafs_v6_mount_options_record_bg_dedup_token(req, tok) ||
      kafs_v6_mount_options_record_fsync_token(req, tok) ||
      kafs_v6_mount_options_record_legacy_mode_token(req, tok) ||
      kafs_v6_mount_options_record_hotplug_token(req, tok))
    return 0;

  return 0;
}

static int kafs_v6_mount_options_is_internal_token(const char *tok)
{
  kafs_v6_runtime_request_t req;
  kafs_v6_runtime_request_init(&req);

  if (kafs_v6_mount_options_record_cache_token(&req, tok) ||
      kafs_v6_mount_options_record_bg_dedup_token(&req, tok) ||
      kafs_v6_mount_options_record_legacy_mode_token(&req, tok) ||
      kafs_v6_mount_options_record_hotplug_token(&req, tok))
    return 1;
  return strcmp(tok, "fsync_policy=full") == 0;
}

int kafs_v6_mount_options_filter_fuse_args(const char *mountpoint, int argc_extra,
                                           char **argv_extra, char **argv_clean, int *argc_clean,
                                           char **owned, int *owned_count,
                                           kafs_v6_mount_thread_options_t *thread, FILE *err)
{
  return kafs_mount_options_filter_fuse_args("kafs-v6", mountpoint, argc_extra, argv_extra,
                                             argv_clean, argc_clean, owned, owned_count, thread,
                                             kafs_v6_mount_options_is_internal_token, err);
}

void kafs_v6_mount_options_free_owned(char **owned, int owned_count)
{
  kafs_mount_options_free_owned(owned, owned_count);
}

unsigned kafs_v6_mount_options_thread_count(const kafs_v6_mount_thread_options_t *thread)
{
  return kafs_mount_options_thread_count(thread);
}
