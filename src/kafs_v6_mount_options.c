#include "kafs_v6_mount_options.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#define KAFS_V6_ARRAY_COUNT(a) (sizeof(a) / sizeof((a)[0]))

static int kafs_v6_mount_options_starts_with(const char *s, const char *prefix)
{
  size_t len = strlen(prefix);
  return strncmp(s, prefix, len) == 0;
}

static int kafs_v6_mount_options_eq_any(const char *tok, const char *const *values, size_t count)
{
  for (size_t i = 0; i < count; ++i)
  {
    if (strcmp(tok, values[i]) == 0)
      return 1;
  }
  return 0;
}

static int kafs_v6_mount_options_starts_with_any(const char *tok, const char *const *prefixes,
                                                 size_t count)
{
  for (size_t i = 0; i < count; ++i)
  {
    if (kafs_v6_mount_options_starts_with(tok, prefixes[i]))
      return 1;
  }
  return 0;
}

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

  if (kafs_v6_mount_options_eq_any(tok, no_writeback, KAFS_V6_ARRAY_COUNT(no_writeback)))
  {
    req->no_writeback_cache_requested = 1;
    return 1;
  }
  if (kafs_v6_mount_options_eq_any(tok, writeback, KAFS_V6_ARRAY_COUNT(writeback)))
  {
    req->writeback_cache_enabled = 1;
    req->writeback_cache_explicit = 1;
    return 1;
  }
  if (kafs_v6_mount_options_eq_any(tok, no_trim, KAFS_V6_ARRAY_COUNT(no_trim)))
  {
    req->no_trim_on_free_requested = 1;
    return 1;
  }
  if (kafs_v6_mount_options_eq_any(tok, trim, KAFS_V6_ARRAY_COUNT(trim)))
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
      "bg_dedup_scan=off",
      "dedup_scan=off",
      "no_bg_dedup_scan",
      "no-bg-dedup-scan",
  };
  static const char *const on_tokens[] = {
      "bg_dedup_scan",
      "dedup_scan",
      "bg_dedup_scan=on",
      "dedup_scan=on",
  };

  if (kafs_v6_mount_options_eq_any(tok, off_tokens, KAFS_V6_ARRAY_COUNT(off_tokens)))
  {
    req->bg_dedup_scan_off_requested = 1;
    return 1;
  }
  if (kafs_v6_mount_options_eq_any(tok, on_tokens, KAFS_V6_ARRAY_COUNT(on_tokens)))
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
  if (kafs_v6_mount_options_starts_with(tok, "fsync_policy="))
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

  if (kafs_v6_mount_options_eq_any(tok, legacy_tokens, KAFS_V6_ARRAY_COUNT(legacy_tokens)))
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
      kafs_v6_mount_options_starts_with_any(tok, hotplug_prefixes,
                                            KAFS_V6_ARRAY_COUNT(hotplug_prefixes)))
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

static int kafs_v6_mount_options_parse_thread_token(const char *tok,
                                                    kafs_v6_mount_thread_options_t *thread,
                                                    FILE *err)
{
  if (kafs_v6_mount_options_starts_with(tok, "max_threads=") || strcmp(tok, "max_threads") == 0)
  {
    thread->saw_max_threads = 1;
    return 0;
  }

  static const char *const mt_tokens[] = {"multi_thread", "multi-thread", "multithread"};
  if (kafs_v6_mount_options_eq_any(tok, mt_tokens, KAFS_V6_ARRAY_COUNT(mt_tokens)))
  {
    thread->enable_mt = KAFS_TRUE;
    return 1;
  }

  const char *vstr = NULL;
  if (kafs_v6_mount_options_starts_with(tok, "multi_thread="))
    vstr = tok + strlen("multi_thread=");
  else if (kafs_v6_mount_options_starts_with(tok, "multi-thread="))
    vstr = tok + strlen("multi-thread=");
  else if (kafs_v6_mount_options_starts_with(tok, "multithread="))
    vstr = tok + strlen("multithread=");
  if (!vstr)
    return 0;

  char *endp = NULL;
  unsigned long value = strtoul(vstr, &endp, 10);
  if (!endp || *endp != '\0')
  {
    fprintf(err ? err : stderr, "invalid -o multi_thread=N: '%s'\n", vstr);
    return 2;
  }
  if (value < 1)
    value = 1;
  if (value > 100000)
    value = 100000;
  thread->enable_mt = KAFS_TRUE;
  thread->mt_cnt_override = (unsigned)value;
  thread->mt_cnt_override_set = 1;
  return 1;
}

static int kafs_v6_mount_options_is_kafs_owned_token(const char *tok)
{
  static const char *const exact[] = {
      "no_writeback_cache",  "no-writeback-cache", "writeback_cache",
      "writeback-cache",     "no_trim_on_free",    "no-trim-on-free",
      "trim_on_free",        "trim-on-free",       "hotplug",
      "sd_card_profile",     "sd-card-profile",    "v6_inspection_mount",
      "v6-inspection-mount", "v6_write_mount",     "v6-write-mount",
      "bg_dedup_scan",       "dedup_scan",         "no_bg_dedup_scan",
      "no-bg-dedup-scan",    "no_dedup_scan",      "no-dedup-scan",
  };
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

  return kafs_v6_mount_options_eq_any(tok, exact, KAFS_V6_ARRAY_COUNT(exact)) ||
         kafs_v6_mount_options_starts_with_any(tok, prefixes, KAFS_V6_ARRAY_COUNT(prefixes));
}

static int kafs_v6_mount_options_is_internal_token(const char *tok,
                                                   kafs_v6_mount_thread_options_t *thread,
                                                   FILE *err)
{
  int rc = kafs_v6_mount_options_parse_thread_token(tok, thread, err);
  if (rc != 0)
    return rc;
  if (kafs_v6_mount_options_is_kafs_owned_token(tok))
    return 1;
  return 0;
}

static int kafs_v6_mount_options_append_filtered_token(char *filtered, size_t filtered_size,
                                                       size_t *used, const char *tok, FILE *err)
{
  size_t tok_len = strlen(tok);
  size_t extra = tok_len + (*used ? 1u : 0u);
  if (extra >= filtered_size || *used > filtered_size - 1u - extra)
  {
    fprintf(err ? err : stderr, "kafs-v6: filtered FUSE option list is too long.\n");
    return 2;
  }
  if (*used)
    filtered[(*used)++] = ',';
  memcpy(filtered + *used, tok, tok_len);
  *used += tok_len;
  filtered[*used] = '\0';
  return 0;
}

static int kafs_v6_mount_options_filter_o_list(const char *oval, char *filtered,
                                               size_t filtered_size,
                                               kafs_v6_mount_thread_options_t *thread, FILE *err)
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
      fprintf(err ? err : stderr, "kafs-v6: empty token in -o option list.\n");
      free(dup);
      return 2;
    }

    int internal = kafs_v6_mount_options_is_internal_token(tok, thread, err);
    if (internal == 2)
    {
      free(dup);
      return 2;
    }
    if (internal == 1)
      continue;

    if (kafs_v6_mount_options_append_filtered_token(filtered, filtered_size, &used, tok, err) != 0)
    {
      free(dup);
      return 2;
    }
  }

  free(dup);
  return 0;
}

static int kafs_v6_mount_options_extract_o_arg(char **argv_extra, int argc_extra, int *index,
                                               const char **oval_out, int *compact_out, FILE *err)
{
  char *arg = argv_extra[*index];
  *oval_out = NULL;
  *compact_out = 0;

  if (strcmp(arg, "-o") == 0)
  {
    if (*index + 1 >= argc_extra)
    {
      fprintf(err, "kafs-v6: -o requires an option list.\n");
      return 2;
    }
    *oval_out = argv_extra[++(*index)];
    if (!*oval_out)
    {
      fprintf(err, "kafs-v6: -o received a null option list.\n");
      return 2;
    }
    return 0;
  }

  if (strncmp(arg, "-o", 2) == 0 && arg[2] != '\0')
  {
    *oval_out = arg + 2;
    *compact_out = 1;
  }
  return 0;
}

static int kafs_v6_mount_options_append_o_arg(char **argv_clean, int *argc_clean, char **owned,
                                              int *owned_count, const char *filtered, int compact)
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

static int kafs_v6_mount_options_validate_filter_args(
    const char *mountpoint, int argc_extra, char **argv_extra, char **argv_clean, int *argc_clean,
    char **owned, int *owned_count, kafs_v6_mount_thread_options_t *thread, FILE *err)
{
  if (!mountpoint || argc_extra < 0 || (argc_extra > 0 && !argv_extra) || !argv_clean ||
      !argc_clean || !owned || !owned_count || !thread)
  {
    fprintf(err, "kafs-v6: invalid mount option filter arguments.\n");
    return 2;
  }
  return 0;
}

static int kafs_v6_mount_options_filter_fuse_arg(char **argv_extra, int argc_extra, int *index,
                                                 char **argv_clean, int *argc_clean, char **owned,
                                                 int *owned_count,
                                                 kafs_v6_mount_thread_options_t *thread, FILE *err)
{
  char *arg = argv_extra[*index];
  if (!arg)
  {
    fprintf(err, "kafs-v6: null FUSE argument.\n");
    return 2;
  }

  const char *oval = NULL;
  int compact = 0;
  if (kafs_v6_mount_options_extract_o_arg(argv_extra, argc_extra, index, &oval, &compact, err) != 0)
    return 2;

  if (!oval)
  {
    argv_clean[(*argc_clean)++] = arg;
    return 0;
  }

  char filtered[strlen(oval) + 1u];
  if (kafs_v6_mount_options_filter_o_list(oval, filtered, sizeof(filtered), thread, err) != 0)
    return 2;
  if (filtered[0] == '\0')
    return 0;
  return kafs_v6_mount_options_append_o_arg(argv_clean, argc_clean, owned, owned_count, filtered,
                                            compact);
}

int kafs_v6_mount_options_filter_fuse_args(const char *mountpoint, int argc_extra,
                                           char **argv_extra, char **argv_clean, int *argc_clean,
                                           char **owned, int *owned_count,
                                           kafs_v6_mount_thread_options_t *thread, FILE *err)
{
  if (!err)
    err = stderr;
  if (kafs_v6_mount_options_validate_filter_args(mountpoint, argc_extra, argv_extra, argv_clean,
                                                 argc_clean, owned, owned_count, thread, err) != 0)
    return 2;

  memset(thread, 0, sizeof(*thread));
  *argc_clean = 0;
  *owned_count = 0;
  argv_clean[(*argc_clean)++] = "kafs-v6";
  argv_clean[(*argc_clean)++] = (char *)mountpoint;

  for (int i = 0; i < argc_extra; ++i)
  {
    if (kafs_v6_mount_options_filter_fuse_arg(argv_extra, argc_extra, &i, argv_clean, argc_clean,
                                              owned, owned_count, thread, err) != 0)
      return 2;
  }
  return 0;
}

void kafs_v6_mount_options_free_owned(char **owned, int owned_count)
{
  for (int i = 0; i < owned_count; ++i)
    free(owned[i]);
}

unsigned kafs_v6_mount_options_thread_count(const kafs_v6_mount_thread_options_t *thread)
{
  unsigned mt_cnt = 8;
  if (thread && thread->mt_cnt_override_set)
    mt_cnt = thread->mt_cnt_override;
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
