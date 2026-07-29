#include "kafs_mount_options_common.h"

#include <stdlib.h>
#include <string.h>

int kafs_mount_options_starts_with(const char *s, const char *prefix)
{
  size_t len = strlen(prefix);
  return strncmp(s, prefix, len) == 0;
}

int kafs_mount_options_eq_any(const char *tok, const char *const *values, size_t count)
{
  for (size_t i = 0; i < count; ++i)
  {
    if (strcmp(tok, values[i]) == 0)
      return 1;
  }
  return 0;
}

int kafs_mount_options_starts_with_any(const char *tok, const char *const *prefixes, size_t count)
{
  for (size_t i = 0; i < count; ++i)
  {
    if (kafs_mount_options_starts_with(tok, prefixes[i]))
      return 1;
  }
  return 0;
}

static int parse_thread_token(const char *tok, kafs_mount_thread_options_t *thread, FILE *err)
{
  if (kafs_mount_options_starts_with(tok, "max_threads=") || strcmp(tok, "max_threads") == 0)
  {
    thread->saw_max_threads = 1;
    return 0;
  }

  static const char *const mt_tokens[] = {"multi_thread", "multi-thread", "multithread"};
  if (kafs_mount_options_eq_any(tok, mt_tokens, KAFS_MOUNT_OPTIONS_ARRAY_COUNT(mt_tokens)))
  {
    thread->enable_mt = KAFS_TRUE;
    return 1;
  }

  const char *vstr = NULL;
  if (kafs_mount_options_starts_with(tok, "multi_thread="))
    vstr = tok + strlen("multi_thread=");
  else if (kafs_mount_options_starts_with(tok, "multi-thread="))
    vstr = tok + strlen("multi-thread=");
  else if (kafs_mount_options_starts_with(tok, "multithread="))
    vstr = tok + strlen("multithread=");
  if (!vstr)
    return 0;

  char *endp = NULL;
  unsigned long value = strtoul(vstr, &endp, 10);
  if (!endp || *endp != '\0')
  {
    fprintf(err, "invalid -o multi_thread=N: '%s'\n", vstr);
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

static int is_unsupported_kafs_token(const char *tok)
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

  return kafs_mount_options_eq_any(tok, exact, KAFS_MOUNT_OPTIONS_ARRAY_COUNT(exact)) ||
         kafs_mount_options_starts_with_any(tok, prefixes,
                                            KAFS_MOUNT_OPTIONS_ARRAY_COUNT(prefixes));
}

static int classify_token(const char *tool_name, const char *tok,
                          kafs_mount_thread_options_t *thread,
                          kafs_mount_option_internal_fn is_internal, FILE *err)
{
  int rc = parse_thread_token(tok, thread, err);
  if (rc != 0)
    return rc;
  if (is_internal && is_internal(tok))
    return 1;
  if (is_unsupported_kafs_token(tok))
  {
    fprintf(err,
            "%s: unsupported KAFS mount option '-o %s'; use descriptor-backed admission options or "
            "FUSE passthrough options.\n",
            tool_name, tok);
    return 2;
  }
  return 0;
}

static int append_filtered_token(const char *tool_name, char *filtered, size_t filtered_size,
                                 size_t *used, const char *tok, FILE *err)
{
  size_t tok_len = strlen(tok);
  size_t extra = tok_len + (*used ? 1u : 0u);
  if (extra >= filtered_size || *used > filtered_size - 1u - extra)
  {
    fprintf(err, "%s: filtered FUSE option list is too long.\n", tool_name);
    return 2;
  }
  if (*used)
    filtered[(*used)++] = ',';
  memcpy(filtered + *used, tok, tok_len);
  *used += tok_len;
  filtered[*used] = '\0';
  return 0;
}

static int filter_o_list(const char *tool_name, const char *oval, char *filtered,
                         size_t filtered_size, kafs_mount_thread_options_t *thread,
                         kafs_mount_option_internal_fn is_internal, FILE *err)
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
      fprintf(err, "%s: empty token in -o option list.\n", tool_name);
      free(dup);
      return 2;
    }

    int internal = classify_token(tool_name, tok, thread, is_internal, err);
    if (internal == 2)
    {
      free(dup);
      return 2;
    }
    if (internal == 1)
      continue;
    if (append_filtered_token(tool_name, filtered, filtered_size, &used, tok, err) != 0)
    {
      free(dup);
      return 2;
    }
  }

  free(dup);
  return 0;
}

static int extract_o_arg(const char *tool_name, char **argv_extra, int argc_extra, int *index,
                         const char **oval_out, int *compact_out, FILE *err)
{
  char *arg = argv_extra[*index];
  *oval_out = NULL;
  *compact_out = 0;
  if (strcmp(arg, "-o") == 0)
  {
    if (*index + 1 >= argc_extra)
    {
      fprintf(err, "%s: -o requires an option list.\n", tool_name);
      return 2;
    }
    *oval_out = argv_extra[++(*index)];
    if (!*oval_out)
    {
      fprintf(err, "%s: -o received a null option list.\n", tool_name);
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

static int append_o_arg(char **argv_clean, int *argc_clean, char **owned, int *owned_count,
                        const char *filtered, int compact)
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

static int filter_fuse_arg(const char *tool_name, char **argv_extra, int argc_extra, int *index,
                           char **argv_clean, int *argc_clean, char **owned, int *owned_count,
                           kafs_mount_thread_options_t *thread,
                           kafs_mount_option_internal_fn is_internal, FILE *err)
{
  char *arg = argv_extra[*index];
  if (!arg)
  {
    fprintf(err, "%s: null FUSE argument.\n", tool_name);
    return 2;
  }

  const char *oval = NULL;
  int compact = 0;
  if (extract_o_arg(tool_name, argv_extra, argc_extra, index, &oval, &compact, err) != 0)
    return 2;
  if (!oval)
  {
    argv_clean[(*argc_clean)++] = arg;
    return 0;
  }

  char filtered[strlen(oval) + 1u];
  if (filter_o_list(tool_name, oval, filtered, sizeof(filtered), thread, is_internal, err) != 0)
    return 2;
  if (filtered[0] == '\0')
    return 0;
  return append_o_arg(argv_clean, argc_clean, owned, owned_count, filtered, compact);
}

int kafs_mount_options_filter_fuse_args(const char *tool_name, const char *mountpoint,
                                        int argc_extra, char **argv_extra, char **argv_clean,
                                        int *argc_clean, char **owned, int *owned_count,
                                        kafs_mount_thread_options_t *thread,
                                        kafs_mount_option_internal_fn is_internal, FILE *err)
{
  if (!err)
    err = stderr;
  if (!tool_name || !mountpoint || argc_extra < 0 || (argc_extra > 0 && !argv_extra) ||
      !argv_clean || !argc_clean || !owned || !owned_count || !thread)
  {
    fprintf(err, "%s: invalid mount option filter arguments.\n", tool_name ? tool_name : "kafs");
    return 2;
  }

  memset(thread, 0, sizeof(*thread));
  *argc_clean = 0;
  *owned_count = 0;
  argv_clean[(*argc_clean)++] = (char *)tool_name;
  argv_clean[(*argc_clean)++] = (char *)mountpoint;
  for (int i = 0; i < argc_extra; ++i)
  {
    if (filter_fuse_arg(tool_name, argv_extra, argc_extra, &i, argv_clean, argc_clean, owned,
                        owned_count, thread, is_internal, err) != 0)
      return 2;
  }
  return 0;
}

void kafs_mount_options_free_owned(char **owned, int owned_count)
{
  for (int i = 0; i < owned_count; ++i)
    free(owned[i]);
}

unsigned kafs_mount_options_thread_count(const kafs_mount_thread_options_t *thread)
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
