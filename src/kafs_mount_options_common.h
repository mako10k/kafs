#pragma once

#include "kafs.h"

#include <stdio.h>

#define KAFS_MOUNT_OPTIONS_ARRAY_COUNT(a) (sizeof(a) / sizeof((a)[0]))

typedef struct kafs_mount_thread_options
{
  kafs_bool_t enable_mt;
  int saw_max_threads;
  unsigned mt_cnt_override;
  int mt_cnt_override_set;
} kafs_mount_thread_options_t;

typedef int (*kafs_mount_option_internal_fn)(const char *tok);

int kafs_mount_options_starts_with(const char *s, const char *prefix);
int kafs_mount_options_eq_any(const char *tok, const char *const *values, size_t count);
int kafs_mount_options_starts_with_any(const char *tok, const char *const *prefixes, size_t count);
int kafs_mount_options_filter_fuse_args(const char *tool_name, const char *mountpoint,
                                        int argc_extra, char **argv_extra, char **argv_clean,
                                        int *argc_clean, char **owned, int *owned_count,
                                        kafs_mount_thread_options_t *thread,
                                        kafs_mount_option_internal_fn is_internal, FILE *err);
void kafs_mount_options_free_owned(char **owned, int owned_count);
unsigned kafs_mount_options_thread_count(const kafs_mount_thread_options_t *thread);
