#pragma once

#include "kafs.h"
#include "kafs_mount_options_common.h"
#include "kafs_v7_runtime.h"

#include <stdio.h>

/*
 * Internal kafs-v7 mount option policy.
 *
 * The dedicated CLI records runtime-admission intent from -o tokens, while the
 * entrypoint adapter strips kafs-owned tokens before handing argv to FUSE.
 * Keep both interpretations routed through this helper so the token vocabulary
 * has one owner.
 */

typedef kafs_mount_thread_options_t kafs_v7_mount_thread_options_t;

int kafs_v7_mount_options_record_runtime_token(kafs_v7_runtime_request_t *req, const char *tok);
int kafs_v7_mount_options_filter_fuse_args(const char *mountpoint, int argc_extra,
                                           char **argv_extra, char **argv_clean, int *argc_clean,
                                           char **owned, int *owned_count,
                                           kafs_v7_mount_thread_options_t *thread, FILE *err);
void kafs_v7_mount_options_free_owned(char **owned, int owned_count);
unsigned kafs_v7_mount_options_thread_count(const kafs_v7_mount_thread_options_t *thread);
