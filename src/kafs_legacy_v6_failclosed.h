#pragma once

#include <stdio.h>
#include <string.h>

/*
 * Production kafs legacy format-v6 fail-closed policy.
 *
 * Successful format-v6 runtime admission belongs to kafs-v6. Production kafs
 * only recognizes legacy v6 mount tokens so it can reject them with explicit
 * operator guidance.
 */

typedef enum kafs_legacy_v6_request
{
  KAFS_LEGACY_V6_REQUEST_NONE = 0,
  KAFS_LEGACY_V6_REQUEST_INSPECTION,
  KAFS_LEGACY_V6_REQUEST_CONTROLLED_WRITE,
} kafs_legacy_v6_request_t;

static inline int kafs_legacy_v6_flag_request(const char *arg, kafs_legacy_v6_request_t *request)
{
  if (strcmp(arg, "--v6-inspection-mount") == 0)
  {
    *request = KAFS_LEGACY_V6_REQUEST_INSPECTION;
    return 1;
  }
  if (strcmp(arg, "--v6-write-mount") == 0)
  {
    *request = KAFS_LEGACY_V6_REQUEST_CONTROLLED_WRITE;
    return 1;
  }
  return 0;
}

static inline int kafs_legacy_v6_option_request(const char *tok, kafs_legacy_v6_request_t *request)
{
  if (strcmp(tok, "v6_inspection_mount") == 0 || strcmp(tok, "v6-inspection-mount") == 0)
  {
    *request = KAFS_LEGACY_V6_REQUEST_INSPECTION;
    return 1;
  }
  if (strcmp(tok, "v6_write_mount") == 0 || strcmp(tok, "v6-write-mount") == 0)
  {
    *request = KAFS_LEGACY_V6_REQUEST_CONTROLLED_WRITE;
    return 1;
  }
  return 0;
}

static inline int kafs_legacy_v6_reject_if_requested(int inspection_requested, int write_requested,
                                                     FILE *err)
{
  if (!err)
    err = stderr;
  if (write_requested)
  {
    fprintf(err, "kafs: legacy v6 controlled write mount moved to kafs-v6; use "
                 "kafs-v6 --image <image> --controlled-write-mount <mountpoint> "
                 "-o rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full.\n");
    return 2;
  }
  if (inspection_requested)
  {
    fprintf(err, "kafs: legacy v6 inspection mount moved to kafs-v6; use "
                 "kafs-v6 --image <image> --inspection-mount <mountpoint> -o ro.\n");
    return 2;
  }
  return 0;
}
