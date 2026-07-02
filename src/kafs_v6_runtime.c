#include "kafs_v6_runtime.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

void kafs_v6_runtime_request_init(kafs_v6_runtime_request_t *req)
{
  if (!req)
    return;
  memset(req, 0, sizeof(*req));
  req->mode = KAFS_V6_RUNTIME_MODE_NONE;
  req->fsync_policy = KAFS_FSYNC_POLICY_JOURNAL_ONLY;
}

static int kafs_v6_runtime_invalid(kafs_v6_runtime_validation_reason_t reason,
                                   kafs_v6_runtime_validation_reason_t *reason_out)
{
  if (reason_out)
    *reason_out = reason;
  return 2;
}

static int kafs_v6_runtime_valid(kafs_v6_runtime_validation_reason_t *reason_out)
{
  if (reason_out)
    *reason_out = KAFS_V6_RUNTIME_VALID;
  return 0;
}

int kafs_v6_runtime_validate_kafs_request(const kafs_v6_runtime_request_t *req,
                                          kafs_v6_runtime_validation_reason_t *reason_out)
{
  if (!req || req->mode == KAFS_V6_RUNTIME_MODE_NONE)
    return kafs_v6_runtime_valid(reason_out);

  if (req->mode == KAFS_V6_RUNTIME_MODE_CONTROLLED_WRITE)
  {
    if (req->inspection_token_seen)
      return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_WITH_INSPECTION,
                                     reason_out);
    if (req->mount_read_only_seen)
      return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_RO, reason_out);
    if (!req->mount_read_write_requested)
      return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_RW, reason_out);
    if (req->writeback_cache_enabled)
      return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_NO_WRITEBACK,
                                     reason_out);
    if (req->trim_on_free_enabled)
      return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_NO_TRIM, reason_out);
    if (req->bg_dedup_scan_enabled)
      return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_BG_OFF, reason_out);
    return kafs_v6_runtime_valid(reason_out);
  }

  if (!req->mount_read_only_requested)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_INSPECTION_NEEDS_RO, reason_out);
  if (req->writeback_cache_explicit && req->writeback_cache_enabled)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_INSPECTION_WRITEBACK_CACHE, reason_out);
  return kafs_v6_runtime_valid(reason_out);
}

int kafs_v6_runtime_validate_entrypoint_request(const kafs_v6_runtime_request_t *req,
                                                kafs_v6_runtime_validation_reason_t *reason_out)
{
  if (!req || req->mode == KAFS_V6_RUNTIME_MODE_NONE)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_NO_MODE, reason_out);
  if (req->legacy_mode_token_seen)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_LEGACY_MODE_TOKEN, reason_out);
  if (req->hotplug_requested)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_HOTPLUG, reason_out);

  if (req->mode == KAFS_V6_RUNTIME_MODE_INSPECTION)
  {
    if (!req->mount_read_only_requested || req->mount_read_write_requested)
      return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_INSPECTION_NEEDS_RO, reason_out);
    return kafs_v6_runtime_valid(reason_out);
  }

  if (req->mount_read_only_requested || req->mount_read_only_seen)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_RO, reason_out);
  if (!req->mount_read_write_requested)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_RW, reason_out);
  if (!req->no_writeback_cache_requested)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_NO_WRITEBACK,
                                   reason_out);
  if (!req->no_trim_on_free_requested)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_NO_TRIM, reason_out);
  if (!req->bg_dedup_scan_off_requested)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_BG_OFF, reason_out);
  if (!req->fsync_policy_full_requested)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_FSYNC_FULL, reason_out);
  if (req->writeback_cache_enabled)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_UNSAFE_WRITEBACK, reason_out);
  if (req->trim_on_free_enabled)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_UNSAFE_TRIM, reason_out);
  if (req->bg_dedup_scan_enabled)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_UNSAFE_BG_DEDUP, reason_out);
  if (req->fsync_policy_other_requested || req->fsync_policy != KAFS_FSYNC_POLICY_FULL)
    return kafs_v6_runtime_invalid(KAFS_V6_RUNTIME_INVALID_CONTROLLED_UNSAFE_FSYNC, reason_out);
  return kafs_v6_runtime_valid(reason_out);
}

int kafs_v6_runtime_check_image_format(const char *image_path, uint32_t expected_format, FILE *err,
                                       const char *tool_name)
{
  if (!tool_name)
    tool_name = "kafs-v6";
  if (!err)
    err = stderr;

  kafs_ssuperblock_t sb;
  int fd = open(image_path, O_RDONLY | O_CLOEXEC);
  if (fd < 0)
  {
    fprintf(err, "%s: open image failed: %s: %s\n", tool_name, image_path, strerror(errno));
    return -errno;
  }

  ssize_t r = pread(fd, &sb, sizeof(sb), 0);
  int saved_errno = errno;
  close(fd);
  if (r != (ssize_t)sizeof(sb))
  {
    fprintf(err, "%s: failed to read superblock from %s: %s\n", tool_name, image_path,
            (r < 0) ? strerror(saved_errno) : "short read");
    return (r < 0) ? -saved_errno : -EIO;
  }
  if (kafs_sb_magic_get(&sb) != KAFS_MAGIC)
  {
    fprintf(err, "%s: invalid KAFS magic in %s.\n", tool_name, image_path);
    return -EINVAL;
  }
  if (kafs_sb_format_version_get(&sb) != expected_format)
  {
    fprintf(err, "%s: image is format v%u; expected format v%u.\n", tool_name,
            (unsigned)kafs_sb_format_version_get(&sb), (unsigned)expected_format);
    return -EPROTONOSUPPORT;
  }
  return 0;
}
