#include "kafs_v7_runtime.h"

#include "kafs_context.h"
#include "kafs_v7_admission.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#ifndef KAFS_V7_TOOL_NAME
#define KAFS_V7_TOOL_NAME "kafs-v7"
#endif

#ifndef KAFS_V7_TOOL_FORMAT_VERSION
#define KAFS_V7_TOOL_FORMAT_VERSION KAFS_FORMAT_VERSION_V7
#endif

#ifndef KAFS_V7_TOOL_FORMAT_LABEL
#define KAFS_V7_TOOL_FORMAT_LABEL "v7"
#endif

void kafs_v7_runtime_request_init(kafs_v7_runtime_request_t *req)
{
  if (!req)
    return;
  memset(req, 0, sizeof(*req));
  req->mode = KAFS_V7_RUNTIME_MODE_NONE;
  req->fsync_policy = KAFS_FSYNC_POLICY_JOURNAL_ONLY;
}

static int kafs_v7_runtime_invalid(kafs_v7_runtime_validation_reason_t reason,
                                   kafs_v7_runtime_validation_reason_t *reason_out)
{
  if (reason_out)
    *reason_out = reason;
  return 2;
}

static int kafs_v7_runtime_valid(kafs_v7_runtime_validation_reason_t *reason_out)
{
  if (reason_out)
    *reason_out = KAFS_V7_RUNTIME_VALID;
  return 0;
}

int kafs_v7_runtime_validate_kafs_request(const kafs_v7_runtime_request_t *req,
                                          kafs_v7_runtime_validation_reason_t *reason_out)
{
  if (!req || req->mode == KAFS_V7_RUNTIME_MODE_NONE)
    return kafs_v7_runtime_valid(reason_out);

  if (req->mode == KAFS_V7_RUNTIME_MODE_CONTROLLED_WRITE)
  {
    if (req->inspection_token_seen)
      return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_WITH_INSPECTION,
                                     reason_out);
    if (req->mount_read_only_seen)
      return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_RO, reason_out);
    if (!req->mount_read_write_requested)
      return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_NEEDS_RW, reason_out);
    if (req->writeback_cache_enabled)
      return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_NEEDS_NO_WRITEBACK,
                                     reason_out);
    if (req->trim_on_free_enabled)
      return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_NEEDS_NO_TRIM, reason_out);
    if (req->bg_dedup_scan_enabled)
      return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_NEEDS_BG_OFF, reason_out);
    return kafs_v7_runtime_valid(reason_out);
  }

  if (!req->mount_read_only_requested)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_INSPECTION_NEEDS_RO, reason_out);
  if (req->writeback_cache_explicit && req->writeback_cache_enabled)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_INSPECTION_WRITEBACK_CACHE, reason_out);
  return kafs_v7_runtime_valid(reason_out);
}

int kafs_v7_runtime_validate_entrypoint_request(const kafs_v7_runtime_request_t *req,
                                                kafs_v7_runtime_validation_reason_t *reason_out)
{
  if (!req || req->mode == KAFS_V7_RUNTIME_MODE_NONE)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_NO_MODE, reason_out);
  if (req->legacy_mode_token_seen)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_LEGACY_MODE_TOKEN, reason_out);
  if (req->hotplug_requested)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_HOTPLUG, reason_out);

  if (req->mode == KAFS_V7_RUNTIME_MODE_INSPECTION)
  {
    if (!req->mount_read_only_requested || req->mount_read_write_requested)
      return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_INSPECTION_NEEDS_RO, reason_out);
    if (req->writeback_cache_explicit && req->writeback_cache_enabled)
      return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_INSPECTION_WRITEBACK_CACHE,
                                     reason_out);
    return kafs_v7_runtime_valid(reason_out);
  }

  if (req->mount_read_only_requested || req->mount_read_only_seen)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_RO, reason_out);
  if (!req->mount_read_write_requested)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_NEEDS_RW, reason_out);
  if (!req->no_writeback_cache_requested)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_NEEDS_NO_WRITEBACK,
                                   reason_out);
  if (!req->no_trim_on_free_requested)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_NEEDS_NO_TRIM, reason_out);
  if (!req->bg_dedup_scan_off_requested)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_NEEDS_BG_OFF, reason_out);
  if (!req->fsync_policy_full_requested)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_NEEDS_FSYNC_FULL, reason_out);
  if (req->writeback_cache_enabled)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_UNSAFE_WRITEBACK, reason_out);
  if (req->trim_on_free_enabled)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_UNSAFE_TRIM, reason_out);
  if (req->bg_dedup_scan_enabled)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_UNSAFE_BG_DEDUP, reason_out);
  if (req->fsync_policy_other_requested || req->fsync_policy != KAFS_FSYNC_POLICY_FULL)
    return kafs_v7_runtime_invalid(KAFS_V7_RUNTIME_INVALID_CONTROLLED_UNSAFE_FSYNC, reason_out);
  return kafs_v7_runtime_valid(reason_out);
}

void kafs_v7_runtime_print_validation_error(kafs_v7_runtime_validation_reason_t reason, FILE *err)
{
  if (!err)
    err = stderr;

  switch (reason)
  {
  case KAFS_V7_RUNTIME_INVALID_NO_MODE:
    fprintf(err, "%s requires --inspection-mount or --controlled-write-mount.\n",
            KAFS_V7_TOOL_NAME);
    return;
  case KAFS_V7_RUNTIME_INVALID_LEGACY_MODE_TOKEN:
    fprintf(err, "%s owns the %s runtime mode; do not pass legacy v6_* mount options.\n",
            KAFS_V7_TOOL_NAME, KAFS_V7_TOOL_FORMAT_LABEL);
    return;
  case KAFS_V7_RUNTIME_INVALID_HOTPLUG:
    fprintf(err, "%s does not admit hotplug delegated write options.\n", KAFS_V7_TOOL_NAME);
    return;
  case KAFS_V7_RUNTIME_INVALID_INSPECTION_NEEDS_RO:
    fprintf(err, "%s inspection mode requires -o ro and does not allow -o rw.\n",
            KAFS_V7_TOOL_NAME);
    return;
  case KAFS_V7_RUNTIME_INVALID_INSPECTION_WRITEBACK_CACHE:
    fprintf(err, "%s inspection mode does not allow writeback_cache.\n", KAFS_V7_TOOL_NAME);
    return;
  case KAFS_V7_RUNTIME_INVALID_CONTROLLED_RO:
    fprintf(err, "%s controlled write mode does not allow -o ro.\n", KAFS_V7_TOOL_NAME);
    return;
  case KAFS_V7_RUNTIME_INVALID_CONTROLLED_UNSAFE_WRITEBACK:
  case KAFS_V7_RUNTIME_INVALID_CONTROLLED_UNSAFE_TRIM:
  case KAFS_V7_RUNTIME_INVALID_CONTROLLED_UNSAFE_BG_DEDUP:
  case KAFS_V7_RUNTIME_INVALID_CONTROLLED_UNSAFE_FSYNC:
    fprintf(err, "%s controlled write mode rejected unsafe mount options.\n", KAFS_V7_TOOL_NAME);
    return;
  case KAFS_V7_RUNTIME_INVALID_CONTROLLED_NEEDS_RW:
  case KAFS_V7_RUNTIME_INVALID_CONTROLLED_NEEDS_NO_WRITEBACK:
  case KAFS_V7_RUNTIME_INVALID_CONTROLLED_NEEDS_NO_TRIM:
  case KAFS_V7_RUNTIME_INVALID_CONTROLLED_NEEDS_BG_OFF:
  case KAFS_V7_RUNTIME_INVALID_CONTROLLED_NEEDS_FSYNC_FULL:
    fprintf(err,
            "%s controlled write mode requires "
            "-o rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full.\n",
            KAFS_V7_TOOL_NAME);
    return;
  case KAFS_V7_RUNTIME_VALID:
  case KAFS_V7_RUNTIME_INVALID_CONTROLLED_WITH_INSPECTION:
  default:
    fprintf(err, "%s rejected invalid %s runtime admission options.\n", KAFS_V7_TOOL_NAME,
            KAFS_V7_TOOL_FORMAT_LABEL);
    return;
  }
}

int kafs_v7_runtime_report_entrypoint_request(const kafs_v7_runtime_request_t *req, FILE *err)
{
  kafs_v7_runtime_validation_reason_t reason = KAFS_V7_RUNTIME_VALID;
  int rc = kafs_v7_runtime_validate_entrypoint_request(req, &reason);
  if (rc != 0)
    kafs_v7_runtime_print_validation_error(reason, err);
  return rc;
}

static int kafs_v7_runtime_read_superblock_fd(int fd, kafs_ssuperblock_t *sbdisk,
                                              int *saved_errno_out, int *short_read_out)
{
  if (saved_errno_out)
    *saved_errno_out = 0;
  if (short_read_out)
    *short_read_out = 0;
  if (fd < 0 || !sbdisk)
    return -EINVAL;

  ssize_t r = pread(fd, sbdisk, sizeof(*sbdisk), 0);
  if (r == (ssize_t)sizeof(*sbdisk))
    return 0;

  if (r < 0)
  {
    int saved_errno = errno;
    if (saved_errno_out)
      *saved_errno_out = saved_errno;
    return -saved_errno;
  }

  if (short_read_out)
    *short_read_out = 1;
  return -EIO;
}

static const char *kafs_v7_runtime_superblock_read_error(int rc, int saved_errno, int short_read)
{
  if (short_read)
    return "short read";
  if (saved_errno != 0)
    return strerror(saved_errno);
  if (rc < 0)
    return strerror(-rc);
  return strerror(rc);
}

static int kafs_v7_runtime_open_readonly_superblock(const char *image_path,
                                                    kafs_ssuperblock_t *sbdisk, FILE *err,
                                                    const char *tool_name, int *fd_out)
{
  if (!tool_name)
    tool_name = KAFS_V7_TOOL_NAME;
  if (!err)
    err = stderr;
  if (fd_out)
    *fd_out = -1;
  if (!image_path || !sbdisk)
    return -EINVAL;

  int fd = open(image_path, O_RDONLY | O_CLOEXEC);
  if (fd < 0)
  {
    int saved_errno = errno;
    fprintf(err, "%s: open image failed: %s: %s\n", tool_name, image_path, strerror(saved_errno));
    return -saved_errno;
  }

  int saved_errno = 0;
  int short_read = 0;
  int rc = kafs_v7_runtime_read_superblock_fd(fd, sbdisk, &saved_errno, &short_read);
  if (rc != 0)
  {
    fprintf(err, "%s: failed to read superblock from %s: %s\n", tool_name, image_path,
            kafs_v7_runtime_superblock_read_error(rc, saved_errno, short_read));
    close(fd);
    return rc;
  }

  if (fd_out)
    *fd_out = fd;
  else
    close(fd);
  return 0;
}

int kafs_v7_runtime_check_image_format(const char *image_path, uint32_t expected_format, FILE *err,
                                       const char *tool_name)
{
  if (!tool_name)
    tool_name = KAFS_V7_TOOL_NAME;
  if (!err)
    err = stderr;

  kafs_ssuperblock_t sb;
  int rc = kafs_v7_runtime_open_readonly_superblock(image_path, &sb, err, tool_name, NULL);
  if (rc != 0)
    return rc;

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

static const char *kafs_v7_runtime_rc_text(int rc, char *buf, size_t buf_sz)
{
  if (rc == 0)
    return "ok";
  if (!buf || buf_sz == 0u)
    return "error";

  int err = (rc < 0) ? -rc : rc;
  int len = snprintf(buf, buf_sz, "rc=%d", rc);
  if (err != 0 && len >= 0 && (size_t)len < buf_sz)
    snprintf(buf + len, buf_sz - (size_t)len, " (%s)", strerror(err));
  return buf;
}

static void kafs_v7_runtime_close_context_fd(kafs_context_t *ctx)
{
  if (!ctx)
    return;
  if (ctx->c_fd >= 0)
    close(ctx->c_fd);
  ctx->c_fd = -1;
}

int kafs_v7_runtime_open_context_image(kafs_context_t *ctx, const char *image_path,
                                       kafs_v7_runtime_mode_t mode, kafs_ssuperblock_t *sbdisk,
                                       FILE *err)
{
  if (!err)
    err = stderr;
  if (!ctx || !image_path || !sbdisk)
    return -EINVAL;

  const int controlled_write = (mode == KAFS_V7_RUNTIME_MODE_CONTROLLED_WRITE);
  if (!controlled_write && mode != KAFS_V7_RUNTIME_MODE_INSPECTION)
    return -EINVAL;

  const int open_flags = controlled_write ? O_RDWR : O_RDONLY;
  ctx->c_fd = open(image_path, open_flags, 0666);
  if (ctx->c_fd < 0)
  {
    int saved_errno = errno;
    perror("open image");
    fprintf(err, "image not found. run mkfs.kafs first.\n");
    return -saved_errno;
  }

  ctx->c_blo_search = 0;
  ctx->c_ino_search = 0;

  int rc = kafs_v7_runtime_read_superblock_fd(ctx->c_fd, sbdisk, NULL, NULL);
  if (rc != 0)
  {
    char errbuf[128];
    fprintf(err, "%s: failed to read superblock: %s.\n", KAFS_V7_TOOL_NAME,
            kafs_v7_runtime_rc_text(rc, errbuf, sizeof(errbuf)));
    kafs_v7_runtime_close_context_fd(ctx);
    return rc;
  }
  if (kafs_sb_magic_get(sbdisk) != KAFS_MAGIC)
  {
    fprintf(err, "%s: invalid magic. run mkfs.kafs to format.\n", KAFS_V7_TOOL_NAME);
    kafs_v7_runtime_close_context_fd(ctx);
    return -EINVAL;
  }

  uint32_t fmt_ver = kafs_sb_format_version_get(sbdisk);
  if (fmt_ver != KAFS_V7_TOOL_FORMAT_VERSION)
  {
    fprintf(err, "%s %s mount applies only to format %s images (found v%u).\n", KAFS_V7_TOOL_NAME,
            controlled_write ? "controlled write" : "inspection", KAFS_V7_TOOL_FORMAT_LABEL,
            fmt_ver);
    kafs_v7_runtime_close_context_fd(ctx);
    return -EPROTONOSUPPORT;
  }
  return 0;
}

static int kafs_v7_runtime_admit_context(kafs_context_t *ctx, const kafs_ssuperblock_t *sbdisk,
                                         kafs_v7_runtime_mode_t mode)
{
  int prot = 0;
  if (mode == KAFS_V7_RUNTIME_MODE_INSPECTION)
    prot = PROT_READ;
  else if (mode == KAFS_V7_RUNTIME_MODE_CONTROLLED_WRITE)
    prot = PROT_READ | PROT_WRITE;
  else
    return -EINVAL;

  return kafs_v7_admission_runtime_context(ctx, sbdisk, prot);
}

int kafs_v7_runtime_admit_mount_context(kafs_context_t *ctx, const kafs_ssuperblock_t *sbdisk,
                                        kafs_v7_runtime_mode_t mode, kafs_inocnt_t *inocnt_out,
                                        kafs_blkcnt_t *r_blkcnt_out, FILE *err)
{
  if (!err)
    err = stderr;
  if (!ctx || !sbdisk)
    return -EINVAL;

  int rc = kafs_v7_runtime_admit_context(ctx, sbdisk, mode);
  if (rc == 0)
  {
    if (mode == KAFS_V7_RUNTIME_MODE_INSPECTION)
      ctx->c_runtime_read_only = 1u;
    else if (mode == KAFS_V7_RUNTIME_MODE_CONTROLLED_WRITE)
      ctx->c_v6_controlled_write_enabled = 1u;
    else
      return -EINVAL;

    if (inocnt_out)
      *inocnt_out = kafs_inocnt_stoh(sbdisk->s_inocnt);
    if (r_blkcnt_out)
      *r_blkcnt_out = kafs_blkcnt_stoh(sbdisk->s_r_blkcnt);

    if (mode == KAFS_V7_RUNTIME_MODE_INSPECTION)
    {
      fprintf(err,
              "format %s inspection mount: selected descriptor retained in read-only "
              "runtime context; descriptor-backed runtime views active; legacy contiguous "
              "inode/bitmap tables are not installed; %s; delayed/background mutations are "
              "disabled; FUSE mount is "
              "inspection-only and write admission remains disabled.\n",
              KAFS_V7_TOOL_FORMAT_LABEL, kafs_ctx_v6_worker_policy_summary());
    }
    else
    {
      fprintf(err,
              "format %s controlled write mount: selected descriptor retained in write runtime "
              "context; descriptor-backed runtime views active; legacy contiguous inode/bitmap "
              "tables are not installed; %s; delayed/background mutations are disabled; FUSE "
              "write surface is limited "
              "to regular-file create/write/fsync/release.\n",
              KAFS_V7_TOOL_FORMAT_LABEL, kafs_ctx_v6_worker_policy_summary());
    }
  }
  else
  {
    char errbuf[128];
    fprintf(err, "format %s %s mount admission failed: %s.\n", KAFS_V7_TOOL_FORMAT_LABEL,
            mode == KAFS_V7_RUNTIME_MODE_CONTROLLED_WRITE ? "controlled write" : "inspection",
            kafs_v7_runtime_rc_text(rc, errbuf, sizeof(errbuf)));
  }
  return rc;
}

int kafs_v7_runtime_init_mount_services(kafs_context_t *ctx, const char *image_path,
                                        kafs_v7_runtime_mode_t mode, kafs_inocnt_t inocnt,
                                        kafs_blkcnt_t r_blkcnt, FILE *err)
{
  if (!err)
    err = stderr;
  if (!ctx || !image_path)
    return -EINVAL;

  kafs_ctx_init_diag_state(ctx, image_path, inocnt);
  ctx->c_alloc_v3_summary_dirty = 1;
  if (mode == KAFS_V7_RUNTIME_MODE_CONTROLLED_WRITE)
    kafs_ctx_init_runtime_journal(ctx, image_path, r_blkcnt, 0);
  else if (mode != KAFS_V7_RUNTIME_MODE_INSPECTION)
    return -EINVAL;

  int rc = kafs_ctx_v6_validate_runtime_views(ctx);
  if (rc == 0)
    rc = kafs_ctx_v6_validate_worker_policy(ctx);
  if (rc != 0)
  {
    char errbuf[128];
    fprintf(err, "%s %s runtime policy failed after service init: %s.\n", KAFS_V7_TOOL_NAME,
            mode == KAFS_V7_RUNTIME_MODE_CONTROLLED_WRITE ? "controlled write" : "inspection",
            kafs_v7_runtime_rc_text(rc, errbuf, sizeof(errbuf)));
  }
  return rc;
}

static void kafs_v7_runtime_preflight_message_prefix(FILE *err, const char *tool_name)
{
  if (tool_name && tool_name[0] != '\0')
    fprintf(err, "%s: ", tool_name);
}

int kafs_v7_runtime_admission_preflight_fd(int fd, const kafs_ssuperblock_t *sbdisk, FILE *err,
                                           const char *tool_name)
{
  if (!err)
    err = stderr;
  int rc = kafs_v7_admission_preflight_core(fd, sbdisk);

  kafs_v7_runtime_preflight_message_prefix(err, tool_name);
  if (rc == 0)
  {
    fprintf(err,
            "format %s admission preflight: descriptor-backed metadata checks OK; "
            "runtime mount remains offline-only.\n",
            KAFS_V7_TOOL_FORMAT_LABEL);
  }
  else
  {
    char errbuf[128];
    fprintf(err, "format %s admission preflight failed: %s.\n", KAFS_V7_TOOL_FORMAT_LABEL,
            kafs_v7_runtime_rc_text(rc, errbuf, sizeof(errbuf)));
  }
  return rc;
}

int kafs_v7_runtime_admission_preflight_image(const char *image_path, FILE *err,
                                              const char *tool_name)
{
  if (!tool_name)
    tool_name = KAFS_V7_TOOL_NAME;
  if (!err)
    err = stderr;

  kafs_ssuperblock_t sb;
  int fd = -1;
  int rc = kafs_v7_runtime_open_readonly_superblock(image_path, &sb, err, tool_name, &fd);
  if (rc != 0)
    return rc;

  if (kafs_sb_magic_get(&sb) != KAFS_MAGIC)
  {
    fprintf(err, "%s: invalid KAFS magic in %s.\n", tool_name, image_path);
    close(fd);
    return -EINVAL;
  }
  if (kafs_sb_format_version_get(&sb) != KAFS_V7_TOOL_FORMAT_VERSION)
  {
    fprintf(err, "%s: image is format v%u; expected format %s.\n", tool_name,
            (unsigned)kafs_sb_format_version_get(&sb), KAFS_V7_TOOL_FORMAT_LABEL);
    close(fd);
    return -EPROTONOSUPPORT;
  }

  rc = kafs_v7_runtime_admission_preflight_fd(fd, &sb, err, tool_name);
  close(fd);
  return rc;
}
