#include "kafs.h"
#include "kafs_cli_opts.h"
#include "kafs_v6_runtime.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

typedef struct kafs_v6_options
{
  const char *image_path;
  const char *mountpoint;
  kafs_v6_runtime_request_t request;
  int show_help;
} kafs_v6_options_t;

static void usage(const char *prog)
{
  fprintf(stderr,
          "Usage:\n"
          "  %s --image <image> --inspection-mount <mountpoint> -o ro [FUSE options...]\n"
          "  %s --image <image> --controlled-write-mount <mountpoint> \\\n"
          "      -o rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full\n"
          "  %s <image> <mountpoint> --inspection-mount -o ro [FUSE options...]\n"
          "\n"
          "Options:\n"
          "  -h, --help                 Show this help and exit\n"
          "  --image <image>            Format v6 image path\n"
          "  --image=<image>            Format v6 image path (inline form)\n"
          "  --inspection-mount         Select the read-only v6 inspection contract\n"
          "  --controlled-write-mount   Select the experimental v6 controlled write contract\n"
          "  --option <opt[,opt...]>    Alias for FUSE -o\n"
          "  --option=<opt[,opt...]>    Inline form of --option\n"
          "\n"
          "Notes:\n"
          "  This is the dedicated format v6 runtime entrypoint skeleton. It validates\n"
          "  the v6 CLI boundary and image format, then fails closed before mounting.\n"
          "  Future v6 runtime admission work should move behind this binary instead of\n"
          "  broadening the production kafs binary.\n",
          prog, prog, prog);
}

static void kafs_v6_options_init(kafs_v6_options_t *opts)
{
  memset(opts, 0, sizeof(*opts));
  kafs_v6_runtime_request_init(&opts->request);
  opts->image_path = getenv("KAFS_IMAGE");
}

static int kafs_v6_set_mode(kafs_v6_options_t *opts, kafs_v6_runtime_mode_t mode)
{
  if (opts->request.mode != KAFS_V6_RUNTIME_MODE_NONE && opts->request.mode != mode)
  {
    fprintf(stderr, "kafs-v6 accepts exactly one runtime mode.\n");
    return -EINVAL;
  }
  opts->request.mode = mode;
  return 0;
}

static void kafs_v6_record_o_token(kafs_v6_options_t *opts, const char *tok)
{
  if (strcmp(tok, "ro") == 0)
  {
    opts->request.mount_read_only_requested = 1;
    opts->request.mount_read_only_seen = 1;
  }
  else if (strcmp(tok, "rw") == 0)
    opts->request.mount_read_write_requested = 1;
  else if (strcmp(tok, "no_writeback_cache") == 0 || strcmp(tok, "no-writeback-cache") == 0)
    opts->request.no_writeback_cache_requested = 1;
  else if (strcmp(tok, "writeback_cache") == 0 || strcmp(tok, "writeback-cache") == 0)
  {
    opts->request.writeback_cache_enabled = 1;
    opts->request.writeback_cache_explicit = 1;
  }
  else if (strcmp(tok, "no_trim_on_free") == 0 || strcmp(tok, "no-trim-on-free") == 0)
    opts->request.no_trim_on_free_requested = 1;
  else if (strcmp(tok, "trim_on_free") == 0 || strcmp(tok, "trim-on-free") == 0)
    opts->request.trim_on_free_enabled = 1;
  else if (strcmp(tok, "bg_dedup_scan=off") == 0 || strcmp(tok, "dedup_scan=off") == 0 ||
           strcmp(tok, "no_bg_dedup_scan") == 0 || strcmp(tok, "no-bg-dedup-scan") == 0)
    opts->request.bg_dedup_scan_off_requested = 1;
  else if (strcmp(tok, "bg_dedup_scan") == 0 || strcmp(tok, "dedup_scan") == 0 ||
           strcmp(tok, "bg_dedup_scan=on") == 0 || strcmp(tok, "dedup_scan=on") == 0)
  {
    opts->request.bg_dedup_scan_enabled = 1;
    opts->request.bg_dedup_scan_explicit = 1;
  }
  else if (strcmp(tok, "fsync_policy=full") == 0)
  {
    opts->request.fsync_policy_full_requested = 1;
    opts->request.fsync_policy = KAFS_FSYNC_POLICY_FULL;
  }
  else if (strncmp(tok, "fsync_policy=", strlen("fsync_policy=")) == 0)
    opts->request.fsync_policy_other_requested = 1;
  else if (strcmp(tok, "v6_inspection_mount") == 0 || strcmp(tok, "v6-inspection-mount") == 0 ||
           strcmp(tok, "v6_write_mount") == 0 || strcmp(tok, "v6-write-mount") == 0)
    opts->request.legacy_mode_token_seen = 1;
  else if (strcmp(tok, "hotplug") == 0 || strncmp(tok, "hotplug=", strlen("hotplug=")) == 0 ||
           strncmp(tok, "hotplug_uds=", strlen("hotplug_uds=")) == 0 ||
           strncmp(tok, "hotplug-uds=", strlen("hotplug-uds=")) == 0 ||
           strncmp(tok, "hotplug_back_bin=", strlen("hotplug_back_bin=")) == 0 ||
           strncmp(tok, "hotplug-back-bin=", strlen("hotplug-back-bin=")) == 0)
    opts->request.hotplug_requested = 1;
}

static int kafs_v6_parse_o_list(kafs_v6_options_t *opts, const char *value)
{
  char buf[1024];
  size_t len;
  char *save = NULL;

  if (!value || value[0] == '\0')
  {
    fprintf(stderr, "kafs-v6: empty -o option list.\n");
    return -EINVAL;
  }

  len = strlen(value);
  if (len >= sizeof(buf))
  {
    fprintf(stderr, "kafs-v6: -o option list is too long.\n");
    return -EINVAL;
  }
  memcpy(buf, value, len + 1u);

  for (char *tok = strtok_r(buf, ",", &save); tok; tok = strtok_r(NULL, ",", &save))
  {
    while (*tok == ' ' || *tok == '\t')
      tok++;
    if (*tok == '\0')
    {
      fprintf(stderr, "kafs-v6: empty token in -o option list.\n");
      return -EINVAL;
    }
    kafs_v6_record_o_token(opts, tok);
  }
  return 0;
}

static int kafs_v6_parse_args(int argc, char **argv, kafs_v6_options_t *opts)
{
  for (int i = 1; i < argc; ++i)
  {
    const char *arg = argv[i];

    if (kafs_cli_is_help_arg(arg))
    {
      opts->show_help = 1;
      continue;
    }
    if (strcmp(arg, "--image") == 0)
    {
      if (i + 1 >= argc)
      {
        usage(argv[0]);
        return -EINVAL;
      }
      opts->image_path = argv[++i];
      continue;
    }
    if (strncmp(arg, "--image=", strlen("--image=")) == 0)
    {
      opts->image_path = arg + strlen("--image=");
      continue;
    }
    if (strcmp(arg, "--inspection-mount") == 0)
    {
      if (kafs_v6_set_mode(opts, KAFS_V6_RUNTIME_MODE_INSPECTION) != 0)
        return -EINVAL;
      continue;
    }
    if (strcmp(arg, "--controlled-write-mount") == 0)
    {
      if (kafs_v6_set_mode(opts, KAFS_V6_RUNTIME_MODE_CONTROLLED_WRITE) != 0)
        return -EINVAL;
      continue;
    }
    if (strcmp(arg, "--v6-inspection-mount") == 0 || strcmp(arg, "--v6-write-mount") == 0)
    {
      opts->request.legacy_mode_token_seen = 1;
      continue;
    }
    if (strcmp(arg, "--option") == 0 || strcmp(arg, "-o") == 0)
    {
      if (i + 1 >= argc || kafs_v6_parse_o_list(opts, argv[++i]) != 0)
        return -EINVAL;
      continue;
    }
    if (strncmp(arg, "--option=", strlen("--option=")) == 0)
    {
      if (kafs_v6_parse_o_list(opts, arg + strlen("--option=")) != 0)
        return -EINVAL;
      continue;
    }
    if (arg[0] == '-' && arg[1] == 'o' && arg[2] != '\0')
    {
      if (kafs_v6_parse_o_list(opts, arg + 2) != 0)
        return -EINVAL;
      continue;
    }
    if (strcmp(arg, "--hotplug") == 0 || strncmp(arg, "--hotplug=", strlen("--hotplug=")) == 0 ||
        strcmp(arg, "--hotplug-uds") == 0 ||
        strncmp(arg, "--hotplug-uds=", strlen("--hotplug-uds=")) == 0 ||
        strcmp(arg, "--hotplug-back-bin") == 0 ||
        strncmp(arg, "--hotplug-back-bin=", strlen("--hotplug-back-bin=")) == 0)
    {
      opts->request.hotplug_requested = 1;
      if ((strcmp(arg, "--hotplug-uds") == 0 || strcmp(arg, "--hotplug-back-bin") == 0) &&
          i + 1 < argc)
        i++;
      continue;
    }
    if (arg[0] == '-')
      continue;

    if (!opts->image_path)
      opts->image_path = arg;
    else if (!opts->mountpoint)
      opts->mountpoint = arg;
  }
  return 0;
}

static void kafs_v6_print_validation_error(kafs_v6_runtime_validation_reason_t reason)
{
  switch (reason)
  {
  case KAFS_V6_RUNTIME_INVALID_NO_MODE:
    fprintf(stderr, "kafs-v6 requires --inspection-mount or --controlled-write-mount.\n");
    return;
  case KAFS_V6_RUNTIME_INVALID_LEGACY_MODE_TOKEN:
    fprintf(stderr, "kafs-v6 owns the v6 runtime mode; do not pass legacy v6_* mount options.\n");
    return;
  case KAFS_V6_RUNTIME_INVALID_HOTPLUG:
    fprintf(stderr, "kafs-v6 does not admit hotplug delegated write options.\n");
    return;
  case KAFS_V6_RUNTIME_INVALID_INSPECTION_NEEDS_RO:
    fprintf(stderr, "kafs-v6 inspection mode requires -o ro and does not allow -o rw.\n");
    return;
  case KAFS_V6_RUNTIME_INVALID_CONTROLLED_RO:
    fprintf(stderr, "kafs-v6 controlled write mode does not allow -o ro.\n");
    return;
  case KAFS_V6_RUNTIME_INVALID_CONTROLLED_UNSAFE_WRITEBACK:
  case KAFS_V6_RUNTIME_INVALID_CONTROLLED_UNSAFE_TRIM:
  case KAFS_V6_RUNTIME_INVALID_CONTROLLED_UNSAFE_BG_DEDUP:
  case KAFS_V6_RUNTIME_INVALID_CONTROLLED_UNSAFE_FSYNC:
    fprintf(stderr, "kafs-v6 controlled write mode rejected unsafe mount options.\n");
    return;
  case KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_RW:
  case KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_NO_WRITEBACK:
  case KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_NO_TRIM:
  case KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_BG_OFF:
  case KAFS_V6_RUNTIME_INVALID_CONTROLLED_NEEDS_FSYNC_FULL:
    fprintf(stderr,
            "kafs-v6 controlled write mode requires "
            "-o rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full.\n");
    return;
  case KAFS_V6_RUNTIME_VALID:
  case KAFS_V6_RUNTIME_INVALID_INSPECTION_WRITEBACK_CACHE:
  case KAFS_V6_RUNTIME_INVALID_CONTROLLED_WITH_INSPECTION:
  default:
    fprintf(stderr, "kafs-v6 rejected invalid v6 runtime admission options.\n");
    return;
  }
}

static int kafs_v6_validate_options(const kafs_v6_options_t *opts)
{
  if (!opts->image_path || !opts->mountpoint)
  {
    fprintf(stderr, "kafs-v6 requires an image path and mountpoint.\n");
    return -EINVAL;
  }

  kafs_v6_runtime_validation_reason_t reason = KAFS_V6_RUNTIME_VALID;
  if (kafs_v6_runtime_validate_entrypoint_request(&opts->request, &reason) != 0)
  {
    kafs_v6_print_validation_error(reason);
    return -EINVAL;
  }
  return 0;
}

int main(int argc, char **argv)
{
  kafs_v6_options_t opts;

  kafs_v6_options_init(&opts);
  if (kafs_v6_parse_args(argc, argv, &opts) != 0)
    return 2;
  if (opts.show_help)
  {
    usage(argv[0]);
    return 0;
  }
  if (kafs_v6_validate_options(&opts) != 0)
  {
    usage(argv[0]);
    return 2;
  }
  if (kafs_v6_runtime_check_image_format(opts.image_path, KAFS_FORMAT_VERSION_V6, stderr,
                                         "kafs-v6") != 0)
    return 2;

  fprintf(stderr, "kafs-v6: format v6 runtime entrypoint skeleton validated the CLI boundary and "
                  "image format, then failed closed before mounting.\n");
  fprintf(stderr, "kafs-v6: move future v6 runtime admission behind this binary before expanding "
                  "the v6 write surface.\n");
  return 2;
}
