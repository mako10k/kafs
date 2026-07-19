#include "kafs_cli_opts.h"
#include "kafs_v7_entrypoint_adapter.h"
#include "kafs_v7_mount_options.h"
#include "kafs_v7_runtime.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef KAFS_V7_TOOL_NAME
#define KAFS_V7_TOOL_NAME "kafs-v7"
#endif

#ifndef KAFS_V7_TOOL_FORMAT_LABEL
#define KAFS_V7_TOOL_FORMAT_LABEL "v7"
#endif

typedef struct kafs_v7_options
{
  const char *image_path;
  const char *mountpoint;
  kafs_v7_runtime_request_t request;
  char **fuse_args;
  int fuse_argc;
  int fuse_arg_cap;
  int show_help;
} kafs_v7_options_t;

static void usage(const char *prog)
{
  fprintf(stderr,
          "Usage:\n"
          "  %s --image <image> --inspection-mount <mountpoint> -o ro [FUSE options...]\n"
          "  %s <image> <mountpoint> --inspection-mount -o ro [FUSE options...]\n"
          "\n"
          "Options:\n"
          "  -h, --help                 Show this help and exit\n"
          "  --image <image>            Format " KAFS_V7_TOOL_FORMAT_LABEL " image path\n"
          "  --image=<image>            Format " KAFS_V7_TOOL_FORMAT_LABEL
          " image path (inline form)\n"
          "  --inspection-mount         Select the read-only " KAFS_V7_TOOL_FORMAT_LABEL
          " inspection contract\n"
          "  --controlled-write-mount   Select bounded aligned direct-block overwrites\n"
          "  --option <opt[,opt...]>    Alias for FUSE -o\n"
          "  --option=<opt[,opt...]>    Inline form of --option\n"
          "\n"
          "Notes:\n"
          "  This is the dedicated format " KAFS_V7_TOOL_FORMAT_LABEL
          " runtime entrypoint. It owns " KAFS_V7_TOOL_FORMAT_LABEL " CLI\n"
          "  admission and rejects legacy v6_* mount tokens. Inspection accepts only\n"
          "  a validated descriptor/checkpoint pair at checkpoint_seq=0 with empty\n"
          "  journal segments. The image is opened and mapped read-only, and all\n"
          "  mutation operations fail with EROFS. Controlled write requires the explicit\n"
          "  safe mount options and admits only aligned full direct-block overwrites.\n"
          "  Non-empty journal replay, repair, and other mutations fail closed.\n",
          prog, prog);
}

static void kafs_v7_options_init(kafs_v7_options_t *opts, char **fuse_args, int fuse_arg_cap)
{
  memset(opts, 0, sizeof(*opts));
  kafs_v7_runtime_request_init(&opts->request);
  opts->image_path = getenv("KAFS_IMAGE");
  opts->fuse_args = fuse_args;
  opts->fuse_arg_cap = fuse_arg_cap;
}

static int kafs_v7_add_fuse_arg(kafs_v7_options_t *opts, char *arg)
{
  if (!opts || !arg || opts->fuse_argc >= opts->fuse_arg_cap)
  {
    fprintf(stderr, "%s: too many FUSE passthrough arguments.\n", KAFS_V7_TOOL_NAME);
    return -EINVAL;
  }
  opts->fuse_args[opts->fuse_argc++] = arg;
  return 0;
}

static int kafs_v7_set_mode(kafs_v7_options_t *opts, kafs_v7_runtime_mode_t mode)
{
  if (opts->request.mode != KAFS_V7_RUNTIME_MODE_NONE && opts->request.mode != mode)
  {
    fprintf(stderr, "%s accepts exactly one runtime mode.\n", KAFS_V7_TOOL_NAME);
    return -EINVAL;
  }
  opts->request.mode = mode;
  return 0;
}

static int kafs_v7_parse_o_list(kafs_v7_options_t *opts, const char *value)
{
  char buf[1024];
  size_t len;
  char *save = NULL;

  if (!value || value[0] == '\0')
  {
    fprintf(stderr, "%s: empty -o option list.\n", KAFS_V7_TOOL_NAME);
    return -EINVAL;
  }

  len = strlen(value);
  if (len >= sizeof(buf))
  {
    fprintf(stderr, "%s: -o option list is too long.\n", KAFS_V7_TOOL_NAME);
    return -EINVAL;
  }
  memcpy(buf, value, len + 1u);

  for (char *tok = strtok_r(buf, ",", &save); tok; tok = strtok_r(NULL, ",", &save))
  {
    while (*tok == ' ' || *tok == '\t')
      tok++;
    if (*tok == '\0')
    {
      fprintf(stderr, "%s: empty token in -o option list.\n", KAFS_V7_TOOL_NAME);
      return -EINVAL;
    }
    if (kafs_v7_mount_options_record_runtime_token(&opts->request, tok) != 0)
      return -EINVAL;
  }
  return 0;
}

static int kafs_v7_parse_args(int argc, char **argv, kafs_v7_options_t *opts)
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
      if (kafs_v7_set_mode(opts, KAFS_V7_RUNTIME_MODE_INSPECTION) != 0)
        return -EINVAL;
      continue;
    }
    if (strcmp(arg, "--controlled-write-mount") == 0)
    {
      if (kafs_v7_set_mode(opts, KAFS_V7_RUNTIME_MODE_CONTROLLED_WRITE) != 0)
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
      if (i + 1 >= argc)
        return -EINVAL;
      const char *value = argv[++i];
      if (kafs_v7_parse_o_list(opts, value) != 0 || kafs_v7_add_fuse_arg(opts, "-o") != 0 ||
          kafs_v7_add_fuse_arg(opts, (char *)value) != 0)
        return -EINVAL;
      continue;
    }
    if (strncmp(arg, "--option=", strlen("--option=")) == 0)
    {
      const char *value = arg + strlen("--option=");
      if (kafs_v7_parse_o_list(opts, value) != 0 || kafs_v7_add_fuse_arg(opts, "-o") != 0 ||
          kafs_v7_add_fuse_arg(opts, (char *)value) != 0)
        return -EINVAL;
      continue;
    }
    if (arg[0] == '-' && arg[1] == 'o' && arg[2] != '\0')
    {
      if (kafs_v7_parse_o_list(opts, arg + 2) != 0 || kafs_v7_add_fuse_arg(opts, (char *)arg) != 0)
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
    {
      if (kafs_v7_add_fuse_arg(opts, (char *)arg) != 0)
        return -EINVAL;
      continue;
    }

    if (!opts->image_path)
      opts->image_path = arg;
    else if (!opts->mountpoint)
      opts->mountpoint = arg;
  }
  return 0;
}

static int kafs_v7_validate_options(const kafs_v7_options_t *opts)
{
  if (!opts->image_path || !opts->mountpoint)
  {
    fprintf(stderr, "%s requires an image path and mountpoint.\n", KAFS_V7_TOOL_NAME);
    return -EINVAL;
  }

  if (kafs_v7_runtime_report_entrypoint_request(&opts->request, stderr) != 0)
    return -EINVAL;
  return 0;
}

static kafs_v7_entrypoint_adapter_mode_t
kafs_v7_adapter_mode_from_runtime(kafs_v7_runtime_mode_t mode)
{
  switch (mode)
  {
  case KAFS_V7_RUNTIME_MODE_INSPECTION:
    return KAFS_V7_ENTRYPOINT_ADAPTER_MODE_INSPECTION;
  case KAFS_V7_RUNTIME_MODE_CONTROLLED_WRITE:
    return KAFS_V7_ENTRYPOINT_ADAPTER_MODE_CONTROLLED_WRITE;
  case KAFS_V7_RUNTIME_MODE_NONE:
  default:
    return KAFS_V7_ENTRYPOINT_ADAPTER_MODE_NONE;
  }
}

static void kafs_v7_adapter_options_from_request(kafs_v7_entrypoint_adapter_options_t *out,
                                                 const kafs_v7_runtime_request_t *req)
{
  memset(out, 0, sizeof(*out));
  out->mode = kafs_v7_adapter_mode_from_runtime(req->mode);
  out->legacy_mode_token_seen = req->legacy_mode_token_seen;
  out->hotplug_requested = req->hotplug_requested;
  out->mount_read_only_requested = req->mount_read_only_requested;
  out->mount_read_only_seen = req->mount_read_only_seen;
  out->mount_read_write_requested = req->mount_read_write_requested;
  out->no_writeback_cache_requested = req->no_writeback_cache_requested;
  out->writeback_cache_enabled = req->writeback_cache_enabled;
  out->writeback_cache_explicit =
      req->writeback_cache_explicit || req->no_writeback_cache_requested;
  out->no_trim_on_free_requested = req->no_trim_on_free_requested;
  out->trim_on_free_enabled = req->trim_on_free_enabled;
  out->trim_on_free_explicit = req->trim_on_free_enabled || req->no_trim_on_free_requested;
  out->bg_dedup_scan_off_requested = req->bg_dedup_scan_off_requested;
  out->bg_dedup_scan_enabled = req->bg_dedup_scan_enabled;
  out->fsync_policy_full_requested = req->fsync_policy_full_requested;
  out->fsync_policy_other_requested = req->fsync_policy_other_requested;
  out->fsync_policy = req->fsync_policy;
}

int main(int argc, char **argv)
{
  kafs_v7_options_t opts;
  char *fuse_args[(argc > 0 ? argc : 1) * 2 + 1];

  kafs_v7_options_init(&opts, fuse_args, (int)(sizeof(fuse_args) / sizeof(fuse_args[0])));
  if (kafs_v7_parse_args(argc, argv, &opts) != 0)
    return 2;
  if (opts.show_help)
  {
    usage(argv[0]);
    return 0;
  }
  if (kafs_v7_validate_options(&opts) != 0)
  {
    usage(argv[0]);
    return 2;
  }
  if (kafs_v7_runtime_admission_preflight_image(opts.image_path, stderr, KAFS_V7_TOOL_NAME) != 0)
    return 2;

  kafs_v7_entrypoint_adapter_options_t adapter_opts;
  kafs_v7_adapter_options_from_request(&adapter_opts, &opts.request);
  return kafs_v7_entrypoint_adapter_mount_main(opts.image_path, opts.mountpoint, opts.fuse_argc,
                                               opts.fuse_args, &adapter_opts, stderr);
}
