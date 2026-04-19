#include "kafs_ioctl.h"
#include "kafs_cli_opts.h"
#include "kafs_core.h"
#include "kafs_rpc.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <limits.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>

typedef enum
{
  KAFS_UNIT_KIB = 0,
  KAFS_UNIT_BYTES,
  KAFS_UNIT_MIB,
  KAFS_UNIT_GIB,
} kafs_unit_t;

static double unit_divisor(kafs_unit_t u)
{
  switch (u)
  {
  case KAFS_UNIT_BYTES:
    return 1.0;
  case KAFS_UNIT_MIB:
    return 1024.0 * 1024.0;
  case KAFS_UNIT_GIB:
    return 1024.0 * 1024.0 * 1024.0;
  case KAFS_UNIT_KIB:
  default:
    return 1024.0;
  }
}

static const char *unit_suffix(kafs_unit_t u)
{
  switch (u)
  {
  case KAFS_UNIT_BYTES:
    return "B";
  case KAFS_UNIT_MIB:
    return "MiB";
  case KAFS_UNIT_GIB:
    return "GiB";
  case KAFS_UNIT_KIB:
  default:
    return "KiB";
  }
}

static void print_bytes(uint64_t bytes, kafs_unit_t unit)
{
  if (unit == KAFS_UNIT_BYTES)
  {
    printf("%" PRIu64 "B", bytes);
    return;
  }

  const double v = (double)bytes / unit_divisor(unit);
  if (v >= 100.0)
    printf("%.0f%s", v, unit_suffix(unit));
  else
    printf("%.1f%s", v, unit_suffix(unit));
}

static double pct_u64(uint64_t used, uint64_t total)
{
  if (total == 0)
    return 0.0;
  return ((double)used * 100.0) / (double)total;
}

static void fmt_time(char out[64], const struct timespec *ts);
static const char *to_kafs_path(const char *mnt_abs, const char *p, char out[KAFS_IOCTL_PATH_MAX]);
static const char *to_mount_rel_path(const char *mnt_abs, const char *p,
                                     char out[KAFS_IOCTL_PATH_MAX]);

static void usage(const char *prog)
{
  fprintf(stderr,
          "Usage:\n"
          "  %s migrate <image> [--yes]\n"
          "  %s fsstat <mountpoint> [-v|--verbose] [--json] [--bytes|--mib|--gib]"
          "   (alias: stats)\n"
          "  %s hotplug status <mountpoint> [--json]\n"
          "  %s hotplug restart-back <mountpoint>   (supervised back restart)\n"
          "  %s hotplug compat <mountpoint> [--json]\n"
          "  %s hotplug set-timeout <mountpoint> <ms>\n"
          "  %s hotplug set-dedup-priority <mountpoint> <normal|idle> [nice(0..19)]\n"
          "  %s hotplug set-runtime <mountpoint> [--fsync-policy=<journal_only|full|adaptive>]"
          " [--pending-ttl-soft-ms=<ms>] [--pending-ttl-hard-ms=<ms>]\n"
          "  %s hotplug env list <mountpoint>\n"
          "  %s hotplug env set <mountpoint> <key>=<value>\n"
          "  %s hotplug env unset <mountpoint> <key>\n"
          "  %s stat <path>\n"
          "  %s cat <path>\n"
          "  %s write <path>   (stdin -> file, trunc)\n"
          "  %s cp <mountpoint> <src> <dst> [--reflink]\n"
          "  %s mv <mountpoint> <src> <dst>\n"
          "  %s rm <path>\n"
          "  %s mkdir <path>\n"
          "  %s rmdir <path>\n"
          "  %s ln <src> <dst>\n"
          "  %s ln -s <target> <linkpath>\n"
          "  %s symlink <target> <linkpath>\n"
          "  %s rsync [rsync options...] <src>... <dst>\n"
          "  %s readlink <path>\n"
          "  %s chmod <octal_mode> <path>\n"
          "  %s touch <path>\n",
          prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog,
          prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog);
}

static void usage_cmd(const char *prog, const char *suffix, const char *details)
{
  fprintf(stderr, "Usage: %s %s\n", prog, suffix);
  if (details && *details)
    fprintf(stderr, "%s", details);
}

static void usage_migrate_cmd(const char *prog)
{
  usage_cmd(prog, "migrate <image> [--yes]",
            "\nOffline pre-start migration from v2/v3 to v4. This operation is irreversible.\n");
}

static void usage_fsstat_cmd(const char *prog)
{
  usage_cmd(prog, "fsstat <mountpoint> [-v|--verbose] [--json] [--bytes|--mib|--gib]",
            "       stats <mountpoint> [-v|--verbose] [--json] [--bytes|--mib|--gib]\n");
}

static void usage_hotplug_cmd(const char *prog)
{
  usage_cmd(
      prog,
      "hotplug <status|restart-back|compat|set-timeout|set-dedup-priority|set-runtime|env> ...",
      NULL);
}

static void usage_hotplug_status_cmd(const char *prog)
{
  usage_cmd(prog, "hotplug status <mountpoint> [--json]", NULL);
}

static void usage_hotplug_restart_cmd(const char *prog)
{
  usage_cmd(prog, "hotplug restart-back <mountpoint>", NULL);
}

static void usage_hotplug_compat_cmd(const char *prog)
{
  usage_cmd(prog, "hotplug compat <mountpoint> [--json]", NULL);
}

static void usage_hotplug_timeout_cmd(const char *prog)
{
  usage_cmd(prog, "hotplug set-timeout <mountpoint> <ms>", NULL);
}

static void usage_hotplug_dedup_priority_cmd(const char *prog)
{
  usage_cmd(prog, "hotplug set-dedup-priority <mountpoint> <normal|idle> [nice(0..19)]", NULL);
}

static void usage_hotplug_set_runtime_cmd(const char *prog)
{
  usage_cmd(prog,
            "hotplug set-runtime <mountpoint> [--fsync-policy=<journal_only|full|adaptive>] "
            "[--pending-ttl-soft-ms=<ms>] [--pending-ttl-hard-ms=<ms>]",
            NULL);
}

static void usage_hotplug_env_cmd(const char *prog)
{
  usage_cmd(prog, "hotplug env <list|set|unset> ...", NULL);
}

static void usage_hotplug_env_list_cmd(const char *prog)
{
  usage_cmd(prog, "hotplug env list <mountpoint>", NULL);
}

static void usage_hotplug_env_set_cmd(const char *prog)
{
  usage_cmd(prog, "hotplug env set <mountpoint> <key>=<value>", NULL);
}

static void usage_hotplug_env_unset_cmd(const char *prog)
{
  usage_cmd(prog, "hotplug env unset <mountpoint> <key>", NULL);
}

static void usage_path_cmd(const char *prog, const char *cmd, const char *args)
{
  char suffix[256];
  snprintf(suffix, sizeof(suffix), "%s %s", cmd, args);
  usage_cmd(prog, suffix, NULL);
}

static void usage_cp_cmd(const char *prog)
{
  usage_cmd(prog, "cp <src> <dst> [--reflink]\n       cp <mountpoint> <src> <dst> [--reflink]",
            NULL);
}

static void usage_mv_cmd(const char *prog)
{
  usage_cmd(prog, "mv <src> <dst>\n       mv <mountpoint> <src> <dst>", NULL);
}

static void usage_ln_cmd(const char *prog)
{
  usage_cmd(prog,
            "ln [-f] [-n] [-T] [-v] <src> <dst>\n"
            "       ln [-f] [-n] [-T] [-v] <mountpoint> <src> <dst>\n"
            "       ln [-f] [-n] [-v] -t <dir> <src>\n"
            "       ln [-f] [-n] [-v] -t <dir> <mountpoint> <src>\n"
            "       ln -s [-f] [-n] [-T] [-v] <target> <linkpath>\n"
            "       ln -s [-f] [-n] [-T] [-v] <mountpoint> <target> <linkpath>\n"
            "       ln -s [-f] [-n] [-v] -t <dir> <target>\n"
            "       ln -s [-f] [-n] [-v] -t <dir> <mountpoint> <target>\n"
            "       ln --symbolic ...  (alias of -s)",
            NULL);
}

static void usage_symlink_cmd(const char *prog)
{
  usage_cmd(prog, "symlink <target> <linkpath>\n       symlink <mountpoint> <target> <linkpath>",
            NULL);
}

static void usage_rsync_cmd(const char *prog)
{
  usage_cmd(prog, "rsync [rsync options...] <src>... <dst>", NULL);
}

static void usage_single_path_cmd(const char *prog, const char *cmd)
{
  char suffix[256];
  snprintf(suffix, sizeof(suffix), "%s <path>\n       %s <mountpoint> <path>", cmd, cmd);
  usage_cmd(prog, suffix, NULL);
}

static void usage_chmod_cmd(const char *prog)
{
  usage_cmd(prog, "chmod <octal_mode> <path>\n       chmod <mountpoint> <octal_mode> <path>", NULL);
}

static int usage_primary_direct_cmd(const char *prog, const char *cmd)
{
  if (strcmp(cmd, "migrate") == 0)
    usage_migrate_cmd(prog);
  else if (strcmp(cmd, "fsstat") == 0 || strcmp(cmd, "stats") == 0)
    usage_fsstat_cmd(prog);
  else if (strcmp(cmd, "hotplug") == 0)
    usage_hotplug_cmd(prog);
  else if (strcmp(cmd, "cp") == 0)
    usage_cp_cmd(prog);
  else if (strcmp(cmd, "mv") == 0)
    usage_mv_cmd(prog);
  else if (strcmp(cmd, "ln") == 0)
    usage_ln_cmd(prog);
  else if (strcmp(cmd, "symlink") == 0)
    usage_symlink_cmd(prog);
  else if (strcmp(cmd, "rsync") == 0)
    usage_rsync_cmd(prog);
  else if (strcmp(cmd, "chmod") == 0)
    usage_chmod_cmd(prog);
  else
    return 0;

  return 1;
}

static int usage_primary_single_path(const char *prog, const char *cmd)
{
  static const char *const names[] = {"stat",  "cat",   "write",    "rm",
                                      "mkdir", "rmdir", "readlink", "touch"};
  for (size_t index = 0; index < sizeof(names) / sizeof(names[0]); ++index)
  {
    if (strcmp(cmd, names[index]) == 0)
    {
      usage_single_path_cmd(prog, names[index]);
      return 1;
    }
  }

  return 0;
}

static void usage_primary_cmd(const char *prog, const char *cmd)
{
  if (!usage_primary_direct_cmd(prog, cmd) && !usage_primary_single_path(prog, cmd))
    usage(prog);
}

static void usage_hotplug_subcommand(const char *prog, const char *cmd)
{
  if (strcmp(cmd, "status") == 0)
    usage_hotplug_status_cmd(prog);
  else if (strcmp(cmd, "restart-back") == 0)
    usage_hotplug_restart_cmd(prog);
  else if (strcmp(cmd, "compat") == 0)
    usage_hotplug_compat_cmd(prog);
  else if (strcmp(cmd, "set-timeout") == 0)
    usage_hotplug_timeout_cmd(prog);
  else if (strcmp(cmd, "set-dedup-priority") == 0)
    usage_hotplug_dedup_priority_cmd(prog);
  else if (strcmp(cmd, "set-runtime") == 0)
    usage_hotplug_set_runtime_cmd(prog);
  else if (strcmp(cmd, "env") == 0)
    usage_hotplug_env_cmd(prog);
  else
    usage_hotplug_cmd(prog);
}

static void usage_hotplug_env_subcommand(const char *prog, const char *cmd)
{
  if (strcmp(cmd, "list") == 0)
    usage_hotplug_env_list_cmd(prog);
  else if (strcmp(cmd, "set") == 0)
    usage_hotplug_env_set_cmd(prog);
  else if (strcmp(cmd, "unset") == 0)
    usage_hotplug_env_unset_cmd(prog);
  else
    usage_hotplug_env_cmd(prog);
}

static void usage_help_target(const char *prog, int argc, char **argv)
{
  if (argc == 2)
  {
    usage(prog);
    return;
  }

  if (strcmp(argv[2], "hotplug") != 0)
  {
    usage_primary_cmd(prog, argv[2]);
    return;
  }

  if (argc == 3)
  {
    usage_hotplug_cmd(prog);
    return;
  }

  if (strcmp(argv[3], "env") != 0)
  {
    usage_hotplug_subcommand(prog, argv[3]);
    return;
  }

  if (argc == 4)
  {
    usage_hotplug_env_cmd(prog);
    return;
  }

  usage_hotplug_env_subcommand(prog, argv[4]);
}

static int try_subcommand_help(int argc, char **argv)
{
  if (argc < 2)
    return -1;

  if (kafs_cli_is_help_arg(argv[1]))
  {
    usage(argv[0]);
    return 0;
  }

  if (strcmp(argv[1], "help") == 0)
  {
    usage_help_target(argv[0], argc, argv);
    return 0;
  }

  if (argc >= 3 && kafs_cli_is_help_arg(argv[2]))
  {
    usage_primary_cmd(argv[0], argv[1]);
    return 0;
  }

  if (argc >= 4 && strcmp(argv[1], "hotplug") == 0 && kafs_cli_is_help_arg(argv[3]))
  {
    usage_hotplug_subcommand(argv[0], argv[2]);
    return 0;
  }

  if (argc >= 5 && strcmp(argv[1], "hotplug") == 0 && strcmp(argv[2], "env") == 0 &&
      kafs_cli_is_help_arg(argv[4]))
  {
    usage_hotplug_env_subcommand(argv[0], argv[3]);
    return 0;
  }

  return -1;
}

static int cmd_migrate(const char *image, int assume_yes)
{
  int rc = kafs_core_migrate_image(image, assume_yes);
  if (rc == 0)
  {
    fprintf(stderr, "migration completed: v2/v3 -> v%u (%s)\n", (unsigned)KAFS_FORMAT_VERSION,
            image);
    return 0;
  }
  if (rc == 1)
  {
    fprintf(stderr, "already v%u: no migration needed\n", (unsigned)KAFS_FORMAT_VERSION);
    return 0;
  }
  if (rc == -ECANCELED)
  {
    fprintf(stderr, "migration canceled by user\n");
    return 2;
  }
  if (rc == -EINVAL)
  {
    fprintf(stderr, "invalid magic: not a KAFS image\n");
    return 2;
  }
  if (rc == -EPROTONOSUPPORT)
  {
    fprintf(stderr, "unsupported format version for migration\n");
    return 2;
  }
  fprintf(stderr, "migration failed: %s\n", strerror(-rc));
  return 1;
}

static const char *hotplug_state_str(uint32_t state)
{
  switch (state)
  {
  case KAFS_HOTPLUG_STATE_DISABLED:
    return "disabled";
  case KAFS_HOTPLUG_STATE_WAITING:
    return "waiting";
  case KAFS_HOTPLUG_STATE_CONNECTED:
    return "connected";
  case KAFS_HOTPLUG_STATE_ERROR:
    return "error";
  default:
    return "unknown";
  }
}

static const char *hotplug_data_mode_str(uint32_t mode)
{
  switch (mode)
  {
  case KAFS_RPC_DATA_INLINE:
    return "inline";
  case KAFS_RPC_DATA_PLAN_ONLY:
    return "plan_only";
  case KAFS_RPC_DATA_SHM:
    return "shm";
  default:
    return "unknown";
  }
}

static const char *hotplug_compat_str(uint32_t result)
{
  switch (result)
  {
  case KAFS_HOTPLUG_COMPAT_OK:
    return "ok";
  case KAFS_HOTPLUG_COMPAT_WARN:
    return "warn";
  case KAFS_HOTPLUG_COMPAT_REJECT:
    return "reject";
  case KAFS_HOTPLUG_COMPAT_UNKNOWN:
  default:
    return "unknown";
  }
}

static const char *hotplug_compat_reason_str(int32_t reason)
{
  switch (reason)
  {
  case 0:
    return "ok";
  case -EPROTONOSUPPORT:
    return "protocol_mismatch";
  case -EBADMSG:
    return "bad_message";
  default:
    return "unknown";
  }
}

static const char *pending_worker_prio_mode_str(uint32_t mode)
{
  switch (mode)
  {
  case KAFS_PENDING_WORKER_PRIO_IDLE:
    return "idle";
  case KAFS_PENDING_WORKER_PRIO_NORMAL:
  default:
    return "normal";
  }
}

static const char *bg_dedup_mode_str(uint32_t mode)
{
  switch (mode)
  {
  case 1:
    return "cold";
  case 2:
    return "adaptive";
  case 3:
    return "pressure";
  default:
    return "unknown";
  }
}

static int hotplug_ctl_exchange(const char *mnt, uint16_t op, const void *req, uint32_t req_len,
                                void *resp, uint32_t resp_cap, uint32_t *resp_len,
                                int32_t *resp_result);
static void hotplug_print_exchange_error(const char *op, const char *mnt, int rc);

static const char *fsync_policy_str(uint32_t policy)
{
  switch (policy)
  {
  case KAFS_FSYNC_POLICY_FULL:
    return "full";
  case KAFS_FSYNC_POLICY_ADAPTIVE:
    return "adaptive";
  case KAFS_FSYNC_POLICY_JOURNAL_ONLY:
  default:
    return "journal_only";
  }
}

static int parse_fsync_policy(const char *s, uint32_t *out)
{
  if (!s || !out)
    return -EINVAL;
  if (strcmp(s, "full") == 0)
  {
    *out = KAFS_FSYNC_POLICY_FULL;
    return 0;
  }
  if (strcmp(s, "journal_only") == 0 || strcmp(s, "journal-only") == 0 || strcmp(s, "journal") == 0)
  {
    *out = KAFS_FSYNC_POLICY_JOURNAL_ONLY;
    return 0;
  }
  if (strcmp(s, "adaptive") == 0)
  {
    *out = KAFS_FSYNC_POLICY_ADAPTIVE;
    return 0;
  }
  return -EINVAL;
}

static int cmd_hotplug_set_runtime(const char *mnt, int argc, char **argv)
{
  kafs_rpc_set_runtime_t req;
  memset(&req, 0, sizeof(req));

  for (int i = 0; i < argc; ++i)
  {
    const char *a = argv[i];
    if (strncmp(a, "--fsync-policy=", 15) == 0)
    {
      if (parse_fsync_policy(a + 15, &req.fsync_policy) != 0)
      {
        fprintf(stderr, "invalid fsync policy: %s\n", a + 15);
        return 2;
      }
      req.flags |= KAFS_RPC_SET_RUNTIME_F_HAS_FSYNC_POLICY;
      continue;
    }
    if (strncmp(a, "--pending-ttl-soft-ms=", 22) == 0)
    {
      char *endp = NULL;
      unsigned long v = strtoul(a + 22, &endp, 10);
      if (!endp || *endp != '\0' || v > UINT32_MAX)
      {
        fprintf(stderr, "invalid pending ttl soft: %s\n", a + 22);
        return 2;
      }
      req.pending_ttl_soft_ms = (uint32_t)v;
      req.flags |= KAFS_RPC_SET_RUNTIME_F_HAS_PENDING_TTL_SOFT_MS;
      continue;
    }
    if (strncmp(a, "--pending-ttl-hard-ms=", 22) == 0)
    {
      char *endp = NULL;
      unsigned long v = strtoul(a + 22, &endp, 10);
      if (!endp || *endp != '\0' || v > UINT32_MAX)
      {
        fprintf(stderr, "invalid pending ttl hard: %s\n", a + 22);
        return 2;
      }
      req.pending_ttl_hard_ms = (uint32_t)v;
      req.flags |= KAFS_RPC_SET_RUNTIME_F_HAS_PENDING_TTL_HARD_MS;
      continue;
    }

    fprintf(stderr, "unknown option: %s\n", a);
    return 2;
  }

  if (req.flags == 0)
  {
    fprintf(stderr, "set-runtime requires at least one option\n");
    return 2;
  }

  if ((req.flags & KAFS_RPC_SET_RUNTIME_F_HAS_PENDING_TTL_SOFT_MS) != 0 &&
      (req.flags & KAFS_RPC_SET_RUNTIME_F_HAS_PENDING_TTL_HARD_MS) != 0 &&
      req.pending_ttl_soft_ms > 0 && req.pending_ttl_hard_ms > 0 &&
      req.pending_ttl_hard_ms < req.pending_ttl_soft_ms)
  {
    fprintf(stderr, "invalid ttl: hard must be >= soft\n");
    return 2;
  }

  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc = hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_SET_RUNTIME, &req, sizeof(req), NULL, 0,
                                &resp_len, &resp_result);
  if (rc != 0)
  {
    hotplug_print_exchange_error("set-runtime", mnt, rc);
    return 1;
  }
  if (resp_result != 0)
  {
    fprintf(stderr, "hotplug set-runtime failed: %s\n", strerror(-resp_result));
    return 1;
  }
  return 0;
}

static int cmd_hotplug_status(const char *mnt, int json);
static int cmd_hotplug_restart(const char *mnt);
static int cmd_hotplug_compat(const char *mnt, int json);
static int cmd_hotplug_set_timeout(const char *mnt, const char *timeout_str);
static int cmd_hotplug_set_dedup_priority(const char *mnt, const char *mode_str,
                                          const char *nice_str);
static int cmd_hotplug_env_list(const char *mnt);
static int cmd_hotplug_env_set(const char *mnt, const char *kv);
static int cmd_hotplug_env_unset(const char *mnt, const char *key);
static int cmd_stats(const char *mnt, int json, int verbose, kafs_unit_t unit);
static int cmd_chmod(const char *mnt, const char *mode_str, const char *path);

typedef int (*kafsctl_path_cmd_fn)(const char *mountpoint, const char *path);

static int kafsctl_dispatch_path_cmd(int argc, char **argv, kafsctl_path_cmd_fn fn)
{
  if (argc == 3)
    return fn(NULL, argv[2]);
  if (argc != 4)
  {
    usage(argv[0]);
    return 2;
  }
  return fn(argv[2], argv[3]);
}

static int kafsctl_dispatch_chmod_cmd(int argc, char **argv)
{
  if (argc == 4)
    return cmd_chmod(NULL, argv[2], argv[3]);
  if (argc != 5)
  {
    usage(argv[0]);
    return 2;
  }
  return cmd_chmod(argv[2], argv[3], argv[4]);
}

static int kafsctl_handle_migrate_command(int argc, char **argv)
{
  int assume_yes = 0;
  for (int i = 3; i < argc; ++i)
  {
    if (strcmp(argv[i], "--yes") == 0)
      assume_yes = 1;
    else
    {
      usage(argv[0]);
      return 2;
    }
  }
  return cmd_migrate(argv[2], assume_yes);
}

static int kafsctl_handle_fsstat_command(int argc, char **argv)
{
  int json = 0;
  int verbose = 0;
  kafs_unit_t unit = KAFS_UNIT_KIB;

  for (int i = 3; i < argc; ++i)
  {
    if (strcmp(argv[i], "--json") == 0)
      json = 1;
    else if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0)
      verbose = 1;
    else if (strcmp(argv[i], "--bytes") == 0)
      unit = KAFS_UNIT_BYTES;
    else if (strcmp(argv[i], "--mib") == 0)
      unit = KAFS_UNIT_MIB;
    else if (strcmp(argv[i], "--gib") == 0)
      unit = KAFS_UNIT_GIB;
    else
    {
      usage(argv[0]);
      return 2;
    }
  }

  return cmd_stats(argv[2], json, verbose, unit);
}

static int kafsctl_handle_hotplug_env_command(int argc, char **argv)
{
  if (argc < 5)
  {
    usage(argv[0]);
    return 2;
  }
  if (strcmp(argv[3], "list") == 0)
  {
    if (argc != 5)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_hotplug_env_list(argv[4]);
  }
  if (strcmp(argv[3], "set") == 0)
  {
    if (argc != 6)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_hotplug_env_set(argv[4], argv[5]);
  }
  if (strcmp(argv[3], "unset") == 0)
  {
    if (argc != 6)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_hotplug_env_unset(argv[4], argv[5]);
  }
  usage(argv[0]);
  return 2;
}

static int kafsctl_parse_json_flag_args(int argc, char **argv, int start_index, int *json_out)
{
  for (int i = start_index; i < argc; ++i)
  {
    if (strcmp(argv[i], "--json") == 0)
      *json_out = 1;
    else
    {
      usage(argv[0]);
      return 2;
    }
  }
  return 0;
}

static int kafsctl_handle_hotplug_command(int argc, char **argv)
{
  if (argc < 4)
  {
    usage(argv[0]);
    return 2;
  }
  if (strcmp(argv[2], "status") == 0)
  {
    int json = 0;
    if (kafsctl_parse_json_flag_args(argc, argv, 4, &json) != 0)
      return 2;
    return cmd_hotplug_status(argv[3], json);
  }
  if (strcmp(argv[2], "restart-back") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_hotplug_restart(argv[3]);
  }
  if (strcmp(argv[2], "compat") == 0)
  {
    int json = 0;
    if (kafsctl_parse_json_flag_args(argc, argv, 4, &json) != 0)
      return 2;
    return cmd_hotplug_compat(argv[3], json);
  }
  if (strcmp(argv[2], "set-timeout") == 0)
  {
    if (argc != 5)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_hotplug_set_timeout(argv[3], argv[4]);
  }
  if (strcmp(argv[2], "set-dedup-priority") == 0)
  {
    if (argc != 5 && argc != 6)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_hotplug_set_dedup_priority(argv[3], argv[4], argc == 6 ? argv[5] : NULL);
  }
  if (strcmp(argv[2], "set-runtime") == 0)
  {
    if (argc < 5)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_hotplug_set_runtime(argv[3], argc - 4, &argv[4]);
  }
  if (strcmp(argv[2], "env") == 0)
    return kafsctl_handle_hotplug_env_command(argc, argv);

  usage(argv[0]);
  return 2;
}

#define KAFS_CTL_REL ".kafs.sock"

static int write_full(int fd, const void *buf, size_t len)
{
  const unsigned char *p = (const unsigned char *)buf;
  size_t done = 0;
  while (done < len)
  {
    ssize_t w = write(fd, p + done, len - done);
    if (w < 0)
      return -errno;
    if (w == 0)
      return -EIO;
    done += (size_t)w;
  }
  return 0;
}

static int read_full(int fd, unsigned char *buf, size_t len)
{
  size_t done = 0;
  while (done < len)
  {
    ssize_t r = read(fd, buf + done, len - done);
    if (r < 0)
      return -errno;
    if (r == 0)
      return -EIO;
    done += (size_t)r;
  }
  return 0;
}

static uint64_t next_req_id(void)
{
  static uint64_t rid = 0;
  return __atomic_add_fetch(&rid, 1u, __ATOMIC_RELAXED);
}

static int hotplug_ctl_exchange(const char *mnt, uint16_t op, const void *req, uint32_t req_len,
                                void *resp, uint32_t resp_cap, uint32_t *resp_len,
                                int32_t *resp_result)
{
  if (req_len > KAFS_RPC_MAX_PAYLOAD)
    return -EMSGSIZE;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
    return -errno;

  int fd = openat(dfd, KAFS_CTL_REL, O_RDWR);
  if (fd < 0)
  {
    int err = -errno;
    close(dfd);
    return err;
  }

  unsigned char sbuf[sizeof(kafs_rpc_hdr_t) + KAFS_RPC_MAX_PAYLOAD];
  kafs_rpc_hdr_t hdr;
  hdr.magic = KAFS_RPC_MAGIC;
  hdr.version = KAFS_RPC_VERSION;
  hdr.op = op;
  hdr.flags = KAFS_RPC_FLAG_ENDIAN_HOST;
  hdr.req_id = next_req_id();
  hdr.session_id = 0;
  hdr.epoch = 0;
  hdr.payload_len = req_len;
  memcpy(sbuf, &hdr, sizeof(hdr));
  if (req_len && req)
    memcpy(sbuf + sizeof(hdr), req, req_len);

  int rc = write_full(fd, sbuf, sizeof(hdr) + req_len);
  if (rc != 0)
  {
    close(fd);
    close(dfd);
    return rc;
  }

  (void)lseek(fd, 0, SEEK_SET);

  unsigned char rbuf[sizeof(kafs_rpc_resp_hdr_t) + KAFS_RPC_MAX_PAYLOAD];
  kafs_rpc_resp_hdr_t rhdr;
  rc = read_full(fd, (unsigned char *)&rhdr, sizeof(rhdr));
  if (rc != 0)
  {
    close(fd);
    close(dfd);
    return rc;
  }

  if (rhdr.payload_len > KAFS_RPC_MAX_PAYLOAD)
  {
    close(fd);
    close(dfd);
    return -EMSGSIZE;
  }

  if (rhdr.payload_len)
  {
    rc = read_full(fd, rbuf, rhdr.payload_len);
    if (rc != 0)
    {
      close(fd);
      close(dfd);
      return rc;
    }
  }

  close(fd);
  close(dfd);

  if (resp_len)
    *resp_len = rhdr.payload_len;
  if (resp_result)
    *resp_result = rhdr.result;

  if (rhdr.payload_len && resp && resp_cap >= rhdr.payload_len)
    memcpy(resp, rbuf, rhdr.payload_len);
  else if (rhdr.payload_len && resp_cap < rhdr.payload_len)
    return -EMSGSIZE;

  if (rhdr.req_id != hdr.req_id)
    return -EBADMSG;
  return 0;
}

static void hotplug_status_from_rpc(kafs_hotplug_status_t *out, const kafs_rpc_hotplug_status_t *in)
{
  memset(out, 0, sizeof(*out));
  out->struct_size = (uint32_t)sizeof(*out);
  out->version = in->version;
  out->state = in->state;
  out->data_mode = in->data_mode;
  out->session_id = in->session_id;
  out->epoch = in->epoch;
  out->last_error = in->last_error;
  out->wait_queue_len = in->wait_queue_len;
  out->wait_timeout_ms = in->wait_timeout_ms;
  out->wait_queue_limit = in->wait_queue_limit;
  out->front_major = in->front_major;
  out->front_minor = in->front_minor;
  out->front_features = in->front_features;
  out->back_major = in->back_major;
  out->back_minor = in->back_minor;
  out->back_features = in->back_features;
  out->compat_result = in->compat_result;
  out->compat_reason = in->compat_reason;
  out->pending_worker_prio_mode = in->pending_worker_prio_mode;
  out->pending_worker_nice = in->pending_worker_nice;
  out->pending_worker_prio_apply_error = in->pending_worker_prio_apply_error;
  out->fsync_policy = in->fsync_policy;
  out->pending_ttl_soft_ms = in->pending_ttl_soft_ms;
  out->pending_ttl_hard_ms = in->pending_ttl_hard_ms;
  out->pending_oldest_age_ms = in->pending_oldest_age_ms;
  out->pending_ttl_over_soft = in->pending_ttl_over_soft;
  out->pending_ttl_over_hard = in->pending_ttl_over_hard;
}

static int get_hotplug_status(const char *mnt, kafs_hotplug_status_t *out)
{
  if (!out)
    return -EINVAL;
  kafs_rpc_hotplug_status_t st;
  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc = hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_STATUS, NULL, 0, &st, sizeof(st), &resp_len,
                                &resp_result);
  if (rc != 0)
    return rc;
  if (resp_result != 0)
    return resp_result;
  if (resp_len != sizeof(st))
    return -EBADMSG;
  hotplug_status_from_rpc(out, &st);
  return 0;
}

static void hotplug_print_exchange_error(const char *op, const char *mnt, int rc)
{
  if (rc == -ENOENT)
  {
    fprintf(stderr,
            "hotplug %s failed: %s (control endpoint '/.kafs.sock' not found under mountpoint "
            "'%s'; ensure it is an active KAFS mount)\n",
            op, strerror(ENOENT), mnt ? mnt : "(null)");
    return;
  }
  if (rc == -ENOSYS)
  {
    fprintf(stderr,
            "hotplug %s failed: %s (hotplug control is disabled on this mount; remount KAFS "
            "with --hotplug/--hotplug-uds, -o hotplug/hotplug_uds=..., or KAFS_HOTPLUG_UDS, "
            "then use restart-back for supervised back restarts)\n",
            op, strerror(ENOSYS));
    return;
  }
  fprintf(stderr, "hotplug %s failed: %s\n", op, strerror(-rc));
}

static int cmd_hotplug_status(const char *mnt, int json)
{
  kafs_hotplug_status_t st;
  int rc = get_hotplug_status(mnt, &st);
  if (rc != 0)
  {
    hotplug_print_exchange_error("status", mnt, rc);
    return 1;
  }

  if (json)
  {
    printf("{\n");
    printf("  \"version\": %u,\n", st.version);
    printf("  \"state\": %u,\n", st.state);
    printf("  \"state_str\": \"%s\",\n", hotplug_state_str(st.state));
    printf("  \"data_mode\": %u,\n", st.data_mode);
    printf("  \"data_mode_str\": \"%s\",\n", hotplug_data_mode_str(st.data_mode));
    printf("  \"session_id\": %" PRIu64 ",\n", st.session_id);
    printf("  \"epoch\": %u,\n", st.epoch);
    printf("  \"last_error\": %d,\n", st.last_error);
    printf("  \"wait_queue_len\": %u,\n", st.wait_queue_len);
    printf("  \"wait_timeout_ms\": %u,\n", st.wait_timeout_ms);
    printf("  \"wait_queue_limit\": %u,\n", st.wait_queue_limit);
    printf("  \"front_major\": %u,\n", st.front_major);
    printf("  \"front_minor\": %u,\n", st.front_minor);
    printf("  \"front_features\": %u,\n", st.front_features);
    printf("  \"back_major\": %u,\n", st.back_major);
    printf("  \"back_minor\": %u,\n", st.back_minor);
    printf("  \"back_features\": %u,\n", st.back_features);
    printf("  \"compat_result\": %u,\n", st.compat_result);
    printf("  \"compat_result_str\": \"%s\",\n", hotplug_compat_str(st.compat_result));
    printf("  \"compat_reason\": %d,\n", st.compat_reason);
    printf("  \"compat_reason_str\": \"%s\",\n", hotplug_compat_reason_str(st.compat_reason));
    printf("  \"pending_worker_prio_mode\": %u,\n", st.pending_worker_prio_mode);
    printf("  \"pending_worker_prio_mode_str\": \"%s\",\n",
           pending_worker_prio_mode_str(st.pending_worker_prio_mode));
    printf("  \"pending_worker_nice\": %d,\n", st.pending_worker_nice);
    printf("  \"pending_worker_prio_apply_error\": %d,\n", st.pending_worker_prio_apply_error);
    printf("  \"fsync_policy\": %u,\n", st.fsync_policy);
    printf("  \"fsync_policy_str\": \"%s\",\n", fsync_policy_str(st.fsync_policy));
    printf("  \"pending_ttl_soft_ms\": %u,\n", st.pending_ttl_soft_ms);
    printf("  \"pending_ttl_hard_ms\": %u,\n", st.pending_ttl_hard_ms);
    printf("  \"pending_oldest_age_ms\": %" PRIu64 ",\n", st.pending_oldest_age_ms);
    printf("  \"pending_ttl_over_soft\": %u,\n", st.pending_ttl_over_soft);
    printf("  \"pending_ttl_over_hard\": %u\n", st.pending_ttl_over_hard);
    printf("}\n");
    return 0;
  }

  printf("kafs hotplug status v%u\n", st.version);
  printf("  state: %u (%s)\n", st.state, hotplug_state_str(st.state));
  printf("  data_mode: %u (%s)\n", st.data_mode, hotplug_data_mode_str(st.data_mode));
  printf("  session_id: %" PRIu64 "\n", st.session_id);
  printf("  epoch: %u\n", st.epoch);
  printf("  last_error: %d\n", st.last_error);
  printf("  wait_queue_len: %u\n", st.wait_queue_len);
  printf("  wait_timeout_ms: %u\n", st.wait_timeout_ms);
  printf("  wait_queue_limit: %u\n", st.wait_queue_limit);
  printf("  front_version: %u.%u\n", st.front_major, st.front_minor);
  printf("  front_features: %u\n", st.front_features);
  printf("  back_version: %u.%u\n", st.back_major, st.back_minor);
  printf("  back_features: %u\n", st.back_features);
  printf("  compat_result: %u (%s)\n", st.compat_result, hotplug_compat_str(st.compat_result));
  printf("  compat_reason: %d (%s)\n", st.compat_reason,
         hotplug_compat_reason_str(st.compat_reason));
  printf("  pending_worker_prio_mode: %u (%s)\n", st.pending_worker_prio_mode,
         pending_worker_prio_mode_str(st.pending_worker_prio_mode));
  printf("  pending_worker_nice: %d\n", st.pending_worker_nice);
  printf("  pending_worker_prio_apply_error: %d\n", st.pending_worker_prio_apply_error);
  printf("  fsync_policy: %u (%s)\n", st.fsync_policy, fsync_policy_str(st.fsync_policy));
  printf("  pending_ttl_soft_ms: %u\n", st.pending_ttl_soft_ms);
  printf("  pending_ttl_hard_ms: %u\n", st.pending_ttl_hard_ms);
  printf("  pending_oldest_age_ms: %" PRIu64 "\n", st.pending_oldest_age_ms);
  printf("  pending_ttl_over_soft: %u\n", st.pending_ttl_over_soft);
  printf("  pending_ttl_over_hard: %u\n", st.pending_ttl_over_hard);
  return 0;
}

static int cmd_hotplug_restart(const char *mnt)
{
  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc =
      hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_RESTART, NULL, 0, NULL, 0, &resp_len, &resp_result);
  if (rc != 0)
  {
    hotplug_print_exchange_error("restart", mnt, rc);
    return 1;
  }
  if (resp_result != 0)
  {
    fprintf(stderr, "hotplug restart failed: %s\n", strerror(-resp_result));
    return 1;
  }
  return 0;
}

static int cmd_hotplug_compat(const char *mnt, int json)
{
  kafs_hotplug_status_t st;
  int rc = get_hotplug_status(mnt, &st);
  if (rc != 0)
  {
    hotplug_print_exchange_error("compat", mnt, rc);
    return 1;
  }

  if (json)
  {
    printf("{\n");
    printf("  \"front_major\": %u,\n", st.front_major);
    printf("  \"front_minor\": %u,\n", st.front_minor);
    printf("  \"front_features\": %u,\n", st.front_features);
    printf("  \"back_major\": %u,\n", st.back_major);
    printf("  \"back_minor\": %u,\n", st.back_minor);
    printf("  \"back_features\": %u,\n", st.back_features);
    printf("  \"compat_result\": %u,\n", st.compat_result);
    printf("  \"compat_result_str\": \"%s\",\n", hotplug_compat_str(st.compat_result));
    printf("  \"compat_reason\": %d,\n", st.compat_reason);
    printf("  \"compat_reason_str\": \"%s\"\n", hotplug_compat_reason_str(st.compat_reason));
    printf("}\n");
    return 0;
  }

  printf("kafs hotplug compat\n");
  printf("  front_version: %u.%u\n", st.front_major, st.front_minor);
  printf("  front_features: %u\n", st.front_features);
  printf("  back_version: %u.%u\n", st.back_major, st.back_minor);
  printf("  back_features: %u\n", st.back_features);
  printf("  compat_result: %u (%s)\n", st.compat_result, hotplug_compat_str(st.compat_result));
  printf("  compat_reason: %d (%s)\n", st.compat_reason,
         hotplug_compat_reason_str(st.compat_reason));
  return 0;
}

static int cmd_hotplug_set_timeout(const char *mnt, const char *timeout_str)
{
  if (!timeout_str || *timeout_str == '\0')
  {
    fprintf(stderr, "invalid timeout\n");
    return 2;
  }
  char *endp = NULL;
  unsigned long v = strtoul(timeout_str, &endp, 10);
  if (!endp || *endp != '\0' || v == 0 || v > UINT32_MAX)
  {
    fprintf(stderr, "invalid timeout\n");
    return 2;
  }

  kafs_rpc_set_timeout_t req;
  req.timeout_ms = (uint32_t)v;
  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc = hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_SET_TIMEOUT, &req, sizeof(req), NULL, 0,
                                &resp_len, &resp_result);
  if (rc != 0)
  {
    hotplug_print_exchange_error("set-timeout", mnt, rc);
    return 1;
  }
  if (resp_result != 0)
  {
    fprintf(stderr, "hotplug set-timeout failed: %s\n", strerror(-resp_result));
    return 1;
  }
  return 0;
}

static int cmd_hotplug_set_dedup_priority(const char *mnt, const char *mode_str,
                                          const char *nice_str)
{
  if (!mode_str)
  {
    fprintf(stderr, "invalid mode\n");
    return 2;
  }

  kafs_rpc_set_dedup_prio_t req;
  memset(&req, 0, sizeof(req));
  if (strcmp(mode_str, "normal") == 0)
    req.mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
  else if (strcmp(mode_str, "idle") == 0)
    req.mode = KAFS_PENDING_WORKER_PRIO_IDLE;
  else
  {
    fprintf(stderr, "invalid mode: %s\n", mode_str);
    return 2;
  }

  if (nice_str)
  {
    char *endp = NULL;
    long v = strtol(nice_str, &endp, 10);
    if (!endp || *endp != '\0' || v < 0 || v > 19)
    {
      fprintf(stderr, "invalid nice value: %s\n", nice_str);
      return 2;
    }
    req.flags |= KAFS_RPC_SET_DEDUP_PRIO_F_HAS_NICE;
    req.nice_value = (int32_t)v;
  }

  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc = hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_SET_DEDUP_PRIO, &req, sizeof(req), NULL, 0,
                                &resp_len, &resp_result);
  if (rc != 0)
  {
    hotplug_print_exchange_error("set-dedup-priority", mnt, rc);
    return 1;
  }
  if (resp_result != 0)
  {
    fprintf(stderr, "hotplug set-dedup-priority failed: %s\n", strerror(-resp_result));
    return 1;
  }
  return 0;
}

static int cmd_hotplug_env_list(const char *mnt)
{
  kafs_rpc_env_list_t env;
  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc = hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_ENV_LIST, NULL, 0, &env, sizeof(env),
                                &resp_len, &resp_result);
  if (rc != 0)
  {
    hotplug_print_exchange_error("env list", mnt, rc);
    return 1;
  }
  if (resp_result != 0)
  {
    fprintf(stderr, "hotplug env list failed: %s\n", strerror(-resp_result));
    return 1;
  }
  if (resp_len != sizeof(env))
  {
    fprintf(stderr, "hotplug env list failed: %s\n", strerror(EBADMSG));
    return 1;
  }

  printf("kafs hotplug env list\n");
  for (uint32_t i = 0; i < env.count && i < KAFS_HOTPLUG_ENV_MAX; ++i)
    printf("  %s=%s\n", env.entries[i].key, env.entries[i].value);
  return 0;
}

static int cmd_hotplug_env_set(const char *mnt, const char *kv)
{
  if (!kv)
  {
    fprintf(stderr, "invalid key=value\n");
    return 2;
  }
  const char *eq = strchr(kv, '=');
  if (!eq || eq == kv)
  {
    fprintf(stderr, "invalid key=value\n");
    return 2;
  }

  kafs_rpc_env_update_t req;
  memset(&req, 0, sizeof(req));
  snprintf(req.key, sizeof(req.key), "%.*s", (int)(eq - kv), kv);
  snprintf(req.value, sizeof(req.value), "%s", eq + 1);
  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc = hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_ENV_SET, &req, sizeof(req), NULL, 0, &resp_len,
                                &resp_result);
  if (rc != 0)
  {
    hotplug_print_exchange_error("env set", mnt, rc);
    return 1;
  }
  if (resp_result != 0)
  {
    fprintf(stderr, "hotplug env set failed: %s\n", strerror(-resp_result));
    return 1;
  }
  return 0;
}

static int cmd_hotplug_env_unset(const char *mnt, const char *key)
{
  if (!key || *key == '\0')
  {
    fprintf(stderr, "invalid key\n");
    return 2;
  }
  kafs_rpc_env_update_t req;
  memset(&req, 0, sizeof(req));
  snprintf(req.key, sizeof(req.key), "%s", key);
  uint32_t resp_len = 0;
  int32_t resp_result = 0;
  int rc = hotplug_ctl_exchange(mnt, KAFS_RPC_OP_CTL_ENV_UNSET, &req, sizeof(req), NULL, 0,
                                &resp_len, &resp_result);
  if (rc != 0)
  {
    hotplug_print_exchange_error("env unset", mnt, rc);
    return 1;
  }
  if (resp_result != 0)
  {
    fprintf(stderr, "hotplug env unset failed: %s\n", strerror(-resp_result));
    return 1;
  }
  return 0;
}

static int path_has_dotdot_component(const char *p)
{
  if (!p)
    return 0;
  const char *s = p;
  while (*s)
  {
    while (*s == '/')
      ++s;
    const char *c = s;
    while (*s && *s != '/')
      ++s;
    size_t len = (size_t)(s - c);
    if (len == 2 && c[0] == '.' && c[1] == '.')
      return 1;
  }
  return 0;
}

typedef struct kafs_path_ref
{
  int dfd;
  char mount[KAFS_IOCTL_PATH_MAX];
  char abs_path[KAFS_IOCTL_PATH_MAX];
  char rel[KAFS_IOCTL_PATH_MAX];
} kafs_path_ref_t;

static void close_path_ref(kafs_path_ref_t *ref)
{
  if (!ref)
    return;
  if (ref->dfd >= 0)
  {
    close(ref->dfd);
    ref->dfd = -1;
  }
}

static void init_path_ref(kafs_path_ref_t *ref)
{
  if (!ref)
    return;
  memset(ref, 0, sizeof(*ref));
  ref->dfd = -1;
}

static int path_join_components(char out[KAFS_IOCTL_PATH_MAX], const char *base, const char *suffix)
{
  if (!out || !base || !suffix)
    return -EINVAL;
  if (snprintf(out, KAFS_IOCTL_PATH_MAX, "%s%s%s", base,
               (suffix[0] != '\0' && strcmp(base, "/") != 0) ? "/" : "",
               suffix) >= KAFS_IOCTL_PATH_MAX)
    return -ENAMETOOLONG;
  return 0;
}

static int normalize_path_raw_input(const char *input, char raw[KAFS_IOCTL_PATH_MAX])
{
  if (input[0] == '/')
  {
    if (snprintf(raw, KAFS_IOCTL_PATH_MAX, "%s", input) >= KAFS_IOCTL_PATH_MAX)
      return -ENAMETOOLONG;
    return 0;
  }

  char cwd[KAFS_IOCTL_PATH_MAX];
  if (!getcwd(cwd, sizeof(cwd)))
    return -errno;
  if (snprintf(raw, KAFS_IOCTL_PATH_MAX, "%s/%s", cwd, input) >= KAFS_IOCTL_PATH_MAX)
    return -ENAMETOOLONG;
  return 0;
}

static void normalize_path_pop_component(char normalized[KAFS_IOCTL_PATH_MAX], size_t *norm_len)
{
  if (*norm_len <= 1)
    return;

  if (normalized[*norm_len - 1] == '/')
    --(*norm_len);
  while (*norm_len > 1 && normalized[*norm_len - 1] != '/')
    --(*norm_len);
  normalized[*norm_len] = '\0';
}

static int normalize_path_append_component(char normalized[KAFS_IOCTL_PATH_MAX], size_t *norm_len,
                                           const char *start, size_t len)
{
  if (*norm_len > 1)
  {
    if (*norm_len + 1 >= KAFS_IOCTL_PATH_MAX)
      return -ENAMETOOLONG;
    normalized[(*norm_len)++] = '/';
  }
  if (*norm_len + len >= KAFS_IOCTL_PATH_MAX)
    return -ENAMETOOLONG;

  memcpy(normalized + *norm_len, start, len);
  *norm_len += len;
  normalized[*norm_len] = '\0';
  return 0;
}

static int normalize_path_handle_segment(char normalized[KAFS_IOCTL_PATH_MAX], size_t *norm_len,
                                         const char *start, size_t len)
{
  if (len == 1 && start[0] == '.')
    return 0;
  if (len == 2 && start[0] == '.' && start[1] == '.')
  {
    normalize_path_pop_component(normalized, norm_len);
    return 0;
  }
  return normalize_path_append_component(normalized, norm_len, start, len);
}

static int normalize_path_input(const char *input, char out[KAFS_IOCTL_PATH_MAX])
{
  if (!input || !*input)
    return -EINVAL;

  char raw[KAFS_IOCTL_PATH_MAX];
  int rc = normalize_path_raw_input(input, raw);
  if (rc != 0)
    return rc;

  char normalized[KAFS_IOCTL_PATH_MAX];
  size_t norm_len = 1;
  normalized[0] = '/';
  normalized[1] = '\0';

  const char *cursor = raw;
  while (*cursor)
  {
    while (*cursor == '/')
      ++cursor;
    if (*cursor == '\0')
      break;

    const char *start = cursor;
    while (*cursor && *cursor != '/')
      ++cursor;
    size_t len = (size_t)(cursor - start);
    rc = normalize_path_handle_segment(normalized, &norm_len, start, len);
    if (rc != 0)
      return rc;
  }

  if (snprintf(out, KAFS_IOCTL_PATH_MAX, "%s", normalized) >= KAFS_IOCTL_PATH_MAX)
    return -ENAMETOOLONG;
  return 0;
}

static int find_existing_ancestor(const char *abs_path, char existing[KAFS_IOCTL_PATH_MAX],
                                  int *is_dir, const char **suffix_out)
{
  if (!abs_path || abs_path[0] != '/' || !existing || !is_dir || !suffix_out)
    return -EINVAL;

  char probe[KAFS_IOCTL_PATH_MAX];
  if (snprintf(probe, sizeof(probe), "%s", abs_path) >= (int)sizeof(probe))
    return -ENAMETOOLONG;

  while (1)
  {
    struct stat st;
    if (lstat(probe, &st) == 0)
    {
      char realbuf[KAFS_IOCTL_PATH_MAX];
      if (!realpath(probe, realbuf))
        return -errno;
      if (snprintf(existing, KAFS_IOCTL_PATH_MAX, "%s", realbuf) >= KAFS_IOCTL_PATH_MAX)
        return -ENAMETOOLONG;
      *is_dir = S_ISDIR(st.st_mode) ? 1 : 0;
      *suffix_out = abs_path + strlen(probe);
      return 0;
    }
    if (errno != ENOENT && errno != ENOTDIR)
      return -errno;
    if (strcmp(probe, "/") == 0)
      return -ENOENT;

    char *slash = strrchr(probe, '/');
    if (!slash)
      return -ENOENT;
    if (slash == probe)
      slash[1] = '\0';
    else
      *slash = '\0';
  }
}

static int is_kafs_mount_dir(const char *path)
{
  int fd = open(path, O_RDONLY | O_DIRECTORY);
  if (fd < 0)
    return 0;

  kafs_stats_t st;
  memset(&st, 0, sizeof(st));
  st.struct_size = (uint32_t)sizeof(st);
  int rc = (ioctl(fd, KAFS_IOCTL_GET_STATS, &st) == 0) ? 1 : 0;
  close(fd);
  return rc;
}

static int find_kafs_mount_root(const char *dir_path, char mount[KAFS_IOCTL_PATH_MAX])
{
  if (!dir_path || dir_path[0] != '/' || !mount)
    return -EINVAL;

  char current[KAFS_IOCTL_PATH_MAX];
  if (snprintf(current, sizeof(current), "%s", dir_path) >= (int)sizeof(current))
    return -ENAMETOOLONG;

  char last_success[KAFS_IOCTL_PATH_MAX];
  last_success[0] = '\0';
  while (1)
  {
    if (is_kafs_mount_dir(current))
    {
      if (snprintf(last_success, sizeof(last_success), "%s", current) >= (int)sizeof(last_success))
        return -ENAMETOOLONG;
    }
    else if (last_success[0] != '\0')
    {
      break;
    }
    else
    {
      return -ENOTTY;
    }

    if (strcmp(current, "/") == 0)
      break;

    char parent[KAFS_IOCTL_PATH_MAX];
    if (snprintf(parent, sizeof(parent), "%s", current) >= (int)sizeof(parent))
      return -ENAMETOOLONG;
    char *slash = strrchr(parent, '/');
    if (!slash)
      break;
    if (slash == parent)
      slash[1] = '\0';
    else
      *slash = '\0';
    if (strcmp(parent, current) == 0)
      break;
    if (snprintf(current, sizeof(current), "%s", parent) >= (int)sizeof(current))
      return -ENAMETOOLONG;
  }

  if (last_success[0] == '\0')
    return -ENOTTY;
  if (snprintf(mount, KAFS_IOCTL_PATH_MAX, "%s", last_success) >= KAFS_IOCTL_PATH_MAX)
    return -ENAMETOOLONG;
  return 0;
}

static const char *skip_leading_slashes(const char *path)
{
  while (*path == '/')
    ++path;
  return path;
}

static int resolve_auto_path_build_abs(const char *existing, const char *suffix,
                                       char resolved_abs[KAFS_IOCTL_PATH_MAX])
{
  const char *rel_suffix = suffix ? skip_leading_slashes(suffix) : "";
  if (*rel_suffix == '\0')
  {
    if (snprintf(resolved_abs, KAFS_IOCTL_PATH_MAX, "%s", existing) >= KAFS_IOCTL_PATH_MAX)
      return -ENAMETOOLONG;
    return 0;
  }
  return path_join_components(resolved_abs, existing, rel_suffix);
}

static int resolve_auto_path_existing_dir(const char *existing, int existing_is_dir,
                                          char existing_dir[KAFS_IOCTL_PATH_MAX])
{
  if (snprintf(existing_dir, KAFS_IOCTL_PATH_MAX, "%s", existing) >= KAFS_IOCTL_PATH_MAX)
    return -ENAMETOOLONG;
  if (existing_is_dir)
    return 0;

  char *slash = strrchr(existing_dir, '/');
  if (!slash)
    return -EINVAL;
  if (slash == existing_dir)
    slash[1] = '\0';
  else
    *slash = '\0';
  return 0;
}

static int resolve_auto_path_rel_to_mount(const char *mount, const char *resolved_abs,
                                          const char **rel_out)
{
  size_t mount_len = strlen(mount);
  if (strncmp(resolved_abs, mount, mount_len) != 0 ||
      (resolved_abs[mount_len] != '\0' && resolved_abs[mount_len] != '/'))
    return -EINVAL;

  *rel_out = skip_leading_slashes(resolved_abs + mount_len);
  return 0;
}

static int resolve_auto_path_fill_ref(kafs_path_ref_t *out, const char *mount,
                                      const char *resolved_abs, const char *rel)
{
  out->dfd = open(mount, O_RDONLY | O_DIRECTORY);
  if (out->dfd < 0)
    return -errno;
  if (snprintf(out->mount, sizeof(out->mount), "%s", mount) >= (int)sizeof(out->mount) ||
      snprintf(out->abs_path, sizeof(out->abs_path), "%s", resolved_abs) >=
          (int)sizeof(out->abs_path) ||
      snprintf(out->rel, sizeof(out->rel), "%s", (*rel != '\0') ? rel : ".") >=
          (int)sizeof(out->rel))
  {
    close_path_ref(out);
    return -ENAMETOOLONG;
  }
  return 0;
}

static int resolve_auto_path(const char *path, kafs_path_ref_t *out)
{
  char normalized[KAFS_IOCTL_PATH_MAX];
  int rc = normalize_path_input(path, normalized);
  if (rc != 0)
    return rc;

  char existing[KAFS_IOCTL_PATH_MAX];
  const char *suffix = NULL;
  int existing_is_dir = 0;
  rc = find_existing_ancestor(normalized, existing, &existing_is_dir, &suffix);
  if (rc != 0)
    return rc;

  char resolved_abs[KAFS_IOCTL_PATH_MAX];
  rc = resolve_auto_path_build_abs(existing, suffix, resolved_abs);
  if (rc != 0)
    return rc;

  char existing_dir[KAFS_IOCTL_PATH_MAX];
  rc = resolve_auto_path_existing_dir(existing, existing_is_dir, existing_dir);
  if (rc != 0)
    return rc;

  char mount[KAFS_IOCTL_PATH_MAX];
  rc = find_kafs_mount_root(existing_dir, mount);
  if (rc != 0)
    return rc;

  const char *rel = NULL;
  rc = resolve_auto_path_rel_to_mount(mount, resolved_abs, &rel);
  if (rc != 0)
    return rc;

  return resolve_auto_path_fill_ref(out, mount, resolved_abs, rel);
}

static int resolve_mount_path(const char *mnt, const char *path, kafs_path_ref_t *out)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  out->dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (out->dfd < 0)
    return -errno;
  if (snprintf(out->mount, sizeof(out->mount), "%s", mnt_abs) >= (int)sizeof(out->mount))
  {
    close_path_ref(out);
    return -ENAMETOOLONG;
  }

  int rc = normalize_path_input(path, out->abs_path);
  if (rc != 0)
  {
    close_path_ref(out);
    return rc;
  }

  const char *rel = to_mount_rel_path(mnt_abs, path, out->rel);
  if (!rel)
  {
    if (strcmp(out->abs_path, mnt_abs) == 0)
    {
      if (snprintf(out->rel, sizeof(out->rel), ".") >= (int)sizeof(out->rel))
      {
        close_path_ref(out);
        return -ENAMETOOLONG;
      }
    }
    else
    {
      close_path_ref(out);
      return -EINVAL;
    }
  }
  return 0;
}

static int resolve_path_ref(const char *mnt, const char *path, kafs_path_ref_t *out)
{
  if (!out)
    return 2;
  init_path_ref(out);

  int rc = (mnt && *mnt) ? resolve_mount_path(mnt, path, out) : resolve_auto_path(path, out);
  if (rc == -ENOTTY)
  {
    fprintf(stderr, "path is not under a KAFS mount: %s\n", path ? path : "(null)");
    return 2;
  }
  if (rc == -ENOENT)
  {
    fprintf(stderr, "path does not exist and no existing parent could be resolved: %s\n",
            path ? path : "(null)");
    return 2;
  }
  if (rc != 0)
  {
    fprintf(stderr, "invalid path: %s\n", path ? path : "(null)");
    return 2;
  }
  return 0;
}

static int try_resolve_auto_path_ref(const char *path, kafs_path_ref_t *out)
{
  if (!out)
    return -EINVAL;
  init_path_ref(out);
  return resolve_auto_path(path, out);
}

static int run_external_command(char *const argv[])
{
  pid_t pid = fork();
  if (pid < 0)
  {
    perror("fork");
    return 1;
  }
  if (pid == 0)
  {
    execvp(argv[0], argv);
    perror(argv[0]);
    _exit(127);
  }

  int status = 0;
  while (waitpid(pid, &status, 0) < 0)
  {
    if (errno == EINTR)
      continue;
    perror("waitpid");
    return 1;
  }

  if (WIFEXITED(status))
    return WEXITSTATUS(status);
  if (WIFSIGNALED(status))
    return 128 + WTERMSIG(status);
  return 1;
}

static int find_path_executable(const char *prog, char out[PATH_MAX])
{
  if (!prog || !*prog || !out)
    return -EINVAL;
  if (strchr(prog, '/') != NULL)
  {
    if (access(prog, X_OK) != 0)
      return -errno;
    if (snprintf(out, PATH_MAX, "%s", prog) >= PATH_MAX)
      return -ENAMETOOLONG;
    return 0;
  }

  const char *path_env = getenv("PATH");
  if (!path_env || !*path_env)
    return -ENOENT;

  const char *cursor = path_env;
  while (*cursor)
  {
    const char *sep = strchr(cursor, ':');
    size_t len = sep ? (size_t)(sep - cursor) : strlen(cursor);
    char dir[PATH_MAX];
    if (len == 0)
    {
      if (snprintf(dir, sizeof(dir), ".") >= (int)sizeof(dir))
        return -ENAMETOOLONG;
    }
    else
    {
      if (len >= sizeof(dir))
        return -ENAMETOOLONG;
      memcpy(dir, cursor, len);
      dir[len] = '\0';
    }

    if (snprintf(out, PATH_MAX, "%s/%s", dir, prog) >= PATH_MAX)
      return -ENAMETOOLONG;
    if (access(out, X_OK) == 0)
      return 0;

    if (!sep)
      break;
    cursor = sep + 1;
  }

  return -ENOENT;
}

static int path_is_kafs_candidate(const char *path)
{
  kafs_path_ref_t ref;
  int rc = try_resolve_auto_path_ref(path, &ref);
  if (rc == 0)
  {
    close_path_ref(&ref);
    return 1;
  }
  return 0;
}

static void print_rsync_operand_summary(int argc, char **argv)
{
  int first_operand = -1;
  for (int i = 2; i < argc; ++i)
  {
    if (strcmp(argv[i], "--") == 0)
    {
      first_operand = i + 1;
      break;
    }
    if (argv[i][0] != '-' || strcmp(argv[i], "-") == 0)
    {
      first_operand = i;
      break;
    }
  }
  if (first_operand < 0 || first_operand >= argc)
    return;

  int operand_count = argc - first_operand;
  int kafs_operands = 0;
  for (int i = first_operand; i < argc; ++i)
    kafs_operands += path_is_kafs_candidate(argv[i]) ? 1 : 0;

  fprintf(stderr, "rsync operand summary: operands=%d kafs_candidates=%d\n", operand_count,
          kafs_operands);
}

static int cmd_rsync(int argc, char **argv)
{
  char rsync_path[PATH_MAX];
  int frc = find_path_executable("rsync", rsync_path);
  if (frc != 0)
  {
    print_rsync_operand_summary(argc, argv);
    fprintf(stderr, "rsync binary not found in PATH\n");
    return 2;
  }

  char **child_argv = (char **)calloc((size_t)argc, sizeof(char *));
  if (!child_argv)
  {
    perror("calloc");
    return 1;
  }

  child_argv[0] = rsync_path;
  for (int i = 2; i < argc; ++i)
    child_argv[i - 1] = argv[i];
  child_argv[argc - 1] = NULL;

  int rc = run_external_command(child_argv);
  free(child_argv);
  return rc;
}

static const char *to_kafs_path(const char *mnt_abs, const char *p, char out[KAFS_IOCTL_PATH_MAX])
{
  if (!p || !*p)
    return NULL;
  if (p[0] == '/')
  {
    size_t ml = mnt_abs ? strlen(mnt_abs) : 0;
    if (ml && strncmp(p, mnt_abs, ml) == 0 && (p[ml] == '/' || p[ml] == '\0'))
    {
      const char *suf = p + ml;
      if (*suf == '\0')
        suf = "/";
      snprintf(out, KAFS_IOCTL_PATH_MAX, "%s", suf);
      return out;
    }
    snprintf(out, KAFS_IOCTL_PATH_MAX, "%s", p);
    return out;
  }
  snprintf(out, KAFS_IOCTL_PATH_MAX, "/%s", p);
  return out;
}

static const char *to_mount_rel_path(const char *mnt_abs, const char *p,
                                     char out[KAFS_IOCTL_PATH_MAX])
{
  if (!p || !*p)
    return NULL;

  const char *suf = p;
  if (p[0] == '/')
  {
    size_t ml = mnt_abs ? strlen(mnt_abs) : 0;
    if (ml && strncmp(p, mnt_abs, ml) == 0 && (p[ml] == '/' || p[ml] == '\0'))
      suf = p + ml;
    if (*suf == '/')
      ++suf;
  }

  if (*suf == '\0')
    return NULL;

  snprintf(out, KAFS_IOCTL_PATH_MAX, "%s", suf);
  if (out[0] == '/' || path_has_dotdot_component(out))
    return NULL;
  return out;
}

typedef struct kafs_stats_report
{
  kafs_stats_t st;
  int have_verbose_scan;
  uint64_t logical_bytes;
  uint64_t unique_bytes;
  uint64_t saved_bytes;
  uint64_t fs_blocks_used;
  uint64_t fs_inodes_used;
  uint64_t bg_dedup_ops;
  uint64_t hrl_or_legacy_total;
  double dedup_ratio;
  double hit_rate;
  double hrl_put_hash_ms;
  double hrl_put_find_ms;
  double hrl_put_cmp_ms;
  double hrl_put_slot_alloc_ms;
  double hrl_put_blk_alloc_ms;
  double hrl_put_blk_write_ms;
  double hrl_put_avg_chain_steps;
  double hrl_put_avg_cmp_calls;
  double lock_inode_cont_rate;
  double lock_inode_wait_ms;
  double lock_inode_alloc_cont_rate;
  double lock_inode_alloc_wait_ms;
  double lock_bitmap_cont_rate;
  double lock_bitmap_wait_ms;
  double lock_hrl_bucket_cont_rate;
  double lock_hrl_bucket_wait_ms;
  double lock_hrl_global_cont_rate;
  double lock_hrl_global_wait_ms;
  double access_fh_fastpath_rate;
  double access_avg_components;
  double dir_snapshot_avg_bytes;
  double pwrite_iblk_read_ms;
  double pwrite_iblk_write_ms;
  double pwrite_iblk_write_p50_ms;
  double pwrite_iblk_write_p95_ms;
  double pwrite_iblk_write_p99_ms;
  double iblk_write_hrl_put_ms;
  double iblk_write_legacy_blk_write_ms;
  double iblk_write_dec_ref_ms;
  double blk_alloc_scan_ms;
  double blk_alloc_claim_ms;
  double blk_alloc_set_usage_ms;
  double blk_alloc_retry_rate;
  double blk_set_usage_bit_ms;
  double blk_set_usage_freecnt_ms;
  double blk_set_usage_wtime_ms;
  double copy_share_hit_rate;
  double bg_dedup_retry_rate;
  double bg_dedup_direct_hit_rate;
  double pending_worker_start_fail_rate;
  double fs_blocks_used_pct;
  double fs_inodes_used_pct;
  double hrl_entries_used_pct;
  double hrl_path_rate;
  double legacy_path_rate;
  double direct_to_hrl_ratio;
  double hrl_hit_rate_pct;
  double hrl_miss_rate_pct;
  double hrl_rescue_hit_rate;
  char tombstone_oldest_buf[64];
} kafs_stats_report_t;

static void kafsctl_stats_report_init(kafs_stats_report_t *report, const kafs_stats_t *st)
{
  memset(report, 0, sizeof(*report));
  report->st = *st;
  report->dedup_ratio = 1.0;
  snprintf(report->tombstone_oldest_buf, sizeof(report->tombstone_oldest_buf), "%s", "none");
}

static void kafsctl_stats_compute_capacity(kafs_stats_report_t *report)
{
  report->have_verbose_scan = ((report->st.result_flags & KAFS_STATS_R_VERBOSE_SCAN) != 0);
  report->logical_bytes = report->st.hrl_refcnt_sum * (uint64_t)report->st.blksize;
  report->unique_bytes = report->st.hrl_entries_used * (uint64_t)report->st.blksize;
  report->saved_bytes =
      (report->st.hrl_refcnt_sum > report->st.hrl_entries_used)
          ? (report->st.hrl_refcnt_sum - report->st.hrl_entries_used) * (uint64_t)report->st.blksize
          : 0;
  if (report->unique_bytes > 0)
    report->dedup_ratio = (double)report->logical_bytes / (double)report->unique_bytes;

  if (report->st.hrl_put_calls > 0)
    report->hit_rate = (double)report->st.hrl_put_hits / (double)report->st.hrl_put_calls;
  report->fs_blocks_used = (report->st.fs_blocks_total >= report->st.fs_blocks_free)
                               ? (report->st.fs_blocks_total - report->st.fs_blocks_free)
                               : 0;
  report->fs_inodes_used = (report->st.fs_inodes_total >= report->st.fs_inodes_free)
                               ? (report->st.fs_inodes_total - report->st.fs_inodes_free)
                               : 0;
  report->fs_blocks_used_pct = pct_u64(report->fs_blocks_used, report->st.fs_blocks_total);
  report->fs_inodes_used_pct = pct_u64(report->fs_inodes_used, report->st.fs_inodes_total);
  report->hrl_entries_used_pct = pct_u64(report->st.hrl_entries_used, report->st.hrl_entries_total);
  report->hrl_or_legacy_total = report->st.hrl_put_calls + report->st.hrl_put_fallback_legacy;
  report->hrl_path_rate = pct_u64(report->st.hrl_put_calls, report->hrl_or_legacy_total);
  report->legacy_path_rate =
      pct_u64(report->st.hrl_put_fallback_legacy, report->hrl_or_legacy_total);
  report->direct_to_hrl_ratio =
      (report->st.hrl_put_calls > 0)
          ? (double)report->st.hrl_put_fallback_legacy / (double)report->st.hrl_put_calls
          : 0.0;
  report->hrl_hit_rate_pct = pct_u64(report->st.hrl_put_hits, report->st.hrl_put_calls);
  report->hrl_miss_rate_pct = pct_u64(report->st.hrl_put_misses, report->st.hrl_put_calls);
}

static void kafsctl_stats_compute_lock_metrics(kafs_stats_report_t *report)
{
  report->lock_inode_cont_rate =
      (report->st.lock_inode_acquire > 0)
          ? (double)report->st.lock_inode_contended / (double)report->st.lock_inode_acquire
          : 0.0;
  report->lock_inode_wait_ms = (double)report->st.lock_inode_wait_ns / 1000000.0;
  report->lock_inode_alloc_cont_rate = (report->st.lock_inode_alloc_acquire > 0)
                                           ? (double)report->st.lock_inode_alloc_contended /
                                                 (double)report->st.lock_inode_alloc_acquire
                                           : 0.0;
  report->lock_inode_alloc_wait_ms = (double)report->st.lock_inode_alloc_wait_ns / 1000000.0;
  report->lock_bitmap_cont_rate =
      (report->st.lock_bitmap_acquire > 0)
          ? (double)report->st.lock_bitmap_contended / (double)report->st.lock_bitmap_acquire
          : 0.0;
  report->lock_bitmap_wait_ms = (double)report->st.lock_bitmap_wait_ns / 1000000.0;
  report->lock_hrl_bucket_cont_rate = (report->st.lock_hrl_bucket_acquire > 0)
                                          ? (double)report->st.lock_hrl_bucket_contended /
                                                (double)report->st.lock_hrl_bucket_acquire
                                          : 0.0;
  report->lock_hrl_bucket_wait_ms = (double)report->st.lock_hrl_bucket_wait_ns / 1000000.0;
  report->lock_hrl_global_cont_rate = (report->st.lock_hrl_global_acquire > 0)
                                          ? (double)report->st.lock_hrl_global_contended /
                                                (double)report->st.lock_hrl_global_acquire
                                          : 0.0;
  report->lock_hrl_global_wait_ms = (double)report->st.lock_hrl_global_wait_ns / 1000000.0;
}

static void kafsctl_stats_compute_io_metrics(kafs_stats_report_t *report)
{
  report->hrl_put_hash_ms = (double)report->st.hrl_put_ns_hash / 1000000.0;
  report->hrl_put_find_ms = (double)report->st.hrl_put_ns_find / 1000000.0;
  report->hrl_put_cmp_ms = (double)report->st.hrl_put_ns_cmp_content / 1000000.0;
  report->hrl_put_slot_alloc_ms = (double)report->st.hrl_put_ns_slot_alloc / 1000000.0;
  report->hrl_put_blk_alloc_ms = (double)report->st.hrl_put_ns_blk_alloc / 1000000.0;
  report->hrl_put_blk_write_ms = (double)report->st.hrl_put_ns_blk_write / 1000000.0;
  report->hrl_put_avg_chain_steps =
      (report->st.hrl_put_calls > 0)
          ? (double)report->st.hrl_put_chain_steps / (double)report->st.hrl_put_calls
          : 0.0;
  report->hrl_put_avg_cmp_calls =
      (report->st.hrl_put_calls > 0)
          ? (double)report->st.hrl_put_cmp_calls / (double)report->st.hrl_put_calls
          : 0.0;
  report->access_fh_fastpath_rate =
      (report->st.access_calls > 0)
          ? (double)report->st.access_fh_fastpath_hits / (double)report->st.access_calls
          : 0.0;
  report->access_avg_components =
      (report->st.access_path_walk_calls > 0)
          ? (double)report->st.access_path_components / (double)report->st.access_path_walk_calls
          : 0.0;
  report->dir_snapshot_avg_bytes =
      (report->st.dir_snapshot_calls > 0)
          ? (double)report->st.dir_snapshot_bytes / (double)report->st.dir_snapshot_calls
          : 0.0;
  report->pwrite_iblk_read_ms = (double)report->st.pwrite_ns_iblk_read / 1000000.0;
  report->pwrite_iblk_write_ms = (double)report->st.pwrite_ns_iblk_write / 1000000.0;
  report->pwrite_iblk_write_p50_ms = (double)report->st.pwrite_iblk_write_p50_ns / 1000000.0;
  report->pwrite_iblk_write_p95_ms = (double)report->st.pwrite_iblk_write_p95_ns / 1000000.0;
  report->pwrite_iblk_write_p99_ms = (double)report->st.pwrite_iblk_write_p99_ns / 1000000.0;
  report->iblk_write_hrl_put_ms = (double)report->st.iblk_write_ns_hrl_put / 1000000.0;
  report->iblk_write_legacy_blk_write_ms =
      (double)report->st.iblk_write_ns_legacy_blk_write / 1000000.0;
  report->iblk_write_dec_ref_ms = (double)report->st.iblk_write_ns_dec_ref / 1000000.0;
  report->blk_alloc_scan_ms = (double)report->st.blk_alloc_ns_scan / 1000000.0;
  report->blk_alloc_claim_ms = (double)report->st.blk_alloc_ns_claim / 1000000.0;
  report->blk_alloc_set_usage_ms = (double)report->st.blk_alloc_ns_set_usage / 1000000.0;
  report->blk_alloc_retry_rate =
      (report->st.blk_alloc_calls > 0)
          ? (double)report->st.blk_alloc_claim_retries / (double)report->st.blk_alloc_calls
          : 0.0;
  report->blk_set_usage_bit_ms = (double)report->st.blk_set_usage_ns_bit_update / 1000000.0;
  report->blk_set_usage_freecnt_ms = (double)report->st.blk_set_usage_ns_freecnt_update / 1000000.0;
  report->blk_set_usage_wtime_ms = (double)report->st.blk_set_usage_ns_wtime_update / 1000000.0;
}

static void kafsctl_stats_compute_bg_metrics(kafs_stats_report_t *report)
{
  report->copy_share_hit_rate =
      (report->st.copy_share_attempt_blocks > 0)
          ? (double)report->st.copy_share_done_blocks / (double)report->st.copy_share_attempt_blocks
          : 0.0;
  report->bg_dedup_ops = report->st.bg_dedup_replacements + report->st.bg_dedup_retries;
  report->bg_dedup_retry_rate = (report->bg_dedup_ops > 0) ? (double)report->st.bg_dedup_retries /
                                                                 (double)report->bg_dedup_ops
                                                           : 0.0;
  report->bg_dedup_direct_hit_rate =
      (report->st.bg_dedup_direct_candidates > 0)
          ? (double)report->st.bg_dedup_direct_hits / (double)report->st.bg_dedup_direct_candidates
          : 0.0;
  report->hrl_rescue_hit_rate =
      (report->st.hrl_rescue_attempts > 0)
          ? (double)report->st.hrl_rescue_hits / (double)report->st.hrl_rescue_attempts
          : 0.0;
  report->pending_worker_start_fail_rate = (report->st.pending_worker_start_calls > 0)
                                               ? (double)report->st.pending_worker_start_failures /
                                                     (double)report->st.pending_worker_start_calls
                                               : 0.0;
}

static void kafsctl_stats_compute_report(kafs_stats_report_t *report)
{
  kafsctl_stats_compute_capacity(report);
  kafsctl_stats_compute_lock_metrics(report);
  kafsctl_stats_compute_io_metrics(report);
  kafsctl_stats_compute_bg_metrics(report);

  if (report->st.tombstone_inodes > 0 &&
      (report->st.tombstone_oldest_dtime_sec != 0 || report->st.tombstone_oldest_dtime_nsec != 0))
  {
    struct timespec tombstone_oldest = {
        .tv_sec = (time_t)report->st.tombstone_oldest_dtime_sec,
        .tv_nsec = (long)report->st.tombstone_oldest_dtime_nsec,
    };
    fmt_time(report->tombstone_oldest_buf, &tombstone_oldest);
  }
}

static int kafsctl_stats_print_json(const kafs_stats_report_t *report)
{
  const kafs_stats_t *st = &report->st;
  printf("{\n");
  printf("  \"version\": %" PRIu32 ",\n", st->version);
  printf("  \"request_flags\": %" PRIu32 ",\n", st->request_flags);
  printf("  \"result_flags\": %" PRIu32 ",\n", st->result_flags);
  printf("  \"verbose_scan\": %s,\n", report->have_verbose_scan ? "true" : "false");
  printf("  \"blksize\": %" PRIu32 ",\n", st->blksize);
  printf("  \"fs_blocks_total\": %" PRIu64 ",\n", st->fs_blocks_total);
  printf("  \"fs_blocks_free\": %" PRIu64 ",\n", st->fs_blocks_free);
  printf("  \"fs_inodes_total\": %" PRIu64 ",\n", st->fs_inodes_total);
  printf("  \"fs_inodes_free\": %" PRIu64 ",\n", st->fs_inodes_free);
  printf("  \"tombstone_inodes\": %" PRIu64 ",\n", st->tombstone_inodes);
  printf("  \"tombstone_oldest_dtime_sec\": %" PRIu64 ",\n", st->tombstone_oldest_dtime_sec);
  printf("  \"tombstone_oldest_dtime_nsec\": %" PRIu64 ",\n", st->tombstone_oldest_dtime_nsec);
  printf("  \"hrl_entries_total\": %" PRIu64 ",\n", st->hrl_entries_total);
  printf("  \"hrl_entries_used\": %" PRIu64 ",\n", st->hrl_entries_used);
  printf("  \"hrl_entries_duplicated\": %" PRIu64 ",\n", st->hrl_entries_duplicated);
  printf("  \"hrl_refcnt_sum\": %" PRIu64 ",\n", st->hrl_refcnt_sum);
  printf("  \"logical_bytes\": %" PRIu64 ",\n", report->logical_bytes);
  printf("  \"unique_bytes\": %" PRIu64 ",\n", report->unique_bytes);
  printf("  \"saved_bytes\": %" PRIu64 ",\n", report->saved_bytes);
  printf("  \"dedup_ratio\": %.6f,\n", report->dedup_ratio);
  printf("  \"hrl_put_calls\": %" PRIu64 ",\n", st->hrl_put_calls);
  printf("  \"hrl_put_hits\": %" PRIu64 ",\n", st->hrl_put_hits);
  printf("  \"hrl_put_misses\": %" PRIu64 ",\n", st->hrl_put_misses);
  printf("  \"hrl_put_fallback_legacy\": %" PRIu64 ",\n", st->hrl_put_fallback_legacy);
  printf("  \"hrl_put_ns_hash\": %" PRIu64 ",\n", st->hrl_put_ns_hash);
  printf("  \"hrl_put_ns_find\": %" PRIu64 ",\n", st->hrl_put_ns_find);
  printf("  \"hrl_put_ns_cmp_content\": %" PRIu64 ",\n", st->hrl_put_ns_cmp_content);
  printf("  \"hrl_put_ns_slot_alloc\": %" PRIu64 ",\n", st->hrl_put_ns_slot_alloc);
  printf("  \"hrl_put_ns_blk_alloc\": %" PRIu64 ",\n", st->hrl_put_ns_blk_alloc);
  printf("  \"hrl_put_ns_blk_write\": %" PRIu64 ",\n", st->hrl_put_ns_blk_write);
  printf("  \"hrl_put_chain_steps\": %" PRIu64 ",\n", st->hrl_put_chain_steps);
  printf("  \"hrl_put_cmp_calls\": %" PRIu64 ",\n", st->hrl_put_cmp_calls);
  printf("  \"hrl_put_hash_ms\": %.3f,\n", report->hrl_put_hash_ms);
  printf("  \"hrl_put_find_ms\": %.3f,\n", report->hrl_put_find_ms);
  printf("  \"hrl_put_cmp_ms\": %.3f,\n", report->hrl_put_cmp_ms);
  printf("  \"hrl_put_slot_alloc_ms\": %.3f,\n", report->hrl_put_slot_alloc_ms);
  printf("  \"hrl_put_blk_alloc_ms\": %.3f,\n", report->hrl_put_blk_alloc_ms);
  printf("  \"hrl_put_blk_write_ms\": %.3f,\n", report->hrl_put_blk_write_ms);
  printf("  \"hrl_put_avg_chain_steps\": %.3f,\n", report->hrl_put_avg_chain_steps);
  printf("  \"hrl_put_avg_cmp_calls\": %.3f,\n", report->hrl_put_avg_cmp_calls);
  printf("  \"hrl_put_hit_rate\": %.6f,\n", report->hit_rate);
  printf("  \"hrl_rescue_attempts\": %" PRIu64 ",\n", st->hrl_rescue_attempts);
  printf("  \"hrl_rescue_hits\": %" PRIu64 ",\n", st->hrl_rescue_hits);
  printf("  \"hrl_rescue_evicts\": %" PRIu64 ",\n", st->hrl_rescue_evicts);
  printf("  \"hrl_rescue_hit_rate\": %.6f,\n", report->hrl_rescue_hit_rate);
  printf("  \"lock_inode_acquire\": %" PRIu64 ",\n", st->lock_inode_acquire);
  printf("  \"lock_inode_contended\": %" PRIu64 ",\n", st->lock_inode_contended);
  printf("  \"lock_inode_wait_ns\": %" PRIu64 ",\n", st->lock_inode_wait_ns);
  printf("  \"lock_inode_contended_rate\": %.6f,\n", report->lock_inode_cont_rate);
  printf("  \"lock_inode_wait_ms\": %.3f,\n", report->lock_inode_wait_ms);
  printf("  \"lock_inode_alloc_acquire\": %" PRIu64 ",\n", st->lock_inode_alloc_acquire);
  printf("  \"lock_inode_alloc_contended\": %" PRIu64 ",\n", st->lock_inode_alloc_contended);
  printf("  \"lock_inode_alloc_wait_ns\": %" PRIu64 ",\n", st->lock_inode_alloc_wait_ns);
  printf("  \"lock_inode_alloc_contended_rate\": %.6f,\n", report->lock_inode_alloc_cont_rate);
  printf("  \"lock_inode_alloc_wait_ms\": %.3f,\n", report->lock_inode_alloc_wait_ms);
  printf("  \"lock_bitmap_acquire\": %" PRIu64 ",\n", st->lock_bitmap_acquire);
  printf("  \"lock_bitmap_contended\": %" PRIu64 ",\n", st->lock_bitmap_contended);
  printf("  \"lock_bitmap_wait_ns\": %" PRIu64 ",\n", st->lock_bitmap_wait_ns);
  printf("  \"lock_bitmap_contended_rate\": %.6f,\n", report->lock_bitmap_cont_rate);
  printf("  \"lock_bitmap_wait_ms\": %.3f,\n", report->lock_bitmap_wait_ms);
  printf("  \"lock_hrl_bucket_acquire\": %" PRIu64 ",\n", st->lock_hrl_bucket_acquire);
  printf("  \"lock_hrl_bucket_contended\": %" PRIu64 ",\n", st->lock_hrl_bucket_contended);
  printf("  \"lock_hrl_bucket_wait_ns\": %" PRIu64 ",\n", st->lock_hrl_bucket_wait_ns);
  printf("  \"lock_hrl_bucket_contended_rate\": %.6f,\n", report->lock_hrl_bucket_cont_rate);
  printf("  \"lock_hrl_bucket_wait_ms\": %.3f,\n", report->lock_hrl_bucket_wait_ms);
  printf("  \"lock_hrl_global_acquire\": %" PRIu64 ",\n", st->lock_hrl_global_acquire);
  printf("  \"lock_hrl_global_contended\": %" PRIu64 ",\n", st->lock_hrl_global_contended);
  printf("  \"lock_hrl_global_wait_ns\": %" PRIu64 ",\n", st->lock_hrl_global_wait_ns);
  printf("  \"lock_hrl_global_contended_rate\": %.6f,\n", report->lock_hrl_global_cont_rate);
  printf("  \"lock_hrl_global_wait_ms\": %.3f,\n", report->lock_hrl_global_wait_ms);
  printf("  \"access_calls\": %" PRIu64 ",\n", st->access_calls);
  printf("  \"access_path_walk_calls\": %" PRIu64 ",\n", st->access_path_walk_calls);
  printf("  \"access_fh_fastpath_hits\": %" PRIu64 ",\n", st->access_fh_fastpath_hits);
  printf("  \"access_path_components\": %" PRIu64 ",\n", st->access_path_components);
  printf("  \"access_fh_fastpath_rate\": %.6f,\n", report->access_fh_fastpath_rate);
  printf("  \"access_avg_components\": %.6f,\n", report->access_avg_components);
  printf("  \"dir_snapshot_calls\": %" PRIu64 ",\n", st->dir_snapshot_calls);
  printf("  \"dir_snapshot_bytes\": %" PRIu64 ",\n", st->dir_snapshot_bytes);
  printf("  \"dir_snapshot_avg_bytes\": %.3f,\n", report->dir_snapshot_avg_bytes);
  printf("  \"dir_snapshot_meta_load_calls\": %" PRIu64 ",\n", st->dir_snapshot_meta_load_calls);
  printf("  \"dirent_view_next_calls\": %" PRIu64 ",\n", st->dirent_view_next_calls);
  printf("  \"pwrite_calls\": %" PRIu64 ",\n", st->pwrite_calls);
  printf("  \"pwrite_bytes\": %" PRIu64 ",\n", st->pwrite_bytes);
  printf("  \"pwrite_ns_iblk_read\": %" PRIu64 ",\n", st->pwrite_ns_iblk_read);
  printf("  \"pwrite_ns_iblk_write\": %" PRIu64 ",\n", st->pwrite_ns_iblk_write);
  printf("  \"pwrite_iblk_write_sample_count\": %" PRIu64 ",\n",
         st->pwrite_iblk_write_sample_count);
  printf("  \"pwrite_iblk_write_sample_cap\": %" PRIu64 ",\n", st->pwrite_iblk_write_sample_cap);
  printf("  \"pwrite_iblk_write_p50_ns\": %" PRIu64 ",\n", st->pwrite_iblk_write_p50_ns);
  printf("  \"pwrite_iblk_write_p95_ns\": %" PRIu64 ",\n", st->pwrite_iblk_write_p95_ns);
  printf("  \"pwrite_iblk_write_p99_ns\": %" PRIu64 ",\n", st->pwrite_iblk_write_p99_ns);
  printf("  \"iblk_write_ns_hrl_put\": %" PRIu64 ",\n", st->iblk_write_ns_hrl_put);
  printf("  \"iblk_write_ns_legacy_blk_write\": %" PRIu64 ",\n",
         st->iblk_write_ns_legacy_blk_write);
  printf("  \"iblk_write_ns_dec_ref\": %" PRIu64 ",\n", st->iblk_write_ns_dec_ref);
  printf("  \"blk_alloc_calls\": %" PRIu64 ",\n", st->blk_alloc_calls);
  printf("  \"blk_alloc_claim_retries\": %" PRIu64 ",\n", st->blk_alloc_claim_retries);
  printf("  \"blk_alloc_ns_scan\": %" PRIu64 ",\n", st->blk_alloc_ns_scan);
  printf("  \"blk_alloc_ns_claim\": %" PRIu64 ",\n", st->blk_alloc_ns_claim);
  printf("  \"blk_alloc_ns_set_usage\": %" PRIu64 ",\n", st->blk_alloc_ns_set_usage);
  printf("  \"blk_set_usage_calls\": %" PRIu64 ",\n", st->blk_set_usage_calls);
  printf("  \"blk_set_usage_alloc_calls\": %" PRIu64 ",\n", st->blk_set_usage_alloc_calls);
  printf("  \"blk_set_usage_free_calls\": %" PRIu64 ",\n", st->blk_set_usage_free_calls);
  printf("  \"blk_set_usage_ns_bit_update\": %" PRIu64 ",\n", st->blk_set_usage_ns_bit_update);
  printf("  \"blk_set_usage_ns_freecnt_update\": %" PRIu64 ",\n",
         st->blk_set_usage_ns_freecnt_update);
  printf("  \"blk_set_usage_ns_wtime_update\": %" PRIu64 ",\n", st->blk_set_usage_ns_wtime_update);
  printf("  \"copy_share_attempt_blocks\": %" PRIu64 ",\n", st->copy_share_attempt_blocks);
  printf("  \"copy_share_done_blocks\": %" PRIu64 ",\n", st->copy_share_done_blocks);
  printf("  \"copy_share_fallback_blocks\": %" PRIu64 ",\n", st->copy_share_fallback_blocks);
  printf("  \"copy_share_skip_unaligned\": %" PRIu64 ",\n", st->copy_share_skip_unaligned);
  printf("  \"copy_share_skip_dst_inline\": %" PRIu64 ",\n", st->copy_share_skip_dst_inline);
  printf("  \"bg_dedup_replacements\": %" PRIu64 ",\n", st->bg_dedup_replacements);
  printf("  \"bg_dedup_evicts\": %" PRIu64 ",\n", st->bg_dedup_evicts);
  printf("  \"bg_dedup_retries\": %" PRIu64 ",\n", st->bg_dedup_retries);
  printf("  \"bg_dedup_steps\": %" PRIu64 ",\n", st->bg_dedup_steps);
  printf("  \"bg_dedup_scanned_blocks\": %" PRIu64 ",\n", st->bg_dedup_scanned_blocks);
  printf("  \"bg_dedup_direct_candidates\": %" PRIu64 ",\n", st->bg_dedup_direct_candidates);
  printf("  \"bg_dedup_direct_hits\": %" PRIu64 ",\n", st->bg_dedup_direct_hits);
  printf("  \"bg_dedup_direct_hit_rate\": %.6f,\n", report->bg_dedup_direct_hit_rate);
  printf("  \"bg_dedup_index_evicts\": %" PRIu64 ",\n", st->bg_dedup_index_evicts);
  printf("  \"bg_dedup_cooldowns\": %" PRIu64 ",\n", st->bg_dedup_cooldowns);
  printf("  \"bg_dedup_mode\": %" PRIu32 ",\n", st->bg_dedup_mode);
  printf("  \"bg_dedup_mode_str\": \"%s\",\n", bg_dedup_mode_str(st->bg_dedup_mode));
  printf("  \"bg_dedup_telemetry_valid\": %" PRIu32 ",\n", st->bg_dedup_telemetry_valid);
  printf("  \"bg_dedup_last_scanned_blocks\": %" PRIu64 ",\n", st->bg_dedup_last_scanned_blocks);
  printf("  \"bg_dedup_last_direct_candidates\": %" PRIu64 ",\n",
         st->bg_dedup_last_direct_candidates);
  printf("  \"bg_dedup_last_replacements\": %" PRIu64 ",\n", st->bg_dedup_last_replacements);
  printf("  \"bg_dedup_idle_skip_streak\": %" PRIu64 ",\n", st->bg_dedup_idle_skip_streak);
  printf("  \"bg_dedup_cold_start_due_ms\": %" PRIu64 ",\n", st->bg_dedup_cold_start_due_ms);
  printf("  \"pending_queue_depth\": %" PRIu64 ",\n", st->pending_queue_depth);
  printf("  \"pending_queue_capacity\": %" PRIu64 ",\n", st->pending_queue_capacity);
  printf("  \"pending_queue_head\": %" PRIu64 ",\n", st->pending_queue_head);
  printf("  \"pending_queue_tail\": %" PRIu64 ",\n", st->pending_queue_tail);
  printf("  \"pending_worker_start_calls\": %" PRIu64 ",\n", st->pending_worker_start_calls);
  printf("  \"pending_worker_start_failures\": %" PRIu64 ",\n", st->pending_worker_start_failures);
  printf("  \"pending_worker_start_fail_rate\": %.6f,\n", report->pending_worker_start_fail_rate);
  printf("  \"pending_worker_start_last_error\": %" PRId32 ",\n",
         st->pending_worker_start_last_error);
  printf("  \"pending_worker_lwp_tid\": %" PRId32 ",\n", st->pending_worker_lwp_tid);
  printf("  \"pending_worker_running\": %" PRId32 ",\n", st->pending_worker_running);
  printf("  \"pending_worker_stop_flag\": %" PRId32 ",\n", st->pending_worker_stop_flag);
  printf("  \"pending_worker_main_entries\": %" PRIu64 ",\n", st->pending_worker_main_entries);
  printf("  \"pending_worker_main_exits\": %" PRIu64 ",\n", st->pending_worker_main_exits);
  printf("  \"pending_resolved\": %" PRIu64 ",\n", st->pending_resolved);
  printf("  \"pending_old_block_freed\": %" PRIu64 ",\n", st->pending_old_block_freed);
  printf("  \"bg_dedup_retry_rate\": %.6f,\n", report->bg_dedup_retry_rate);
  printf("  \"copy_share_hit_rate\": %.6f,\n", report->copy_share_hit_rate);
  printf("  \"pwrite_iblk_read_ms\": %.3f,\n", report->pwrite_iblk_read_ms);
  printf("  \"pwrite_iblk_write_ms\": %.3f,\n", report->pwrite_iblk_write_ms);
  printf("  \"pwrite_iblk_write_p50_ms\": %.3f,\n", report->pwrite_iblk_write_p50_ms);
  printf("  \"pwrite_iblk_write_p95_ms\": %.3f,\n", report->pwrite_iblk_write_p95_ms);
  printf("  \"pwrite_iblk_write_p99_ms\": %.3f,\n", report->pwrite_iblk_write_p99_ms);
  printf("  \"iblk_write_hrl_put_ms\": %.3f,\n", report->iblk_write_hrl_put_ms);
  printf("  \"iblk_write_legacy_blk_write_ms\": %.3f,\n", report->iblk_write_legacy_blk_write_ms);
  printf("  \"iblk_write_dec_ref_ms\": %.3f,\n", report->iblk_write_dec_ref_ms);
  printf("  \"blk_alloc_scan_ms\": %.3f,\n", report->blk_alloc_scan_ms);
  printf("  \"blk_alloc_claim_ms\": %.3f,\n", report->blk_alloc_claim_ms);
  printf("  \"blk_alloc_set_usage_ms\": %.3f,\n", report->blk_alloc_set_usage_ms);
  printf("  \"blk_alloc_retry_rate\": %.6f,\n", report->blk_alloc_retry_rate);
  printf("  \"blk_set_usage_bit_ms\": %.3f,\n", report->blk_set_usage_bit_ms);
  printf("  \"blk_set_usage_freecnt_ms\": %.3f,\n", report->blk_set_usage_freecnt_ms);
  printf("  \"blk_set_usage_wtime_ms\": %.3f\n", report->blk_set_usage_wtime_ms);
  printf("}\n");
  return 0;
}

static int kafsctl_stats_print_text(const kafs_stats_report_t *report, kafs_unit_t unit)
{
  const kafs_stats_t *st = &report->st;
  printf("kafs fsstat v%" PRIu32 "\n", st->version);
  printf("  mode: %s\n", report->have_verbose_scan ? "verbose" : "lightweight");
  if (!report->have_verbose_scan)
    printf("  note: tombstone/HRL full scans are skipped by default; rerun with -v for them.\n");
  printf("  summary.capacity:\n");
  printf("    fs_blocks: used=");
  print_bytes(report->fs_blocks_used * (uint64_t)st->blksize, unit);
  printf(" / total=");
  print_bytes(st->fs_blocks_total * (uint64_t)st->blksize, unit);
  printf(" (%.2f%%)\n", report->fs_blocks_used_pct);
  printf("    fs_inodes: used=%" PRIu64 " / total=%" PRIu64 " (%.2f%%)\n", report->fs_inodes_used,
         st->fs_inodes_total, report->fs_inodes_used_pct);
  if (report->have_verbose_scan)
    printf("    hrl_entries: used=%" PRIu64 " / total=%" PRIu64 " (%.2f%%)\n", st->hrl_entries_used,
           st->hrl_entries_total, report->hrl_entries_used_pct);
  else
    printf("    hrl_entries: total=%" PRIu64 " (used count requires -v)\n", st->hrl_entries_total);

  printf("  summary.ratios:\n");
  if (report->have_verbose_scan)
    printf("    dedup_ratio: %.3f (logical/unique)\n", report->dedup_ratio);
  else
    printf("    dedup_ratio: unavailable without -v\n");
  printf("    write_path: hrl=%.2f%% legacy_direct=%.2f%% (hrl_calls=%" PRIu64
         " fallback_legacy=%" PRIu64 ")\n",
         report->hrl_path_rate, report->legacy_path_rate, st->hrl_put_calls,
         st->hrl_put_fallback_legacy);
  printf("    direct_to_hrl: %.6f (legacy_direct/hrl_calls)\n", report->direct_to_hrl_ratio);
  printf("    hrl_hit_miss: hit=%.2f%% miss=%.2f%%\n", report->hrl_hit_rate_pct,
         report->hrl_miss_rate_pct);
  printf("    copy_share_hit: %.2f%% (done/attempt)\n", report->copy_share_hit_rate * 100.0);

  printf("  blksize: ");
  print_bytes(st->blksize, unit);
  printf("\n");

  printf("  fs: blocks total=%" PRIu64 " (", st->fs_blocks_total);
  print_bytes(st->fs_blocks_total * (uint64_t)st->blksize, unit);
  printf(") free=%" PRIu64 " (", st->fs_blocks_free);
  print_bytes(st->fs_blocks_free * (uint64_t)st->blksize, unit);
  printf(")\n");
  printf("      inodes total=%" PRIu64 " free=%" PRIu64 "\n", st->fs_inodes_total,
         st->fs_inodes_free);
  if (!report->have_verbose_scan)
    printf("      tombstones: omitted without -v\n");
  else if (st->tombstone_inodes > 0)
    printf("      tombstones count=%" PRIu64 " oldest_dtime=%s (%" PRIu64 ".%09" PRIu64 ")\n",
           st->tombstone_inodes, report->tombstone_oldest_buf, st->tombstone_oldest_dtime_sec,
           st->tombstone_oldest_dtime_nsec);
  else
    printf("      tombstones count=0 oldest_dtime=none\n");

  if (report->have_verbose_scan)
  {
    printf("  hrl: entries used=%" PRIu64 "/%" PRIu64 " duplicated=%" PRIu64 " refsum=%" PRIu64
           "\n",
           st->hrl_entries_used, st->hrl_entries_total, st->hrl_entries_duplicated,
           st->hrl_refcnt_sum);
    printf("  dedup: logical=");
    print_bytes(report->logical_bytes, unit);
    printf(" unique=");
    print_bytes(report->unique_bytes, unit);
    printf(" saved=");
    print_bytes(report->saved_bytes, unit);
    printf(" ratio=%.3f\n", report->dedup_ratio);
  }
  else
  {
    printf("  hrl: total entries=%" PRIu64 " (used/dup/refsum require -v)\n",
           st->hrl_entries_total);
    printf("  dedup: unavailable without -v\n");
  }

  printf("  hrl_put: calls=%" PRIu64 " hits=%" PRIu64 " misses=%" PRIu64 " fallback_legacy=%" PRIu64
         " hit_rate=%.3f\n",
         st->hrl_put_calls, st->hrl_put_hits, st->hrl_put_misses, st->hrl_put_fallback_legacy,
         report->hit_rate);
  printf("  hrl_rescue: attempts=%" PRIu64 " hits=%" PRIu64 " evicts=%" PRIu64 " hit_rate=%.3f\n",
         st->hrl_rescue_attempts, st->hrl_rescue_hits, st->hrl_rescue_evicts,
         report->hrl_rescue_hit_rate);
  printf("  hrl_put_decomp: hash_ms=%.3f find_ms=%.3f cmp_ms=%.3f slot_alloc_ms=%.3f "
         "blk_alloc_ms=%.3f blk_write_ms=%.3f avg_chain_steps=%.3f avg_cmp_calls=%.3f\n",
         report->hrl_put_hash_ms, report->hrl_put_find_ms, report->hrl_put_cmp_ms,
         report->hrl_put_slot_alloc_ms, report->hrl_put_blk_alloc_ms, report->hrl_put_blk_write_ms,
         report->hrl_put_avg_chain_steps, report->hrl_put_avg_cmp_calls);
  printf("  lock[inode]: acquire=%" PRIu64 " contended=%" PRIu64 " rate=%.3f wait_ms=%.3f\n",
         st->lock_inode_acquire, st->lock_inode_contended, report->lock_inode_cont_rate,
         report->lock_inode_wait_ms);
  printf("  lock[inode_alloc]: acquire=%" PRIu64 " contended=%" PRIu64 " rate=%.3f wait_ms=%.3f\n",
         st->lock_inode_alloc_acquire, st->lock_inode_alloc_contended,
         report->lock_inode_alloc_cont_rate, report->lock_inode_alloc_wait_ms);
  printf("  lock[bitmap]: acquire=%" PRIu64 " contended=%" PRIu64 " rate=%.3f wait_ms=%.3f\n",
         st->lock_bitmap_acquire, st->lock_bitmap_contended, report->lock_bitmap_cont_rate,
         report->lock_bitmap_wait_ms);
  printf("  lock[hrl_bucket]: acquire=%" PRIu64 " contended=%" PRIu64 " rate=%.3f wait_ms=%.3f\n",
         st->lock_hrl_bucket_acquire, st->lock_hrl_bucket_contended,
         report->lock_hrl_bucket_cont_rate, report->lock_hrl_bucket_wait_ms);
  printf("  lock[hrl_global]: acquire=%" PRIu64 " contended=%" PRIu64 " rate=%.3f wait_ms=%.3f\n",
         st->lock_hrl_global_acquire, st->lock_hrl_global_contended,
         report->lock_hrl_global_cont_rate, report->lock_hrl_global_wait_ms);
  printf("  metadata_lookup: access_calls=%" PRIu64 " path_walk_calls=%" PRIu64
         " fh_fastpath_hits=%" PRIu64 " fh_fastpath_rate=%.3f avg_components=%.3f\n",
         st->access_calls, st->access_path_walk_calls, st->access_fh_fastpath_hits,
         report->access_fh_fastpath_rate, report->access_avg_components);
  printf("                   dir_snapshot_calls=%" PRIu64 " snapshot_bytes=%" PRIu64
         " avg_snapshot_bytes=%.3f meta_load_calls=%" PRIu64 " view_next_calls=%" PRIu64 "\n",
         st->dir_snapshot_calls, st->dir_snapshot_bytes, report->dir_snapshot_avg_bytes,
         st->dir_snapshot_meta_load_calls, st->dirent_view_next_calls);
  printf("  pwrite: calls=%" PRIu64 " bytes=%" PRIu64 " iblk_read_ms=%.3f iblk_write_ms=%.3f\n",
         st->pwrite_calls, st->pwrite_bytes, report->pwrite_iblk_read_ms,
         report->pwrite_iblk_write_ms);
  printf("          iblk_write_lat: samples=%" PRIu64 "/%" PRIu64
         " p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f\n",
         st->pwrite_iblk_write_sample_count, st->pwrite_iblk_write_sample_cap,
         report->pwrite_iblk_write_p50_ms, report->pwrite_iblk_write_p95_ms,
         report->pwrite_iblk_write_p99_ms);
  printf("  iblk_write: hrl_put_ms=%.3f legacy_blk_write_ms=%.3f dec_ref_ms=%.3f\n",
         report->iblk_write_hrl_put_ms, report->iblk_write_legacy_blk_write_ms,
         report->iblk_write_dec_ref_ms);
  printf("  blk_alloc: calls=%" PRIu64 " retries=%" PRIu64 " retry_rate=%.3f scan_ms=%.3f "
         "claim_ms=%.3f set_usage_ms=%.3f\n",
         st->blk_alloc_calls, st->blk_alloc_claim_retries, report->blk_alloc_retry_rate,
         report->blk_alloc_scan_ms, report->blk_alloc_claim_ms, report->blk_alloc_set_usage_ms);
  printf("  blk_set_usage: calls=%" PRIu64 " alloc_calls=%" PRIu64 " free_calls=%" PRIu64
         " bit_ms=%.3f freecnt_ms=%.3f wtime_ms=%.3f\n",
         st->blk_set_usage_calls, st->blk_set_usage_alloc_calls, st->blk_set_usage_free_calls,
         report->blk_set_usage_bit_ms, report->blk_set_usage_freecnt_ms,
         report->blk_set_usage_wtime_ms);
  printf("  copy_share: attempt_blocks=%" PRIu64 " done_blocks=%" PRIu64 " fallback_blocks=%" PRIu64
         " skip_unaligned=%" PRIu64 " skip_dst_inline=%" PRIu64 " hit_rate=%.3f\n",
         st->copy_share_attempt_blocks, st->copy_share_done_blocks, st->copy_share_fallback_blocks,
         st->copy_share_skip_unaligned, st->copy_share_skip_dst_inline,
         report->copy_share_hit_rate);
  printf("  bg_dedup: replacements=%" PRIu64 " evicts=%" PRIu64 " retries=%" PRIu64
         " retry_rate=%.3f\n",
         st->bg_dedup_replacements, st->bg_dedup_evicts, st->bg_dedup_retries,
         report->bg_dedup_retry_rate);
  printf("            steps=%" PRIu64 " scanned_blocks=%" PRIu64 " direct_candidates=%" PRIu64
         " direct_hits=%" PRIu64 " direct_hit_rate=%.3f index_evicts=%" PRIu64 " cooldowns=%" PRIu64
         "\n",
         st->bg_dedup_steps, st->bg_dedup_scanned_blocks, st->bg_dedup_direct_candidates,
         st->bg_dedup_direct_hits, report->bg_dedup_direct_hit_rate, st->bg_dedup_index_evicts,
         st->bg_dedup_cooldowns);
  printf("            mode=%" PRIu32 " (%s) telemetry_valid=%" PRIu32 " last_scanned=%" PRIu64
         " last_direct_candidates=%" PRIu64 " last_replacements=%" PRIu64
         " idle_skip_streak=%" PRIu64 " cold_due_ms=%" PRIu64 "\n",
         st->bg_dedup_mode, bg_dedup_mode_str(st->bg_dedup_mode), st->bg_dedup_telemetry_valid,
         st->bg_dedup_last_scanned_blocks, st->bg_dedup_last_direct_candidates,
         st->bg_dedup_last_replacements, st->bg_dedup_idle_skip_streak,
         st->bg_dedup_cold_start_due_ms);
  printf("  pending: depth=%" PRIu64 "/%" PRIu64 " head=%" PRIu64 " tail=%" PRIu64 "\n",
         st->pending_queue_depth, st->pending_queue_capacity, st->pending_queue_head,
         st->pending_queue_tail);
  printf("           worker running=%" PRId32 " stop=%" PRId32 " start_calls=%" PRIu64
         " start_failures=%" PRIu64 " fail_rate=%.3f last_error=%" PRId32 " lwp_tid=%" PRId32 "\n",
         st->pending_worker_running, st->pending_worker_stop_flag, st->pending_worker_start_calls,
         st->pending_worker_start_failures, report->pending_worker_start_fail_rate,
         st->pending_worker_start_last_error, st->pending_worker_lwp_tid);
  printf("           worker_main entries=%" PRIu64 " exits=%" PRIu64 "\n",
         st->pending_worker_main_entries, st->pending_worker_main_exits);
  printf("           pending_resolved=%" PRIu64 " old_block_freed=%" PRIu64 "\n",
         st->pending_resolved, st->pending_old_block_freed);
  return 0;
}

static int cmd_stats(const char *mnt, int json, int verbose, kafs_unit_t unit)
{
  int fd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }

  kafs_stats_t st;
  memset(&st, 0, sizeof(st));
  st.struct_size = (uint32_t)sizeof(st);
  if (verbose)
    st.request_flags |= KAFS_STATS_F_VERBOSE_SCAN;
  if (ioctl(fd, KAFS_IOCTL_GET_STATS, &st) != 0)
  {
    perror("ioctl(KAFS_IOCTL_GET_STATS)");
    close(fd);
    return 1;
  }
  close(fd);
  kafs_stats_report_t report;
  kafsctl_stats_report_init(&report, &st);
  kafsctl_stats_compute_report(&report);

  if (json)
    return kafsctl_stats_print_json(&report);
  return kafsctl_stats_print_text(&report, unit);
}

static void fmt_time(char out[64], const struct timespec *ts)
{
  if (!out)
    return;
  if (!ts)
  {
    out[0] = '\0';
    return;
  }
  time_t t = ts->tv_sec;
  struct tm tm;
  if (localtime_r(&t, &tm) == NULL)
  {
    snprintf(out, 64, "%lld", (long long)ts->tv_sec);
    return;
  }
  strftime(out, 64, "%Y-%m-%d %H:%M:%S", &tm);
}

static int cmd_stat(const char *mnt, const char *path)
{
  kafs_path_ref_t ref;
  int rc = resolve_path_ref(mnt, path, &ref);
  if (rc != 0)
    return rc;

  struct stat st;
  if (fstatat(ref.dfd, ref.rel, &st, AT_SYMLINK_NOFOLLOW) != 0)
  {
    perror("fstatat");
    close_path_ref(&ref);
    return 1;
  }

  const char *t = "unknown";
  if (S_ISREG(st.st_mode))
    t = "file";
  else if (S_ISDIR(st.st_mode))
    t = "dir";
  else if (S_ISLNK(st.st_mode))
    t = "symlink";
  else if (S_ISCHR(st.st_mode))
    t = "char";
  else if (S_ISBLK(st.st_mode))
    t = "block";
  else if (S_ISFIFO(st.st_mode))
    t = "fifo";
  else if (S_ISSOCK(st.st_mode))
    t = "sock";

  char at[64], mt[64], ct[64];
#if defined(__APPLE__)
  (void)at;
  (void)mt;
  (void)ct;
#else
  fmt_time(at, &st.st_atim);
  fmt_time(mt, &st.st_mtim);
  fmt_time(ct, &st.st_ctim);
#endif

  printf("path: %s\n", path);
  printf("type: %s\n", t);
  printf("mode: %04o\n", (unsigned int)(st.st_mode & 07777));
  printf("uid: %u\n", (unsigned int)st.st_uid);
  printf("gid: %u\n", (unsigned int)st.st_gid);
  printf("size: %lld\n", (long long)st.st_size);
  printf("nlink: %llu\n", (unsigned long long)st.st_nlink);
  printf("ino: %llu\n", (unsigned long long)st.st_ino);
#if !defined(__APPLE__)
  printf("atime: %s\n", at);
  printf("mtime: %s\n", mt);
  printf("ctime: %s\n", ct);
#endif

  close_path_ref(&ref);
  return 0;
}

static int cmd_cat(const char *mnt, const char *path)
{
  kafs_path_ref_t ref;
  int rc = resolve_path_ref(mnt, path, &ref);
  if (rc != 0)
    return rc;

  int fd = openat(ref.dfd, ref.rel, O_RDONLY);
  if (fd < 0)
  {
    perror("openat");
    close_path_ref(&ref);
    return 1;
  }

  char buf[64 * 1024];
  while (1)
  {
    ssize_t r = read(fd, buf, sizeof(buf));
    if (r < 0)
    {
      perror("read");
      close(fd);
      close_path_ref(&ref);
      return 1;
    }
    if (r == 0)
      break;
    ssize_t off = 0;
    while (off < r)
    {
      ssize_t w = write(STDOUT_FILENO, buf + off, (size_t)(r - off));
      if (w < 0)
      {
        perror("write");
        close(fd);
        close_path_ref(&ref);
        return 1;
      }
      off += w;
    }
  }

  close(fd);
  close_path_ref(&ref);
  return 0;
}

static int cmd_write(const char *mnt, const char *path)
{
  kafs_path_ref_t ref;
  int rc = resolve_path_ref(mnt, path, &ref);
  if (rc != 0)
    return rc;

  int fd = openat(ref.dfd, ref.rel, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0)
  {
    perror("openat");
    close_path_ref(&ref);
    return 1;
  }

  char buf[64 * 1024];
  while (1)
  {
    ssize_t r = read(STDIN_FILENO, buf, sizeof(buf));
    if (r < 0)
    {
      perror("read(stdin)");
      close(fd);
      close_path_ref(&ref);
      return 1;
    }
    if (r == 0)
      break;
    ssize_t off = 0;
    while (off < r)
    {
      ssize_t w = write(fd, buf + off, (size_t)(r - off));
      if (w < 0)
      {
        perror("write(file)");
        close(fd);
        close_path_ref(&ref);
        return 1;
      }
      off += w;
    }
  }

  close(fd);
  close_path_ref(&ref);
  return 0;
}

static int cmd_cp(const char *mnt, const char *src, const char *dst, int reflink)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int fd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }

  kafs_ioctl_copy_t req;
  memset(&req, 0, sizeof(req));
  req.struct_size = (uint32_t)sizeof(req);
  req.flags = reflink ? KAFS_IOCTL_COPY_F_REFLINK : 0;

  char srcbuf[KAFS_IOCTL_PATH_MAX];
  char dstbuf[KAFS_IOCTL_PATH_MAX];
  char src_rel[KAFS_IOCTL_PATH_MAX];
  char dst_rel[KAFS_IOCTL_PATH_MAX];
  const char *s = to_kafs_path(mnt_abs, src, srcbuf);
  const char *d = to_kafs_path(mnt_abs, dst, dstbuf);
  const char *s_mount = to_mount_rel_path(mnt_abs, src, src_rel);
  const char *d_mount = to_mount_rel_path(mnt_abs, dst, dst_rel);
  if (!s || !d || !s_mount || !d_mount)
  {
    fprintf(stderr, "invalid path\n");
    close(fd);
    return 2;
  }

  struct stat st_src;
  struct stat st_dst;
  if (fstatat(fd, s_mount, &st_src, 0) != 0)
  {
    perror("fstatat(src)");
    close(fd);
    return 1;
  }
  if (fstatat(fd, d_mount, &st_dst, 0) == 0 && st_src.st_ino == st_dst.st_ino)
  {
    fprintf(stderr, "cp: '%s' and '%s' are the same file\n", src, dst);
    close(fd);
    return 1;
  }

  snprintf(req.src, sizeof(req.src), "%s", s);
  snprintf(req.dst, sizeof(req.dst), "%s", d);

  if (ioctl(fd, KAFS_IOCTL_COPY, &req) != 0)
  {
    perror("ioctl(KAFS_IOCTL_COPY)");
    close(fd);
    return 1;
  }
  close(fd);
  return 0;
}

static int cmd_cp_auto(const char *src, const char *dst, int reflink)
{
  kafs_path_ref_t src_ref;
  kafs_path_ref_t dst_ref;
  init_path_ref(&src_ref);
  init_path_ref(&dst_ref);

  int src_rc = try_resolve_auto_path_ref(src, &src_ref);
  int dst_rc = try_resolve_auto_path_ref(dst, &dst_ref);
  int same_mount =
      (src_rc == 0 && dst_rc == 0 && strcmp(src_ref.mount, dst_ref.mount) == 0) ? 1 : 0;

  if (same_mount)
  {
    int rc = cmd_cp(src_ref.mount, src_ref.abs_path, dst_ref.abs_path, reflink);
    close_path_ref(&src_ref);
    close_path_ref(&dst_ref);
    return rc;
  }

  close_path_ref(&src_ref);
  close_path_ref(&dst_ref);

  if (reflink)
  {
    char *const argv[] = {"cp", "--reflink=always", (char *)src, (char *)dst, NULL};
    return run_external_command(argv);
  }

  char *const argv[] = {"cp", (char *)src, (char *)dst, NULL};
  return run_external_command(argv);
}

static int cmd_rm(const char *mnt, const char *path)
{
  kafs_path_ref_t ref;
  int rc = resolve_path_ref(mnt, path, &ref);
  if (rc != 0)
    return rc;

  if (unlinkat(ref.dfd, ref.rel, 0) != 0)
  {
    perror("unlinkat");
    close_path_ref(&ref);
    return 1;
  }
  close_path_ref(&ref);
  return 0;
}

static int cmd_rmdir(const char *mnt, const char *path)
{
  kafs_path_ref_t ref;
  int rc = resolve_path_ref(mnt, path, &ref);
  if (rc != 0)
    return rc;

  if (unlinkat(ref.dfd, ref.rel, AT_REMOVEDIR) != 0)
  {
    perror("unlinkat(AT_REMOVEDIR)");
    close_path_ref(&ref);
    return 1;
  }
  close_path_ref(&ref);
  return 0;
}

static int cmd_mkdir(const char *mnt, const char *path)
{
  kafs_path_ref_t ref;
  int rc = resolve_path_ref(mnt, path, &ref);
  if (rc != 0)
    return rc;

  if (mkdirat(ref.dfd, ref.rel, 0755) != 0)
  {
    perror("mkdirat");
    close_path_ref(&ref);
    return 1;
  }
  close_path_ref(&ref);
  return 0;
}

static int cmd_mv(const char *mnt, const char *src, const char *dst)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char srel[KAFS_IOCTL_PATH_MAX];
  char drel[KAFS_IOCTL_PATH_MAX];
  const char *s = to_mount_rel_path(mnt_abs, src, srel);
  const char *d = to_mount_rel_path(mnt_abs, dst, drel);
  if (!s || !d)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (renameat(dfd, s, dfd, d) != 0)
  {
    perror("renameat");
    close(dfd);
    return 1;
  }

  close(dfd);
  return 0;
}

static int cmd_mv_auto(const char *src, const char *dst)
{
  kafs_path_ref_t src_ref;
  kafs_path_ref_t dst_ref;
  init_path_ref(&src_ref);
  init_path_ref(&dst_ref);

  int src_rc = try_resolve_auto_path_ref(src, &src_ref);
  int dst_rc = try_resolve_auto_path_ref(dst, &dst_ref);
  int same_mount =
      (src_rc == 0 && dst_rc == 0 && strcmp(src_ref.mount, dst_ref.mount) == 0) ? 1 : 0;

  if (same_mount)
  {
    int rc = cmd_mv(src_ref.mount, src_ref.abs_path, dst_ref.abs_path);
    close_path_ref(&src_ref);
    close_path_ref(&dst_ref);
    return rc;
  }

  close_path_ref(&src_ref);
  close_path_ref(&dst_ref);

  char *const argv[] = {"mv", (char *)src, (char *)dst, NULL};
  return run_external_command(argv);
}

static int cmd_ln(const char *mnt, const char *src, const char *dst)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char srel[KAFS_IOCTL_PATH_MAX];
  char drel[KAFS_IOCTL_PATH_MAX];
  const char *s = to_mount_rel_path(mnt_abs, src, srel);
  const char *d = to_mount_rel_path(mnt_abs, dst, drel);
  if (!s || !d)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (linkat(dfd, s, dfd, d, 0) != 0)
  {
    perror("linkat");
    close(dfd);
    return 1;
  }

  close(dfd);
  return 0;
}

static int cmd_ln_auto(const char *src, const char *dst)
{
  kafs_path_ref_t src_ref;
  kafs_path_ref_t dst_ref;
  init_path_ref(&src_ref);
  init_path_ref(&dst_ref);

  int src_rc = try_resolve_auto_path_ref(src, &src_ref);
  int dst_rc = try_resolve_auto_path_ref(dst, &dst_ref);
  int same_mount =
      (src_rc == 0 && dst_rc == 0 && strcmp(src_ref.mount, dst_ref.mount) == 0) ? 1 : 0;

  if (!same_mount)
  {
    close_path_ref(&src_ref);
    close_path_ref(&dst_ref);
    errno = EXDEV;
    perror("linkat");
    return 1;
  }

  int rc = cmd_ln(src_ref.mount, src_ref.abs_path, dst_ref.abs_path);
  close_path_ref(&src_ref);
  close_path_ref(&dst_ref);
  return rc;
}

typedef struct kafs_ln_opts
{
  int symbolic;
  int force;
  int no_deref;
  int no_target_directory;
  int verbose;
  const char *target_dir;
} kafs_ln_opts_t;

static int copy_path_basename(char out[KAFS_IOCTL_PATH_MAX], const char *path)
{
  if (!path || !*path)
    return -EINVAL;

  const char *end = path + strlen(path);
  while (end > path && end[-1] == '/')
    --end;
  if (end == path)
  {
    if (snprintf(out, KAFS_IOCTL_PATH_MAX, "/") >= KAFS_IOCTL_PATH_MAX)
      return -ENAMETOOLONG;
    return 0;
  }

  const char *start = end;
  while (start > path && start[-1] != '/')
    --start;
  size_t len = (size_t)(end - start);
  if (len + 1 > KAFS_IOCTL_PATH_MAX)
    return -ENAMETOOLONG;
  memcpy(out, start, len);
  out[len] = '\0';
  return 0;
}

static int build_link_path(char out[KAFS_IOCTL_PATH_MAX], const char *dir, const char *source_name)
{
  char base[KAFS_IOCTL_PATH_MAX];
  int rc = copy_path_basename(base, source_name);
  if (rc != 0)
    return rc;
  return path_join_components(out, dir, base);
}

static int path_ref_is_directory(const char *mnt, const char *path, int no_deref)
{
  kafs_path_ref_t ref;
  int rc = resolve_path_ref(mnt, path, &ref);
  if (rc != 0)
    return rc;

  struct stat st;
  if (fstatat(ref.dfd, ref.rel, &st, no_deref ? AT_SYMLINK_NOFOLLOW : 0) != 0)
  {
    int err = errno;
    close_path_ref(&ref);
    if (err == ENOENT)
      return 0;
    perror("fstatat");
    return 1;
  }

  close_path_ref(&ref);
  return S_ISDIR(st.st_mode) ? 1 : 0;
}

static int remove_existing_link_destination(const char *mnt, const char *path)
{
  kafs_path_ref_t ref;
  int rc = resolve_path_ref(mnt, path, &ref);
  if (rc != 0)
    return rc;

  if (unlinkat(ref.dfd, ref.rel, 0) != 0)
  {
    int err = errno;
    close_path_ref(&ref);
    if (err == ENOENT)
      return 0;
    if (err == EISDIR || err == EPERM)
    {
      fprintf(stderr, "destination is a directory: %s\n", path);
      return 1;
    }
    errno = err;
    perror("unlinkat");
    return 1;
  }

  close_path_ref(&ref);
  return 0;
}

static int check_same_link_file_on_mount(const char *mnt, const char *src, const char *dst)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return -1;
  }

  char src_rel[KAFS_IOCTL_PATH_MAX];
  char dst_rel[KAFS_IOCTL_PATH_MAX];
  const char *s = to_mount_rel_path(mnt_abs, src, src_rel);
  const char *d = to_mount_rel_path(mnt_abs, dst, dst_rel);
  if (!s || !d)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return -1;
  }

  struct stat st_src;
  struct stat st_dst;
  if (fstatat(dfd, s, &st_src, 0) != 0)
  {
    perror("fstatat(src)");
    close(dfd);
    return -1;
  }
  if (fstatat(dfd, d, &st_dst, 0) != 0)
  {
    int err = errno;
    close(dfd);
    if (err == ENOENT)
      return 0;
    errno = err;
    perror("fstatat(dst)");
    return -1;
  }

  close(dfd);
  return st_src.st_ino == st_dst.st_ino;
}

static int reject_same_link_file_on_mount(const char *mnt, const char *src, const char *dst)
{
  int same_file = check_same_link_file_on_mount(mnt, src, dst);
  if (same_file <= 0)
    return same_file;

  fprintf(stderr, "ln: '%s' and '%s' are the same file\n", src, dst);
  return 1;
}

static int reject_same_link_file_auto(const char *src, const char *dst)
{
  kafs_path_ref_t src_ref;
  kafs_path_ref_t dst_ref;
  init_path_ref(&src_ref);
  init_path_ref(&dst_ref);

  int src_rc = try_resolve_auto_path_ref(src, &src_ref);
  int dst_rc = try_resolve_auto_path_ref(dst, &dst_ref);
  int same_mount =
      (src_rc == 0 && dst_rc == 0 && strcmp(src_ref.mount, dst_ref.mount) == 0) ? 1 : 0;
  if (!same_mount)
  {
    close_path_ref(&src_ref);
    close_path_ref(&dst_ref);
    return 0;
  }

  int rc = reject_same_link_file_on_mount(src_ref.mount, src_ref.abs_path, dst_ref.abs_path);
  close_path_ref(&src_ref);
  close_path_ref(&dst_ref);
  return rc;
}

static int is_ln_symbolic_option(const char *arg)
{
  return arg && (strcmp(arg, "-s") == 0 || strcmp(arg, "--symbolic") == 0);
}

static int cmd_ln_with_opts(const char *mnt, const char *src, const char *dst,
                            const kafs_ln_opts_t *opts)
{
  if (opts && opts->force)
  {
    int same_file_rc = reject_same_link_file_on_mount(mnt, src, dst);
    if (same_file_rc != 0)
      return same_file_rc < 0 ? 1 : same_file_rc;

    int rc = remove_existing_link_destination(mnt, dst);
    if (rc != 0)
      return rc;
  }

  int rc = cmd_ln(mnt, src, dst);
  if (rc == 0 && opts && opts->verbose)
    printf("linked %s -> %s\n", dst, src);
  return rc;
}

static int cmd_ln_auto_with_opts(const char *src, const char *dst, const kafs_ln_opts_t *opts)
{
  if (opts && opts->force)
  {
    int same_file_rc = reject_same_link_file_auto(src, dst);
    if (same_file_rc != 0)
      return same_file_rc < 0 ? 1 : same_file_rc;

    int rc = remove_existing_link_destination(NULL, dst);
    if (rc != 0)
      return rc;
  }

  int rc = cmd_ln_auto(src, dst);
  if (rc == 0 && opts && opts->verbose)
    printf("linked %s -> %s\n", dst, src);
  return rc;
}

static int cmd_symlink(const char *mnt, const char *target, const char *linkpath)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char lrel[KAFS_IOCTL_PATH_MAX];
  const char *l = to_mount_rel_path(mnt_abs, linkpath, lrel);
  if (!l)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (symlinkat(target, dfd, l) != 0)
  {
    perror("symlinkat");
    close(dfd);
    return 1;
  }

  close(dfd);
  return 0;
}

static int cmd_symlink_auto(const char *target, const char *linkpath)
{
  kafs_path_ref_t ref;
  int rc = resolve_path_ref(NULL, linkpath, &ref);
  if (rc != 0)
    return rc;

  if (symlinkat(target, ref.dfd, ref.rel) != 0)
  {
    perror("symlinkat");
    close_path_ref(&ref);
    return 1;
  }

  close_path_ref(&ref);
  return 0;
}

static int cmd_symlink_with_opts(const char *mnt, const char *target, const char *linkpath,
                                 const kafs_ln_opts_t *opts)
{
  if (opts && opts->force)
  {
    int rc = remove_existing_link_destination(mnt, linkpath);
    if (rc != 0)
      return rc;
  }

  int rc = cmd_symlink(mnt, target, linkpath);
  if (rc == 0 && opts && opts->verbose)
    printf("linked %s -> %s\n", linkpath, target);
  return rc;
}

static int cmd_symlink_auto_with_opts(const char *target, const char *linkpath,
                                      const kafs_ln_opts_t *opts)
{
  if (opts && opts->force)
  {
    int rc = remove_existing_link_destination(NULL, linkpath);
    if (rc != 0)
      return rc;
  }

  int rc = cmd_symlink_auto(target, linkpath);
  if (rc == 0 && opts && opts->verbose)
    printf("linked %s -> %s\n", linkpath, target);
  return rc;
}

static int parse_ln_target_directory_option(int argc, char **argv, int *index, const char *arg,
                                            kafs_ln_opts_t *opts)
{
  if (strcmp(arg, "-t") == 0 || strcmp(arg, "--target-directory") == 0)
  {
    if (*index + 1 >= argc)
    {
      fprintf(stderr, "missing argument for %s\n", arg);
      return 2;
    }
    opts->target_dir = argv[++(*index)];
    return 0;
  }

  if (strncmp(arg, "--target-directory=", 19) == 0)
  {
    opts->target_dir = arg + 19;
    return 0;
  }

  return -1;
}

static int parse_ln_flag_option(const char *arg, kafs_ln_opts_t *opts)
{
  if (is_ln_symbolic_option(arg))
    opts->symbolic = 1;
  else if (strcmp(arg, "-f") == 0 || strcmp(arg, "--force") == 0)
    opts->force = 1;
  else if (strcmp(arg, "-n") == 0 || strcmp(arg, "--no-dereference") == 0)
    opts->no_deref = 1;
  else if (strcmp(arg, "-T") == 0 || strcmp(arg, "--no-target-directory") == 0)
    opts->no_target_directory = 1;
  else if (strcmp(arg, "-v") == 0 || strcmp(arg, "--verbose") == 0)
    opts->verbose = 1;
  else
    return -1;

  return 0;
}

static int parse_ln_option_arg(int argc, char **argv, int *index, kafs_ln_opts_t *opts)
{
  const char *arg = argv[*index];
  if (strcmp(arg, "--") == 0)
  {
    ++(*index);
    return 3;
  }
  if (kafs_cli_is_help_arg(arg))
    return 1;
  if (arg[0] != '-' || strcmp(arg, "-") == 0)
    return 4;

  int rc = parse_ln_flag_option(arg, opts);
  if (rc == 0)
    return 0;

  rc = parse_ln_target_directory_option(argc, argv, index, arg, opts);
  if (rc >= 0)
    return rc;

  fprintf(stderr, "unknown option: %s\n", arg);
  return 2;
}

static int parse_ln_options(int argc, char **argv, kafs_ln_opts_t *opts, int *operand_index)
{
  if (!opts || !operand_index)
    return 2;

  memset(opts, 0, sizeof(*opts));
  int i = 2;
  while (i < argc)
  {
    int rc = parse_ln_option_arg(argc, argv, &i, opts);
    if (rc == 3)
      break;
    if (rc == 4)
      break;
    if (rc != 0)
      return rc;
    ++i;
  }

  *operand_index = i;
  return 0;
}

static int cmd_ln_dispatch_parse(int argc, char **argv, kafs_ln_opts_t *opts, int *operand_index)
{
  int parse_rc = parse_ln_options(argc, argv, opts, operand_index);
  if (parse_rc == 1)
  {
    usage_ln_cmd(argv[0]);
    return 0;
  }
  if (parse_rc != 0)
    usage_ln_cmd(argv[0]);
  return parse_rc;
}

static int cmd_ln_dispatch_target_dir(int argc, char **argv, int operand_index,
                                      const kafs_ln_opts_t *opts, const char **mnt_out,
                                      const char **src_out, const char **dst_out,
                                      char dst_buf[KAFS_IOCTL_PATH_MAX])
{
  int remaining = argc - operand_index;
  if (remaining == 1)
  {
    *src_out = argv[operand_index];
  }
  else if (remaining == 2)
  {
    *mnt_out = argv[operand_index];
    *src_out = argv[operand_index + 1];
  }
  else
  {
    return 2;
  }

  if (build_link_path(dst_buf, opts->target_dir, *src_out) != 0)
  {
    fprintf(stderr, "invalid target directory or source name\n");
    return 2;
  }

  *dst_out = dst_buf;
  return 0;
}

static int cmd_ln_dispatch_direct(int argc, char **argv, int operand_index, const char **mnt_out,
                                  const char **src_out, const char **dst_out)
{
  int remaining = argc - operand_index;
  if (remaining == 2)
  {
    *src_out = argv[operand_index];
    *dst_out = argv[operand_index + 1];
    return 0;
  }
  if (remaining == 3)
  {
    *mnt_out = argv[operand_index];
    *src_out = argv[operand_index + 1];
    *dst_out = argv[operand_index + 2];
    return 0;
  }
  return 2;
}

static int cmd_ln_dispatch_dir_destination(const char *mnt, const char *src, const char **dst_inout,
                                           const kafs_ln_opts_t *opts,
                                           char dst_buf[KAFS_IOCTL_PATH_MAX])
{
  if (opts->no_target_directory)
    return 0;

  int is_dir = path_ref_is_directory(mnt, *dst_inout, opts->no_deref);
  if (is_dir != 0 && is_dir != 1)
    return is_dir;
  if (is_dir == 0)
    return 0;

  if (build_link_path(dst_buf, *dst_inout, src) != 0)
  {
    fprintf(stderr, "invalid destination path\n");
    return 2;
  }

  *dst_inout = dst_buf;
  return 0;
}

static int cmd_ln_dispatch_exec(const char *mnt, const char *src, const char *dst,
                                const kafs_ln_opts_t *opts)
{
  if (opts->symbolic)
    return mnt ? cmd_symlink_with_opts(mnt, src, dst, opts)
               : cmd_symlink_auto_with_opts(src, dst, opts);

  return mnt ? cmd_ln_with_opts(mnt, src, dst, opts) : cmd_ln_auto_with_opts(src, dst, opts);
}

static int cmd_ln_dispatch(int argc, char **argv)
{
  kafs_ln_opts_t opts;
  int operand_index = 0;
  int parse_rc = cmd_ln_dispatch_parse(argc, argv, &opts, &operand_index);
  if (parse_rc != 0)
    return parse_rc;

  const char *mnt = NULL;
  const char *src = NULL;
  const char *dst = NULL;
  char dst_buf[KAFS_IOCTL_PATH_MAX];

  if (opts.target_dir)
  {
    int rc =
        cmd_ln_dispatch_target_dir(argc, argv, operand_index, &opts, &mnt, &src, &dst, dst_buf);
    if (rc != 0)
    {
      usage_ln_cmd(argv[0]);
      return rc;
    }
  }
  else
  {
    int rc = cmd_ln_dispatch_direct(argc, argv, operand_index, &mnt, &src, &dst);
    if (rc != 0)
    {
      usage_ln_cmd(argv[0]);
      return rc;
    }

    rc = cmd_ln_dispatch_dir_destination(mnt, src, &dst, &opts, dst_buf);
    if (rc != 0)
      return rc;
  }

  return cmd_ln_dispatch_exec(mnt, src, dst, &opts);
}

static int cmd_readlink(const char *mnt, const char *path)
{
  kafs_path_ref_t ref;
  int rc = resolve_path_ref(mnt, path, &ref);
  if (rc != 0)
    return rc;

  char buf[KAFS_IOCTL_PATH_MAX];
  ssize_t n = readlinkat(ref.dfd, ref.rel, buf, sizeof(buf) - 1);
  if (n < 0)
  {
    perror("readlinkat");
    close_path_ref(&ref);
    return 1;
  }
  buf[n] = '\0';
  printf("%s\n", buf);

  close_path_ref(&ref);
  return 0;
}

static int parse_octal_mode(const char *s, mode_t *out)
{
  if (!s || !*s || !out)
    return -EINVAL;
  char *end = NULL;
  errno = 0;
  long v = strtol(s, &end, 8);
  if (errno != 0 || !end || *end != '\0' || v < 0)
    return -EINVAL;
  *out = (mode_t)(v & 07777);
  return 0;
}

static int cmd_chmod(const char *mnt, const char *mode_str, const char *path)
{
  mode_t mode;
  if (parse_octal_mode(mode_str, &mode) != 0)
  {
    fprintf(stderr, "invalid mode\n");
    return 2;
  }

  kafs_path_ref_t ref;
  int rc = resolve_path_ref(mnt, path, &ref);
  if (rc != 0)
    return rc;

  if (fchmodat(ref.dfd, ref.rel, mode, 0) != 0)
  {
    perror("fchmodat");
    close_path_ref(&ref);
    return 1;
  }

  close_path_ref(&ref);
  return 0;
}

static int cmd_touch(const char *mnt, const char *path)
{
  kafs_path_ref_t ref;
  int rc = resolve_path_ref(mnt, path, &ref);
  if (rc != 0)
    return rc;

  int fd = openat(ref.dfd, ref.rel, O_CREAT | O_WRONLY, 0644);
  if (fd >= 0)
    close(fd);
  else if (errno != EISDIR)
  {
    perror("openat");
    close_path_ref(&ref);
    return 1;
  }

  if (utimensat(ref.dfd, ref.rel, NULL, 0) != 0)
  {
    perror("utimensat");
    close_path_ref(&ref);
    return 1;
  }

  close_path_ref(&ref);
  return 0;
}

int main(int argc, char **argv)
{
  if (try_subcommand_help(argc, argv) == 0)
    return 0;

  if (argc < 3)
  {
    usage(argv[0]);
    return 2;
  }

  if (strcmp(argv[1], "migrate") == 0)
    return kafsctl_handle_migrate_command(argc, argv);

  if (strcmp(argv[1], "fsstat") == 0 || strcmp(argv[1], "stats") == 0)
    return kafsctl_handle_fsstat_command(argc, argv);

  if (strcmp(argv[1], "hotplug") == 0)
    return kafsctl_handle_hotplug_command(argc, argv);

  if (strcmp(argv[1], "stat") == 0)
    return kafsctl_dispatch_path_cmd(argc, argv, cmd_stat);

  if (strcmp(argv[1], "cat") == 0)
    return kafsctl_dispatch_path_cmd(argc, argv, cmd_cat);

  if (strcmp(argv[1], "write") == 0)
    return kafsctl_dispatch_path_cmd(argc, argv, cmd_write);

  if (strcmp(argv[1], "cp") == 0)
  {
    if (argc == 4)
      return cmd_cp_auto(argv[2], argv[3], 0);
    if (argc == 5 && strcmp(argv[4], "--reflink") == 0)
      return cmd_cp_auto(argv[2], argv[3], 1);
    if (argc < 5)
    {
      usage(argv[0]);
      return 2;
    }
    int reflink = 0;
    for (int i = 5; i < argc; ++i)
    {
      if (strcmp(argv[i], "--reflink") == 0)
        reflink = 1;
      else
      {
        usage(argv[0]);
        return 2;
      }
    }
    return cmd_cp(argv[2], argv[3], argv[4], reflink);
  }

  if (strcmp(argv[1], "mv") == 0)
  {
    if (argc == 4)
      return cmd_mv_auto(argv[2], argv[3]);
    if (argc != 5)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_mv(argv[2], argv[3], argv[4]);
  }

  if (strcmp(argv[1], "rm") == 0)
    return kafsctl_dispatch_path_cmd(argc, argv, cmd_rm);

  if (strcmp(argv[1], "mkdir") == 0)
    return kafsctl_dispatch_path_cmd(argc, argv, cmd_mkdir);

  if (strcmp(argv[1], "rmdir") == 0)
    return kafsctl_dispatch_path_cmd(argc, argv, cmd_rmdir);

  if (strcmp(argv[1], "ln") == 0)
    return cmd_ln_dispatch(argc, argv);

  if (strcmp(argv[1], "symlink") == 0)
  {
    if (argc == 4)
      return cmd_symlink_auto(argv[2], argv[3]);
    if (argc != 5)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_symlink(argv[2], argv[3], argv[4]);
  }

  if (strcmp(argv[1], "rsync") == 0)
  {
    if (argc < 3)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_rsync(argc, argv);
  }

  if (strcmp(argv[1], "readlink") == 0)
    return kafsctl_dispatch_path_cmd(argc, argv, cmd_readlink);

  if (strcmp(argv[1], "chmod") == 0)
    return kafsctl_dispatch_chmod_cmd(argc, argv);

  if (strcmp(argv[1], "touch") == 0)
    return kafsctl_dispatch_path_cmd(argc, argv, cmd_touch);

  usage(argv[0]);
  return 2;
}
