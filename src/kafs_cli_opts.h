#pragma once

#include "kafs_config.h"
#include <string.h>

typedef void (*kafs_usage_fn_t)(const char *prog);

static inline int kafs_cli_is_help_arg(const char *arg)
{
  return arg && (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0);
}

// Returns 0 when explicit help was handled and the caller should exit successfully.
// Returns -1 when no help flag was found.
static inline int kafs_cli_exit_if_help(int argc, char **argv, kafs_usage_fn_t usage,
                                        const char *prog)
{
  for (int i = 1; i < argc; ++i)
  {
    if (!kafs_cli_is_help_arg(argv[i]))
      continue;
    usage(prog);
    return 0;
  }
  return -1;
}

// Parse shared CLI options used by front/back tools.
// Returns 1 when handled, 0 when not handled.
// When handled, exit_code >= 0 means caller should exit with that code.
static inline int kafs_cli_parse_uds_help(const char *arg, const char *next_arg,
                                          const char **uds_path, int *consume_next, int *exit_code,
                                          kafs_usage_fn_t usage, const char *prog)
{
  if (consume_next)
    *consume_next = 0;
  if (exit_code)
    *exit_code = -1;

  if (strcmp(arg, "--uds") == 0)
  {
    if (!next_arg)
    {
      usage(prog);
      if (exit_code)
        *exit_code = 2;
      return 1;
    }
    *uds_path = next_arg;
    if (consume_next)
      *consume_next = 1;
    return 1;
  }

  if (kafs_cli_is_help_arg(arg))
  {
    usage(prog);
    if (exit_code)
      *exit_code = 0;
    return 1;
  }

  return 0;
}

// Parse argv for tools that only support --uds and --help/-h.
// Returns >=0 when caller should exit with that code, otherwise -1.
static inline int kafs_cli_parse_uds_help_loop(int argc, char **argv, const char **uds_path,
                                               kafs_usage_fn_t usage, const char *prog)
{
  for (int i = 1; i < argc; ++i)
  {
    int consume_next = 0;
    int exit_code = -1;
    int handled = kafs_cli_parse_uds_help(argv[i], (i + 1 < argc) ? argv[i + 1] : NULL, uds_path,
                                          &consume_next, &exit_code, usage, prog);
    if (!handled)
      continue;
    if (exit_code >= 0)
      return exit_code;
    if (consume_next)
      ++i;
  }
  return -1;
}
