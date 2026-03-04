#pragma once

#include <string.h>

typedef void (*kafs_usage_fn_t)(const char *prog);

// Parse shared CLI options used by front/back tools.
// Returns 1 when handled, 0 when not handled.
// When handled, exit_code >= 0 means caller should exit with that code.
static inline int kafs_cli_parse_uds_help(const char *arg, const char *next_arg,
                                          const char **uds_path, int *consume_next,
                                          int *exit_code, kafs_usage_fn_t usage,
                                          const char *prog)
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

  if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0)
  {
    usage(prog);
    if (exit_code)
      *exit_code = 0;
    return 1;
  }

  return 0;
}
