#ifndef KAFS_CRASH_DIAG_H
#define KAFS_CRASH_DIAG_H

#include "kafs_config.h"
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef __linux__
#include <execinfo.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#endif

static const char *kafs_crash_diag_prog_name = "kafs";

static void kafs_crash_diag_handler(int sig)
{
  const char *name = strsignal(sig);
  fprintf(stderr, "%s: caught fatal signal %d (%s)\n", kafs_crash_diag_prog_name, sig,
          name ? name : "?");
#ifdef __linux__
  void *bt[64];
  int n = backtrace(bt, (int)(sizeof(bt) / sizeof(bt[0])));
  if (n > 0)
  {
    fprintf(stderr, "%s: stack backtrace (%d frames):\n", kafs_crash_diag_prog_name, n);
    backtrace_symbols_fd(bt, n, STDERR_FILENO);
  }
#endif

  signal(sig, SIG_DFL);
  raise(sig);
}

static void kafs_crash_diag_prepare_core_dump(const char *prog_name)
{
#ifdef __linux__
  struct rlimit lim;
  if (getrlimit(RLIMIT_CORE, &lim) == 0)
  {
    if (lim.rlim_cur == 0 && lim.rlim_max > 0)
      (void)setrlimit(RLIMIT_CORE, &lim);
  }

  (void)prctl(PR_SET_DUMPABLE, 1, 0, 0, 0);

  FILE *f = fopen("/proc/self/coredump_filter", "w");
  if (f)
  {
    /* Include anonymous/private/shared mappings for richer post-mortem dumps. */
    (void)fputs("0x3f\n", f);
    (void)fclose(f);
  }
#else
  (void)prog_name;
#endif
}

static void kafs_crash_diag_install(const char *prog_name)
{
  if (prog_name && *prog_name)
    kafs_crash_diag_prog_name = prog_name;

  kafs_crash_diag_prepare_core_dump(prog_name);

  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = kafs_crash_diag_handler;
  sigemptyset(&sa.sa_mask);

  (void)sigaction(SIGSEGV, &sa, NULL);
  (void)sigaction(SIGABRT, &sa, NULL);
  (void)sigaction(SIGBUS, &sa, NULL);
  (void)sigaction(SIGILL, &sa, NULL);
  (void)sigaction(SIGFPE, &sa, NULL);
}

#endif
