# Crash Diagnostics and Core Dump Guide

## Purpose

This document describes how KAFS emits crash diagnostics and how to collect core dumps for post-mortem analysis.

## What Is Enabled in KAFS

The following binaries install crash diagnostics at startup:

- `kafs`
- `kafs-back`

On fatal signals (`SIGSEGV`, `SIGABRT`, `SIGBUS`, `SIGILL`, `SIGFPE`), they do the following:

1. Print a fatal signal line to `stderr`.
2. Print a stack backtrace to `stderr` (Linux).
3. Re-raise the signal with default handler so OS-level core dump flow runs.

Additionally on Linux startup:

- Attempts to keep process dumpable (`PR_SET_DUMPABLE=1`).
- Tries to uncap `RLIMIT_CORE` when soft limit is zero and hard limit allows it.
- Writes `0x3f` to `/proc/self/coredump_filter` to keep richer mappings in core dumps.

## Quick Verification (Optional)

A minimal smoke run should show both of these:

- Stderr log lines like `caught fatal signal ...` and `stack backtrace ...`.
- A system core handling event (either a core file or a core-handler capture).

Observed in this environment:

- `stderr` backtrace output: confirmed.
- `core_pattern`: `|/wsl-capture-crash %t %E %p %s`.
- Result: no `core*` file in working directory because core is piped to external handler.

## How to Collect Core Dumps

First inspect `/proc/sys/kernel/core_pattern`.

### Case A: Direct core files (no leading `|`)

- Core files are written according to `core_pattern` path template.
- Ensure shell soft limit is non-zero (`ulimit -c unlimited`).
- Collect the core path and analyze with `gdb`.

### Case B: Piped handler (`core_pattern` starts with `|`)

- Core is forwarded to a handler process, so `./core` is typically not created.
- Use platform handler tooling/logs to retrieve the dump.
- On systemd environments this is usually `coredumpctl`.
- On WSL-like environments, vendor-specific crash capture tools may own the dump.

## WSL Practical Retrieval Steps

This workspace currently reports:

- `core_pattern`: `|/wsl-capture-crash %t %E %p %s`

In this mode, WSL forwards the crash to `CaptureCrash`, and dump files are typically written on the Windows side. In this environment, dumps are present under:

- `/mnt/c/Users/<windows-user>/AppData/Local/Temp/wsl-crashes/`

Example filename pattern:

- `wsl-crash-<ts>-<pid>-_<path>_<exe>-<signal>.dmp`

Suggested workflow:

1. Reproduce the crash and keep `stderr` output.
2. Confirm capture in journal logs (look for `CaptureCrash`):
   - `journalctl --no-pager -n 200 | rg -i "CaptureCrash|wsl-capture|signal"`
3. Find newest WSL crash dump:
   - `ls -lt /mnt/c/Users/$USER/AppData/Local/Temp/wsl-crashes/*.dmp | head`
4. Narrow by executable name if needed:
   - `ls -lt /mnt/c/Users/$USER/AppData/Local/Temp/wsl-crashes/*.dmp | rg "kafs|kafs-back|kafs-front"`

If `$USER` does not match your Windows account name, replace it with the actual Windows profile directory name.

## Recommended Incident Bundle

When reporting a crash, include all of:

- Binary name and build revision.
- Full `stderr` crash output (signal + backtrace).
- `core_pattern` value.
- Core dump reference (file path or handler-side identifier).
- Reproduction steps and relevant input image/workload.
- For WSL: matching `CaptureCrash` journal lines and the selected `.dmp` filename.

## Notes

- If permissions prevent changing core limits, the diagnostics still emit stack traces to `stderr`.
- `backtrace_symbols_fd` output quality depends on symbol availability (debug symbols improve readability).
