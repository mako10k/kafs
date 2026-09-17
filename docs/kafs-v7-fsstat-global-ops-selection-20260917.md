# v7 fsstat and global-operation selection — 2026-09-17

- Scope: owner request of 2026-09-17 after the interrupted KINGMAX run. Enable
  truthful v7 `fsstat`, audit global operations in small request paths, and
  assess localization alternatives if found. No card writes, deployment,
  durability-policy change, or broad optimizer implementation is selected.
- Checkout: `fix/arm32-portability` at `159af1097cfeb7e2d807a6919f9a24e3dc2f63b7`;
  same SHA read back from the remote branch. Pre-existing untracked test
  artifacts remain untouched. Plan: `plans/v7-fsstat-global-ops-20260917.pert`.
  Tool: `perttool 0.11.0`.
- Current capability: v7 `statfs` uses recovered free counters, but `fsstat`
  uses legacy superblock free counters and reported the real card as 100% used.
  The earlier controlled local append demonstrated size-dependent repeated
  metadata work; function-level cost split is not yet measured.
- Plan changes from the prior non-power card plan: the card run is stopped and
  is not an active predecessor. This bounded observability/assessment goal is
  separate from production-cutover qualification in `plans/current.pert`.
- Estimates are low confidence point-scale implementation/analysis effort,
  excluding external waits. One primary stream of capacity one is observed;
  no parallel implementation capacity is assumed.

`./scripts/pert-next-task.sh plans/v7-fsstat-global-ops-20260917.pert`
passed document check. The precedence makespan is `4p`; its critical path is
`V7_FSSTAT_REPAIR -> FSSTAT_RESULT_REQUIRED`. With one resource stream, the
reported resource makespan is `7.167p` and the resource-critical sequence is
`V7_FSSTAT_REPAIR -> GLOBAL_OPS_AUDIT -> LOCALIZATION_ASSESSMENT`.
`RUNNABLE NOW` and the recommended set contain only `V7_FSSTAT_REPAIR`;
`GLOBAL_OPS_AUDIT` is ready but waiting for the same resource; there is no
active or blocked task; `LOCALIZATION_ASSESSMENT` is upcoming.

Selection: `V7_FSSTAT_REPAIR` (PASS). It unlocks truthful v7 operational
capacity readback. Starting with the audit is a credible alternative and
would expose more optimization candidates sooner, but it delays the already
observed false-capacity correction. Starting with a write-path optimization
would create integrity and recovery qualification debt before the global
callsite inventory is complete. No nonzero priority or false dependency was
used to select the first task.

## Fsstat closeout and refreshed frontier

The v7 mount regression now checks both ioctl modes against `statvfs`, v7 HRL
capacity, and `kafsctl fsstat --json` recovered capacity/support disclosure.
`make -j2`, `./scripts/format.sh`, the direct
`tests/v7_inspection_mount_smoketest` run, and
`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` passed; the full suite reported
all 46 tests passed. This is local file-image evidence, not a Moxa deployment or
a renewed real-card test. `V7_FSSTAT_REPAIR` is therefore recorded `done` and
its destination milestone `reached`.

The refreshed `perttool` result has `GLOBAL_OPS_AUDIT` alone in `RUNNABLE NOW`
and the recommended set, with expected `1.167p`, zero float, and one primary
resource unit. `LOCALIZATION_ASSESSMENT` remains upcoming until that inventory
is recorded. Both tasks form the current precedence and resource critical
path (`3.167p` remaining point-scale estimate; no calendar forecast because
no velocity is encoded). The audit is selected next because it establishes the
actual operation set before comparing local replacements. Implementing a
validator bypass first would create unquantified crash-recovery and integrity
qualification debt.

## Global-operation audit closeout and next selection

`docs/kafs-v7-global-ops-audit-20260917.md` records source-backed image-wide,
group-wide, file-wide, observation-only, and persistence-barrier work with
separate measured and unknown cost claims. The audit was read-only for the
runtime and card. `GLOBAL_OPS_AUDIT` is now `done`, and its destination
milestone is `reached`.

The refreshed `perttool` output puts only `LOCALIZATION_ASSESSMENT` in the
recommended set and `RUNNABLE NOW` (`2p`, zero float; one primary resource
unit). It is the sole remaining critical task. The alternative of directly
editing one validator callsite is not selected: the inventory shows multiple
nested callsites, per-block retirement, and file-tree scans, so a one-site
bypass would not establish the requested performance outcome and could relax
crash/recovery checks. The selected assessment will compare bounded designs
without changing the existing mutation path.

## Scoped assessment closeout

The global-operation inventory and alternatives are recorded in
`docs/kafs-v7-global-ops-audit-20260917.md`. `LOCALIZATION_ASSESSMENT` and its
milestone are `done`/`reached`; the scoped finish milestone is reached by the
two completed result gates. The final `perttool` run passes document check,
reports `MAKESPAN 0p`, and has no `RUNNABLE NOW`, active, or blocked task.

`./scripts/static-checks.sh` passed format, lint, and complexity but exited 1
on clones. Inspection of `report/clone/src/jscpd-report.json` found 126 clone
pairs, all 126 involving the pre-existing untracked `kafs-0.4.0/` source
bundle; zero pairs remained when that bundle was excluded from the report
filter. The bundle was not removed or added to tracked files. No commit, push,
Moxa installation, or SD-card write is part of this closeout.
