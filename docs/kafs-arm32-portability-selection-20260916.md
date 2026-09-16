# KAFS ARM32 portability selection record

## Record

- Record ID: `KAFS-ARM32-PORTABILITY-20260916`
- Recorded: 2026-09-16T13:05:35+09:00
- Branch: `fix/arm32-portability`
- HEAD: `1c9b18650c9f6fa19791743a87e2562daa05a50a`
- Worktree before source edits: only `plans/arm32-portability.pert` was untracked
- Scoped plan: `plans/arm32-portability.pert`
- Tool: `perttool 0.11.0`
- Evidence freshness: the GitHub remote was fetched through the repository
  credential domain on 2026-09-16; `origin/master` remained `1c9b186`.

The repository-default `plans/current.pert` is not used for this selection. It
models the accepted production-cutover and physical-media qualification goal.
The user separately selected the repository-level ARM32 portability and Moxa
installation outcome on 2026-09-16, so this record names a scoped replacement.

## Accepted goal and current capability

The accepted end goal is a repository revision that builds warning-clean on the
observed Debian 9 ARMv7/GCC 6 target, passes proportional non-destructive target
qualification, and is installed and independently read back on `192.168.0.99`.
No task writes, formats, mounts, or tests the inserted SD card.

Directly observed current capabilities are:

- SSH access, root filesystem capacity, compiler/build prerequisites, and
  `/dev/fuse` are available on the target;
- FUSE 3.10.5 builds on the target in an isolated prefix;
- the complete KAFS diagnostic build links on ARMv7 when the GCC 6 attribute
  compatibility definition is preincluded and `int-to-pointer-cast` is reduced
  from error to warning;
- the repository source is not yet an install candidate because five source
  sites require that warning reduction, related explicit pointer-width casts
  remain, and configure accepts the incompatible installed FUSE 3.4.1.

## Plan changes and estimates

This scoped plan adds one serial capability path:

`ARM32_PORTABILITY_IMPLEMENTED -> ARM32_TARGET_QUALIFICATION -> MOXA_INSTALL -> MOXA_INSTALL_READBACK`

One primary stream is available. Estimates use points with an explicit planning
velocity of `8p/1d`; this relationship is not an elapsed-time promise.

- Source portability: `1p / 2p / 4p`, medium confidence. The observed compiler
  diagnostics are bounded, but same-cause range handling can add test work.
- Target qualification: `1p / 2p / 4p`, medium confidence. A complete target
  diagnostic build succeeded; clean rebuild and tests remain.
- Installation and readback: each `0.5p / 1p / 2p`, high confidence, assuming
  qualification passes. The target paths and staged FUSE source are known.

No external wait is included. A new target or newly discovered platform
contract would require plan refresh rather than consuming hidden estimate.

## Exact selection evidence

Command:

```sh
./scripts/pert-next-task.sh plans/arm32-portability.pert
```

Relevant exact output:

```text
OK plans/arm32-portability.pert project=KAFS_ARM32_PORTABILITY milestones=5 tasks=4 gates=0 resources=1 temporal=milestone_deadlines:0,task_not_before:0,task_deadlines:0
MAKESPAN 6.5p
VELOCITY FORECAST 0.813d
PRECEDENCE CRITICAL
TASKS ARM32_PORTABILITY_IMPLEMENTED, ARM32_TARGET_QUALIFICATION, MOXA_INSTALL, MOXA_INSTALL_READBACK
RESOURCE CRITICAL
TASKS ARM32_PORTABILITY_IMPLEMENTED, ARM32_TARGET_QUALIFICATION, MOXA_INSTALL, MOXA_INSTALL_READBACK
ACTIVE
-
RUNNABLE NOW
ARM32_PORTABILITY_IMPLEMENTED priority=0 expected=2.167p forecast=0.271d TF=0p precedence_critical=true schedule_critical=true owner=KAFS implementation stream resources=PRIMARY_STREAM=1
READY / WAITING RESOURCE
-
BLOCKED NOW
-
UPCOMING
ARM32_TARGET_QUALIFICATION priority=0 expected=2.167p forecast=0.271d TF=0p precedence_critical=true schedule_critical=true owner=KAFS implementation stream resources=PRIMARY_STREAM=1
MOXA_INSTALL priority=0 expected=1.083p forecast=0.135d TF=0p precedence_critical=true schedule_critical=true owner=KAFS deployment stream resources=PRIMARY_STREAM=1
MOXA_INSTALL_READBACK priority=0 expected=1.083p forecast=0.135d TF=0p precedence_critical=true schedule_critical=true owner=KAFS deployment stream resources=PRIMARY_STREAM=1
```

Decision: `SELECT ARM32_PORTABILITY_IMPLEMENTED`. It is the only `RUNNABLE NOW`
task, has zero total float, and closes the source capability required by every
later target qualification and installation node.

Installing the warning-suppressed diagnostic binaries first would create an
unqualified deployed state and later replacement work. Applying a target-only
patch would duplicate the source correction and leave the deployment revision
unreproducible. Neither ordering shortens the accepted qualified-deployment
path.

## Task Start Record: ARM32_PORTABILITY_IMPLEMENTED

### Checkout and task source

- Branch and HEAD: `fix/arm32-portability` at `1c9b186` from refreshed
  `origin/master`.
- Worktree: dedicated `.worktree/arm32-portability`; the primary worktree and
  its unrelated untracked `NUL` remain outside this worktree.
- Task source: user selection of repository-level correction, target compiler
  evidence collected on 2026-09-16, and the scoped PERT calculation above.

### Refreshed code and test evidence

- `configure.ac` checks unversioned `fuse3`, although production source uses
  `fuse_log`, `fuse_set_log_func`, and `struct fuse_operations.lseek`.
- Upstream libfuse symbol history places the log API in 3.7; its changelog
  introduces `FUSE_LSEEK` in 3.8. FUSE 3.8 is therefore the observed API floor.
- `src/kafs_config.h` owns forced portability definitions, while source uses
  the glibc-internal `__attribute_maybe_unused__` spelling at 24 sites.
- `src/kafs_hrl.c` performs the three reported wide-offset pointer additions;
  its entry helper contains the same explicit narrowing mechanism.
- `src/fsck_kafs.c` stores two numeric offsets as pointers before adding them to
  the mapped base. The shared runtime also narrows calculated offsets to
  `intptr_t` before pointer addition.
- Runtime contexts track `c_img_base`, `c_img_size`, and `c_mapsize`; mkfs and
  HRL test contexts do not consistently establish those fields.
- Existing HRL smoke tests and temporary-image helpers provide proportional
  behavior coverage; the target warning-clean build provides direct GCC 6 and
  ARM32 acceptance evidence.

### Assumptions

- Valid: this is a compiler and address-width portability correction; no
  on-disk format or FUSE-visible operation needs to change.
- Contradicted: correcting only the five emitted warnings would close the
  same-cause scope. Explicit pointer-width narrowing exists in active shared
  runtime and HRL helper code.
- Unknown until validation: whether an existing test relies on an incomplete
  mapping context. Such a failure must be repaired by establishing the mapping
  invariant, not by disabling bounds checks.

### States, variability, invariants, and boundaries

- Target variability: 32-bit `size_t`/`uintptr_t`, 64-bit `off_t`, GCC 6,
  glibc without the newer internal attribute macro, and libfuse version.
- Valid mapped-region state requires a non-null base and an offset plus length
  representable within the recorded mapping size.
- Invalid or unrepresentable regions fail before pointer construction or
  memory access.
- Existing image formats, metadata geometry, lock ranks/order, runtime IPC,
  FUSE operation semantics, and packaging entrypoints remain unchanged.

### Exit criteria

- a project-owned maybe-unused attribute replaces the libc-internal name;
- configure and supporting checks require `fuse3 >= 3.8.0`;
- active HRL, fsck, and shared-runtime image-offset conversions cannot silently
  truncate to pointer width;
- malformed HRL regions have negative range coverage;
- the narrow build/tests and repository formatting/lint checks pass locally;
- the subsequent PERT wave, not this task, owns warning-clean ARMv7 target
  qualification.

Non-goals are new filesystem features, format migration, lock-policy changes,
daemon or transport changes, installation, and any SD-card access.

Start decision: `PASS`. The boundary closes one coherent portability capability
selected by PERT and has direct native plus ARMv7 acceptance evidence.
