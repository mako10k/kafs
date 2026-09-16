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

## Resumed selection: ARM32 target qualification

- Recorded: 2026-09-16 (Asia/Tokyo), after local source validation.
- Branch and source revision: `fix/arm32-portability` at `0f77f2e`.
- Completed edge: `ARM32_PORTABILITY_IMPLEMENTED`; milestone
  `PORTABLE_SOURCE_READY` means locally validated source ready for ARMv7
  qualification, not a claim that GCC 6 already built it.
- Current capability: native build and focused HRL tests passed; formatting and
  lint passed; the distribution includes both new portability headers. The
  first native 46-test suite had one v5 remount readback mismatch. The isolated
  rerun and the second full suite passed, with all 46 tests passing in the
  second run. The first mismatch remains an unresolved qualification note.
- Target prerequisites read back: Debian 9 ARMv7/GCC 6.3, 5.6 GB free on the
  root filesystem, system FUSE 3.4.1, private FUSE 3.10.5, and an absent
  `/var/tmp/kafs-arm32-0f77f2e` staging path.

The refreshed `./scripts/pert-next-task.sh plans/arm32-portability.pert`
passed `document check`, `dag analyze --schedule both`, and `dag next`.
The remaining precedence and resource critical path is
`ARM32_TARGET_QUALIFICATION -> MOXA_INSTALL -> MOXA_INSTALL_READBACK`, with
expected remaining effort `4.333p`. `ARM32_TARGET_QUALIFICATION` is the sole
`RUNNABLE NOW` task at `2.167p` expected effort and `0p` float; the other two
are upcoming. Points and the `8p/1d` velocity are planning estimates, not an
elapsed-time or delivery promise.

Decision: `SELECT ARM32_TARGET_QUALIFICATION`. The alternative of installing
the warning-suppressed diagnostic build would leave the accepted source
revision unqualified and require replacement. There is no other runnable
critical-path edge. This wave transfers the exact candidate to isolated
target staging, verifies the FUSE version floor, builds warning-clean with
GCC 6 without diagnostic suppressions, and runs bounded temporary-image
tests. Installation, persistent FUSE placement, and SD-card access are not
part of this wave. The installation path was separately chosen by the user:
preserve shared FUSE 3.4.1 and place FUSE 3.10.5 with KAFS in a dedicated
stable area, exposing KAFS commands only.

## Target build interruption and revised selection

The `0f77f2e` target source distribution was transferred to
`/var/tmp/kafs-arm32-0f77f2e`; all 171 transferred-file manifest hashes
matched the local distribution. Target `configure` rejected system FUSE
3.4.1 and accepted private FUSE 3.10.5. The subsequent GCC 6 build stopped
before portability compilation because `kafs_crash_diag.h` and
`kafs_hotplug.h` were absent from the distribution. Comparing tracked
headers with the distribution found ten omitted headers, all missing from
`src/Makefile.am`'s `noinst_HEADERS`. The transfer was not the cause. The
source-ready milestone and target selection above were withdrawn, and PERT
again selected the source edge; no KAFS binary was installed.

Revision `7942fd8` adds those ten tracked headers to the distribution list.
The regenerated distribution contains all 49 tracked `src` headers. It
configured and built out of tree with `-Wall -Werror`, and its HRL smoke
target built and ran. The old target candidate remains as diagnostic evidence
and must not be installed. The refreshed PERT passes all three commands and
again places `ARM32_TARGET_QUALIFICATION` alone in `RUNNABLE NOW` with `0p`
float; install and readback remain upcoming. `SELECT
ARM32_TARGET_QUALIFICATION` now applies to exact source revision `7942fd8`,
not to the superseded `0f77f2e` distribution. The first native v5 remount
mismatch remains unresolved, with isolated and full-suite reruns passing.

## ARMv7 qualification evidence and open gate

The exact `7942fd8` distribution was transferred to
`/var/tmp/kafs-arm32-7942fd8`; its full file manifest matched the local
distribution. In `build-fuse310`, configure rejected shared FUSE 3.4.1 and
accepted private FUSE 3.10.5. The Debian 9 ARMv7/GCC 6.3 build completed
with `-Wall -Werror`, without the diagnostic preinclude or warning downgrade.
The target HRL smoke test and v5 mixed-tail mount smoke test passed using the
private library. ELF readback showed ARM32 binaries. This is build and bounded
test evidence, not installed-state evidence.

The first v7 inspection test ran under `/tmp` and stopped after
`format_and_seed PASS`. Its first copied image was truncated and `/tmp` was a
full 128 MiB tmpfs. An audited diagnosis identified scratch-capacity
exhaustion as the immediate copy failure. The same test was rerun with
`TMPDIR=/var/tmp` and private FUSE. It passed the inspection mount,
controlled-write normal matrix, three indirect recovery matrices, and
directory-transition normal matrix, among other printed cases. It then
exited 1 at `checkpoint_copy` / `direct-append-interior` in the directory
recovery matrix. The failing `v7-create-recovery-2-6.img` copy is 63,082,496
bytes; its source is 134,217,728 bytes. Target rootfs available space fell
to effectively zero at that failure. The test copies before fault injection,
so rootfs exhaustion during copy preparation is **inferred**, not an
errno-confirmed cause. The v7 recovery behavior for that case was not
exercised by this failed copy and remains unqualified.

The rootfs was recovered to 4.2 GiB available by deleting only explicitly
identified, completed temporary case copies. The failed `2-6` image, its
source, the original image, and logs remain in
`/var/tmp/kafs-v7-inspection-mount-21469-3efYlz`. Removed copies can be
regenerated but not recovered as the same files. No removable SD device was
read or written, and no permanent KAFS/FUSE installation occurred. The
capacity incident exposed a target-fit gap in the test procedure: checking
rootfs free space alone did not budget the cumulative full-image copies.

`ARM32_TARGET_QUALIFICATION` remains open. Before a rerun, define a bounded
test-storage strategy that retains the failed-case evidence and leaves enough
rootfs headroom; do not simply rerun the same full matrix. The next action is
to reconcile that strategy with the accepted non-destructive qualification
contract. The refreshed PERT passed `document check`, `dag analyze --schedule
both`, and `dag next`; it still lists `ARM32_TARGET_QUALIFICATION` alone in
`RUNNABLE NOW`, with install and readback upcoming. Only a passing target
qualification can select `MOXA_INSTALL`.

## Storage-aware qualification candidate

The first and second target interruptions share one test-side mechanism:
`copy_image` wrote all-zero chunks as physical data. On the target a
134,217,728-byte source image occupied about 2.7 MiB, while each old copied
fault image occupied about 128 MiB. The 128 MiB `/tmp` could not hold the
first copy; the 6.7 GiB root filesystem could not hold the accumulated full
copies in the later matrix. `KAFS_TEST_KEEP_WORKDIR=1` preserved files after
exit but did not cause the within-run accumulation. The source cause is the
test copy implementation; inadequate target scratch budgeting and generic
copy-failure messages were escape/detection causes, not the source cause.
This classification was audited with command-line LLMThink.

WIP revision `f1f126c` changes only the v7 inspection test: all-zero copy
chunks become sparse holes, destination logical length is set explicitly,
copy errors identify the source, destination, and error code, and a pre-matrix
test verifies copied size and digest. Formatting, warning-clean native test
build, and the complete native v7 inspection test passed. A native
134,217,728-byte copied fault image occupied under 1 MiB after the change.
This is not yet ARMv7 qualification: the exact new revision must be staged,
built, and retested on the target with private FUSE. If that passes, update
the PERT edge and only then select the installation wave. No SD device or
shared FUSE installation is in this qualification step.

The first target `make -j2` attempt for `f1f126c` stopped while GCC wrote
`/tmp/cc*.s`: `/tmp` still had only 752 KiB free because the first failed
v7 test images were retained there. This is a scratch-space failure, not a
source compiler diagnostic. The exact old test directory was moved to
`/var/tmp/kafs-v7-inspection-mount-initial-6886-jyoWTz`; both image hashes
matched before and after, and `/tmp` then had 126 MiB free. A retry of the
same build with `TMPDIR=/var/tmp` passed under GCC 6.3 and `-Wall -Werror`.
The exact `f1f126c` distribution and test-source hashes matched on the target;
target HRL and v5 mixed-tail tests passed. The full target v7 inspection test
is running with `TMPDIR=/var/tmp`; its image-copy integrity check passed, but
the matrix has not yet completed. Future target build and test invocations
must use the verified scratch path; rootfs and `/tmp` are different capacity
domains.

## ARMv7 qualification closeout and installation selection

The exact `f1f126c` distribution and test-source hashes matched after transfer.
With private FUSE 3.10.5, Debian 9 ARMv7/GCC 6.3 completed the default
`-Wall -Werror` build. Target HRL and v5 mixed-tail reruns exited zero. The
full v7 inspection test also exited zero, including `image_copy_integrity`,
the previously interrupted `directory_transition_recovery_matrix`, degraded
inspection, and unpaired fail-closed cases. Its retained workdir is
`/var/tmp/kafs-v7-inspection-mount-29649-E6IGC6`; rootfs had 3.9 GiB free
after completion. The historical first native v5 mismatch remains noted, but
isolated and later full native reruns and the target v5 rerun passed.

`ARM32_TARGET_QUALIFICATION` and `TARGET_BUILD_QUALIFIED` are now done/reached
in `plans/arm32-portability.pert`. The refreshed
`./scripts/pert-next-task.sh plans/arm32-portability.pert` passed document
check, both schedule analyses, and `dag next` at HEAD `f1f126c`. The remaining
critical path is `MOXA_INSTALL -> MOXA_INSTALL_READBACK`, expected `2.167p`;
both have `0p` float. `MOXA_INSTALL` is the sole `RUNNABLE NOW` task at
`1.083p`, and readback is upcoming. The points and velocity are estimates,
not elapsed time or a calendar commitment. `SELECT MOXA_INSTALL`: it unlocks
the installed-state milestone, whereas another identical qualification run
would defer user access without resolving a currently identified uncertainty.

Start decision: `PASS` for a dedicated, side-by-side `/opt/kafs` installation
of FUSE 3.10.5 and qualified KAFS revision `f1f126c`. Shared FUSE 3.4.1,
`fusermount3`, service/udev configuration, and all SD-device contents are
non-goals. Before installation, `/opt/kafs` and all proposed public KAFS
command names were absent. The FUSE 3.10.5 library-only build (`utils=false`)
was staged and its manifest contained only library, headers, and pkg-config
files. The old relocated Meson build could not regenerate because it retained
absolute `/tmp` paths; a new build from the intact `/var/tmp` source passed.
The dedicated FUSE library was installed at `/opt/kafs/fuse3-3.10.5`; readback
showed private `pkg-config` version 3.10.5 and unchanged shared version 3.4.1.
KAFS final-prefix build and installation were still open at that record update.

## Installation and readback closeout

The final-prefix target build from the exact `f1f126c` distribution passed
with GCC 6.3 and `-Wall -Werror`. Its staged Automake install contained 12
KAFS executables, `mount.kafs`, and documentation/completion files solely
under `/opt/kafs/releases/f1f126c`. The built `kafs-v7` had an RPATH to
`/opt/kafs/fuse3-3.10.5/lib`, and `ldd` resolved the private library there.
The installation wrote that versioned release and 13 previously absent public
KAFS symlinks in `/usr/local/bin` and `/usr/local/sbin`; no FUSE command or
global service/udev path was published.

`MOXA_INSTALL` and `MOXA_KAFS_INSTALLED` were marked done/reached, and the
refreshed PERT check/analyze/next selected `MOXA_INSTALL_READBACK` alone in
`RUNNABLE NOW` at `0p` float. Formal readback then verified each public link,
ELF32/ARM header, no missing dynamic libraries, and byte equality of all 12
installed executables and `mount.kafs` to the final build output. The 11
FUSE-dependent executables resolve `/opt/kafs/fuse3-3.10.5/lib/libfuse3.so.3`;
the retired `kafs-v6` placeholder has no FUSE dependency. Public `kafs` and
`kafs-v7` help commands exited zero without `LD_LIBRARY_PATH`. Private
pkg-config reported FUSE 3.10.5; shared pkg-config and `fusermount3` still
reported 3.4.1. Rootfs had 3.9 GiB free. `lsblk` showed removable `/dev/sda`
at 3.8 GiB with no mountpoint; no command in this deployment read, wrote,
formatted, or mounted it. These checks establish installed command readiness,
not real-SD functional qualification or bitwise proof of unchanged SD data.

`MOXA_INSTALL_READBACK` and `MOXA_KAFS_VERIFIED` were marked done/reached.
The final `./scripts/pert-next-task.sh plans/arm32-portability.pert` passed
all three stages and reported no active, runnable, blocked, or upcoming node;
the scoped install goal is complete. The remaining product step, if wanted,
is a separately scoped SD-media qualification with exact-device and destructive
approval. No GitHub push or release was performed in this continuation.
