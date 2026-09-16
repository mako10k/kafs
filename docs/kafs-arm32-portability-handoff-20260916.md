# KAFS ARM32 portability WIP handoff

## Preservation state

- Date: 2026-09-16 (Asia/Tokyo)
- Branch: `fix/arm32-portability`
- Base: `origin/master` at `1c9b18650c9f6fa19791743a87e2562daa05a50a`
- WIP revision: the commit containing this handoff; resolve with
  `git rev-parse HEAD` after checkout
- Dedicated worktree used during development:
  `/home/katsumata-m/kafs/.worktree/arm32-portability`
- Original worktree and its unrelated untracked `NUL` were not modified.
- No SD-card device was read, formatted, mounted, or tested.

The preservation state and original restart instructions below describe the
initial shutdown handoff. They are superseded by the continuation status that
follows; this remains WIP repository history, not a
published release.

## 2026-09-16 continuation status

The installed source candidate is WIP revision `f1f126c` on
`fix/arm32-portability`; the scoped PERT and selection record have been
refreshed for this continuation. Its product-code predecessor `7942fd8`
built warning-clean with GCC 6.3 on
`moxa@192.168.0.99` using the private FUSE 3.10.5 library. Target HRL and
v5 mixed-tail mount tests passed. A full v7 inspection test advanced through
many mount, controlled-write, and recovery cases, then exited 1 as retained
temporary-image copies filled the target root filesystem. The late failed
copy and logs are
retained under `/var/tmp/kafs-v7-inspection-mount-21469-3efYlz`; the target
rootfs was read back with 4.2 GiB available after exact temporary-copy
cleanup. The capacity explanation was inferred because the old copy errno was
not logged. The replacement `f1f126c` test later completed on ARMv7 with
3.9 GiB rootfs free, including the formerly interrupted directory-recovery
case. Shared FUSE 3.4.1 remains unchanged, and no SD device was accessed.

The user chose side-by-side stable installation: keep shared FUSE 3.4.1,
place private FUSE 3.10.5 and qualified KAFS under a dedicated `/opt/kafs`
layout, and expose only KAFS commands. `/opt/kafs` and the proposed public
KAFS command paths were absent on readback. Do not treat this choice as a
waiver of target qualification. The prior storage-boundedness problem is
addressed by the new test-local candidate below, not by rerunning the old
test with only `TMPDIR=/var/tmp` changed.

Revision `f1f126c` changes only the v7 inspection test's copy helper to
preserve zero regions as sparse holes, adds copy size/digest verification, and
prints copy errors with their source, destination, and return code. The
complete native v7 test passed. Its source distribution was hash-verified on
ARMv7 and built there with GCC 6.3, `-Wall -Werror`, and private FUSE 3.10.5;
target HRL and v5 mixed-tail tests passed. The full target v7 test exited zero
with `TMPDIR=/var/tmp`; image-copy integrity and directory-transition recovery
passed. The target-qualification PERT edge is closed. The private FUSE 3.10.5
library is installed under `/opt/kafs/fuse3-3.10.5`, and KAFS under
`/opt/kafs/releases/f1f126c`, with 13 public KAFS command links. The final
GCC 6.3 build passed; all 12 installed ARM32 executables match their build
outputs byte-for-byte. The 11 FUSE-dependent commands resolve the private
library without an environment override; `kafs-v6` is a FUSE-independent
retirement placeholder. `kafs` and `kafs-v7` help commands executed from the
public paths. Shared FUSE and `fusermount3` remain 3.4.1. Removable `/dev/sda`
is present but unmounted and was not read or written. The final PERT readback
node and goal milestone are closed; no SD-media qualification was performed.
The first target build attempt for `f1f126c` failed because GCC used the
still-full `/tmp`; the old first-failure workdir was moved intact to
`/var/tmp/kafs-v7-inspection-mount-initial-6886-jyoWTz`, with both file
hashes matching and `/tmp` free space restored to 126 MiB. The build retry
used `TMPDIR=/var/tmp` and passed. The old relocated FUSE Meson build also
failed to regenerate after its prefix changed because it retained absolute
`/tmp` paths. A new build from the intact `/var/tmp` source passed, and only
its library-only manifest was installed under the private prefix.

## End-of-day handoff, 2026-09-16

The scoped ARM32 portability, target qualification, side-by-side Moxa install,
and installed-state readback are complete as recorded above and in
`docs/kafs-arm32-portability-selection-20260916.md`. The installed KAFS source
is exact WIP revision `f1f126c`; the repository branch is
`fix/arm32-portability`. The final PERT check/analyze/next reported no active,
runnable, blocked, or upcoming node for this scoped goal. This is not a
published release, PR-ready history, or SD-card qualification.

Closeout inspection found no active KAFS test/build process and no KAFS or
`/dev/sda` mount. On the Moxa host `/tmp` had 126 MiB free and the root
filesystem had 3.9 GiB free. Installed files under `/opt/kafs`, the 13 public
KAFS command symlinks, and the retained target test/source evidence were left
in place for immediate resumption. In particular, the passing v7 workdir is
`/var/tmp/kafs-v7-inspection-mount-29649-E6IGC6`; the earlier failed-case
evidence is `/var/tmp/kafs-v7-inspection-mount-21469-3efYlz`; the qualified
source/build is `/var/tmp/kafs-arm32-f1f126c`. No cleanup deletion was needed
or performed. The local worktree also retains the untracked `kafs-0.4.0/`
distribution directory and generated `tests/v5_v7_import_smoketest`,
`tests/v7_block_tree_smoketest`, `tests/v7_checkpoint_publication_smoketest`,
`tests/v7_fuse_write_smoketest`, `tests/v7_inspection_mount_smoketest`,
`tests/v7_journal_replay_smoketest`, `tests/v7_locks_smoketest`,
`tests/v7_mutation_routing_smoketest`, and `tests/v7_recovery_diagnostic`
binaries. They are not staged; do not discard them as a side effect of
resuming or preparing a PR.

Tomorrow, first inspect `worktimectl agent`, the branch/HEAD/worktree state,
and the remote SHA of `origin/fix/arm32-portability`; read this handoff and the
selection record before choosing more work. Recheck Moxa with source-bound SSH
(`ssh -b 192.168.0.110 moxa@192.168.0.99`) if installed state matters; do
not infer current runtime state from this dated record. Decide separately
whether to perform a reviewed-scope finalization/PR or a real-SD qualification.
The latter needs an exact-device, test-boundary, and destructive-write decision;
the removable `/dev/sda` was not used today. A historical first native v5
remount mismatch remains unexplained despite isolated and subsequent full
reruns and target v5 testing passing. Do not silently treat that uncertainty as
an SD-media result or as proof of a product defect.

The user requested a WIP commit and push for this closeout. Verify the exact
remote branch revision independently after that push; this handoff intentionally
does not pre-claim a remote SHA before writeback.

## Accepted outcome and boundary

The user selected a repository-level fix that builds warning-clean on the
observed Debian 9 ARMv7/GCC 6 target, followed by non-destructive target
qualification, installation on `192.168.0.99`, and independent readback.
The scoped critical path is recorded in `plans/arm32-portability.pert`; the
selection and Task Start Record are in
`docs/kafs-arm32-portability-selection-20260916.md`.

Explicit non-goals for this WIP are SD-card access, image-format changes,
filesystem feature changes, lock-order changes, deployment, and installation.

## Target observations already established

- Target: `moxa@192.168.0.99`, Debian 9, ARMv7, GCC 6, Linux 4.4.
- From WSL, bind SSH to the known working source address:
  `ssh -b 192.168.0.110 moxa@192.168.0.99`.
- The root filesystem had sufficient capacity and `/dev/fuse` was present.
- System-installed FUSE is 3.4.1, which is below the API floor used by KAFS.
- A private FUSE 3.10.5 build exists at
  `/var/tmp/kafs-install.YbhlhP/fuse310-prefix`.
- Diagnostic KAFS sources and build artifacts exist under
  `/var/tmp/kafs-install.YbhlhP/kafs-candidate`.
- The diagnostic target build needed a compatibility preinclude for
  `__attribute_maybe_unused__` and a warning downgrade for five pointer-width
  conversions. Those artifacts were not installed and must not be promoted.

The exact device or policy responsible for the default SSH source-address
failure was not established. The source-bound route is the verified working
access method, not a root-cause claim.

## Changes present in this WIP

- Replaces the glibc-internal `__attribute_maybe_unused__` spelling with the
  project-owned `KAFS_MAYBE_UNUSED` macro.
- Changes configure, bootstrap checking, lint checking, and README dependency
  text to require libfuse3 3.8.0 or newer.
- Adds checked integer, alignment, and mapped-region helpers in
  `src/kafs_portability.h`.
- Adds one checked v4/v5 map-layout calculation in
  `src/kafs_legacy_map_layout.h` and routes runtime and fsck mappings through
  it instead of storing numeric offsets in pointers or narrowing to
  `intptr_t`.
- Range-checks HRL index and entry regions before constructing pointers.
- Establishes `c_img_base`, `c_img_size`, and `c_mapsize` in mkfs and HRL test
  contexts.
- Adds an HRL smoke assertion that an entry region outside the mapping is
  rejected before access.

At the initial interruption, these changes had not received the repository's
reviewed-scope review and remained under a `WIP:` commit. The current
continuation and validation status is recorded above and in the selection
record; do not infer PR readiness from the target installation.

## Validation completed before interruption

Passed:

```sh
autoreconf -fi
build_dir=$(mktemp -d /tmp/kafs-arm32-wip-build.XXXXXX)
cd "$build_dir"
/home/katsumata-m/kafs/.worktree/arm32-portability/configure
make -j2
./tests/hrl_smoketest
```

The out-of-tree native build completed with the repository's `-Wall -Werror`
flags. The direct `hrl_smoketest` execution returned zero.

`./scripts/format.sh` initially found formatting differences in seven changed
source files. Those files were formatted with the repository configuration and
the check then reported `Formatting OK.` The post-format incremental rebuild
was interrupted for immediate shutdown, so the earlier build result has not
been reconfirmed against the formatted snapshot.

Attempted but intentionally interrupted:

```sh
make -C /tmp/kafs-arm32-wip-build.Or8hJD/tests check TESTS=hrl_smoketest
```

Automake first began building every `check_PROGRAMS` target, so this command was
interrupted to prioritize the requested handoff. It is not evidence of a test
failure. The already-built `hrl_smoketest` was then run directly and passed.

Not yet run at the initial interruption (superseded by later evidence):

- `./scripts/lint.sh`, `./scripts/clones.sh`, and
  `./scripts/static-checks.sh`;
- full `make check -j2`;
- review of the entire WIP diff;
- clean ARMv7/GCC 6 build without the diagnostic preinclude and warning
  downgrade;
- target-side non-destructive tests;
- install or installed-state readback.

## Original restart sequence (historical)

1. Fetch and check out `fix/arm32-portability`; confirm the remote WIP SHA and
   reread this handoff plus the selection record.
2. Inspect the complete WIP diff, especially overflow handling in
   `kafs_legacy_map_layout_compute`, HRL disabled/partially configured states,
   and cleanup behavior after mapping errors.
3. Run formatting and the narrow native HRL tests, then the full native build,
   `make check -j2`, lint, clone, and static gates. Correct any failure without
   treating this WIP as an accepted design.
4. On `192.168.0.99`, verify that configure rejects system FUSE 3.4.1 and
   accepts the isolated FUSE 3.10.5 prefix.
5. Copy or check out the exact reviewed revision, then build warning-clean with
   GCC 6 without `-Wno-error=int-to-pointer-cast` and without the compatibility
   preinclude. Run only non-destructive target qualification.
6. Refresh the scoped PERT and selection record. Installation may proceed only
   after the target qualification node is closed; independently read back
   installed versions, architecture, linkage, commands, and unchanged SD
   device state.

Do not reuse or install the earlier warning-suppressed diagnostic KAFS
binaries. Do not access `/dev/sda` or another removable device without a new,
explicitly scoped instruction.

## User-value state

The Moxa operator can now invoke the installed KAFS commands from the public
paths; previously KAFS was not installed on that host. This is realized
command-availability value, evidenced by installed-link, byte-identity,
linkage, and help-command readback. It does not establish use with the inserted
SD card: that removable device remains unmounted and untested. Repository
history is still WIP and was not pushed or released in this continuation.
