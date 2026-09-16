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

This is interrupted work, not a completed portability change, deployment
candidate, or installable release.

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

These changes have not received the repository's reviewed-scope review and
must remain under a `WIP:` commit until that review and the remaining
validation are complete.

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

Not yet run:

- `./scripts/lint.sh`, `./scripts/clones.sh`, and
  `./scripts/static-checks.sh`;
- full `make check -j2`;
- review of the entire WIP diff;
- clean ARMv7/GCC 6 build without the diagnostic preinclude and warning
  downgrade;
- target-side non-destructive tests;
- install or installed-state readback.

## Required restart sequence

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

Realized value for the intended target user remains zero: KAFS is not installed
on the Moxa host. The evidenced future-value contribution is a locally
buildable repository WIP, a bounded target environment, and an explicit path
from source review through ARMv7 qualification to install and readback. The
capability becomes usable only after those named gates pass.
