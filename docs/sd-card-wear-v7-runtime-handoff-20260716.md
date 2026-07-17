# KAFS format v7 runtime handoff 2026-07-16

## Scope

This handoff covers the format v7 runtime foundation through
`SDW-V7RT-T16 v7-owned runtime mutation/admission policy`. It is intended to
make the next runtime mutation/admission slice resumable from another host
without reopening the accepted wear-leveling and fault-tolerance decisions.

Repository checkpoint before this handoff WIP commit:

- Branch: `feat/v7-runtime-admission-foundation`
- Push target: `origin/feat/v7-runtime-admission-foundation`
- Latest implementation commit: `bccb26b feat: enforce cross-family lock order`
- Worktree after implementation validation and `make clean`: clean

Relevant implementation checkpoints:

- `f22ddac feat: add ranked v7 write locks`
- `f761df8 feat: serialize v7 global sequences`
- `02c546a feat: publish v7 journal transactions`
- `b96d4e4 test: add v7 multi-group mutation fault matrix`
- `80ad365 feat: close out v7 metadata transactions`
- `bccb26b feat: enforce cross-family lock order`
- `47367f0 docs: add v7 runtime handoff`

## Current Runtime Boundary

The accepted v7 surface currently provides:

- accepted image creation with `mkfs.kafs --format-version 7`;
- descriptor/checkpoint/journal inspection through `kafsdump`;
- detect-only validation through `fsck.kafs`;
- explicit read-only inspection admission through `kafs-v7`;
- v7-owned journal encoding, replay, metadata apply, checkpoint publication,
  and journal reclamation APIs exercised by focused regression tests.

Runtime controlled write is still fail closed. The `kafs-v7`
controlled-write token is recognized for boundary testing, but it is rejected
before FUSE admission. Production `kafs` remains the v4/v5 runtime and does not
admit v7. Frozen experimental v6 behavior is not a compatibility contract for
v7.

The metadata durability order fixed by T14 is:

```text
journal data/header
  -> metadata apply and read-back
  -> two byte-identical covering checkpoints
  -> covered journal headers reset one segment at a time
```

Cross-group atomic transactions remain disabled. Transactions own exactly one
group, while filesystem-global sequence validation joins group-local journal
prefixes and fails closed on collision, gap, middle-group loss, or a
checksum-consistent foreign-group mutation.

## T15 Closeout

T15 introduced one format-neutral per-thread rank stack shared by v7 and the
existing metadata lock wrappers:

1. `v7_write_gate` rank 1
2. `v7_sequence` rank 2
3. `v7_group` rank 3
4. `hrl_global` rank 10
5. `inode_alloc` rank 20
6. `inode` rank 30
7. `hrl_bucket` rank 40
8. `bitmap` rank 50

The tracker records both rank and mutex identity. A metadata-to-v7 inversion
returns `EDEADLK` before the v7 mutex is touched. The forward v7-to-metadata
order is allowed and must unwind in strict identity-LIFO order. Existing
same-rank metadata nesting, such as two inode locks, remains supported.

Applying the shared tracker exposed two existing reverse-release defects. They
were fixed without weakening the tracker:

- legacy directory block replacement now defers HRL reference release until
  the outermost inode unlock instead of temporarily dropping a non-top inode;
- regular-file copy releases its two inode locks in the strict reverse of their
  deterministic acquisition order on every exit path.

## T16 Closeout

T16 removed the v7 runtime admission dependency on the v6 controlled-write
state. `kafs_v7_runtime_admit_mount_context()` now initializes and sets only
`c_v7_controlled_write_enabled` through `kafs_v7_fuse_policy.h`; it no longer
sets `c_v6_controlled_write_enabled`.

The v7-owned policy remains fail closed. An inactive policy returns `EROFS`, an
unknown operation returns `EOPNOTSUPP`, and the only operations represented for
the later bounded write slice are `create`, regular-file `write`, `fsync`, and
`release`. This policy vocabulary does not admit controlled write: the CLI
entrypoint still rejects that mode before FUSE starts.

`scripts/check-v7-runtime-policy-ownership.sh` rejects reintroduction of the v6
controlled-write flag, helper names, or policy include into the v7 runtime and
policy files. Frozen v6 behavior was left unchanged.

## Validation Evidence

Completed against implementation commit `bccb26b` on 2026-07-16:

```sh
make -C src -j2
make -C tests v7_locks_smoketest -j2
./tests/v7_locks_smoketest
valgrind --leak-check=full --show-leak-kinds=all \
  --errors-for-leak-kinds=definite,indirect --error-exitcode=99 \
  ./tests/v7_locks_smoketest
KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
./scripts/format.sh
./scripts/lint.sh
./scripts/clones.sh
./scripts/static-checks.sh
./scripts/check-v7-layout-ownership.sh
git diff --check 80ad365..HEAD
make clean
```

Results:

- `make check -j2`: all 38 tests passed, including FUSE and v7 fault/recovery
  tests.
- `v7_locks_smoketest`: PASS for every metadata rank class in both rejected
  inverse and accepted forward cross-family order.
- Valgrind: 0 errors, 0 leaks; 15 allocations and 15 frees.
- Formatting, lint, v7 ownership, and Git whitespace checks: PASS.
- `./scripts/clones.sh`: FAIL at the existing baseline of 87 clones and 1,246
  duplicated lines (2.66%). No neutral tracker or v7 wrapper clone was found.
- `./scripts/static-checks.sh`: completed with the clone step as its single
  non-passing step; the new tracker and v7 wrapper added no complexity warning.
- Final worktree after `make clean`: clean.

T16 validation completed on 2026-07-17:

- `autoreconf -fi`, `./configure`, and `make -j2`: PASS.
- Focused `v7_entrypoint_smoketest v6_descriptor_smoketest`: 2/2 PASS.
- `v7_entrypoint_smoketest` under Valgrind: 0 errors, 0 leaks; 39 allocations
  and 39 frees.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: all 38 tests passed.
- Formatting, lint, both v7 ownership checks, Git whitespace, and clangd
  diagnostics for the v7 policy/runtime/focused test: PASS.
- The strict clone gate remained at the existing 87 clones and 1,246 duplicated
  lines (2.66%); no new v7 policy clone or complexity warning was reported.

## Remaining Risks And Constraints

- No FUSE mutation is yet routed through the complete v7 lock, sequence,
  journal publication, metadata apply, checkpoint, and reclamation lifecycle.
- The first admitted write surface must remain bounded; do not infer support
  for truncate, fallocate, unlink, rename, link, symlink, copy/reflink,
  control-plane write, hotplug delegated write, runtime TRIM, writeback cache,
  or delayed/background mutation.
- Cross-group HRL policy and multi-group atomic mutation remain later work.
- FTL/ECC correlated-failure injection is an RC media-qualification constraint,
  not an implementation blocker. RC still requires independent review and
  real-media format/mount/unmount/remount/fsck evidence. Any RC that admits
  controlled write additionally requires full-fsync and controlled
  power-interruption cycles.

## Recommended Next Slice

Route the bounded `create` / regular-file `write` / `fsync` / `release` surface
through the full v7 transaction lifecycle. That
path must acquire v7 ranks 1-3 before metadata ranks 10-50, avoid `KAFS_CALL`
while locked, and retain one cleanup path with strict reverse unlock. Keep the
controlled-write entrypoint fail closed until end-to-end durability, recovery,
and fault regression proves the complete path.

## Resume Checklist

1. Fetch and check out `origin/feat/v7-runtime-admission-foundation`.
2. Confirm `47367f0` is an ancestor and inspect the commits after that handoff.
3. Confirm `git status --short --branch` is clean.
4. Read, in order:
   - this handoff;
   - [sd-card-wear-tickets.md](sd-card-wear-tickets.md) at T14-T16 and the next
     candidates;
   - [sd-card-wear-format-v7-pivot.md](sd-card-wear-format-v7-pivot.md);
   - [.github/lock-policy.md](../.github/lock-policy.md).
5. Bootstrap and establish a local baseline:

   ```sh
   autoreconf -fi
   ./configure
   make -j2
   make -C tests check TESTS='v7_entrypoint_smoketest v7_locks_smoketest v6_descriptor_smoketest'
   ```

6. Start with bounded FUSE-to-v7 transaction routing. Do not enable controlled
   write until the complete lifecycle and fault matrix pass.
7. Follow the reviewed file/hunk WIP workflow in
   [github-dev-rules.md](../.github/github-dev-rules.md).
