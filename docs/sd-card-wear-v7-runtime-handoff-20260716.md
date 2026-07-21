# KAFS format v7 runtime handoff (updated 2026-07-21)

## Scope

This handoff began at `SDW-V7RT-T19 retained data-block retirement and retry
closeout` and now records the current runtime through the post-RCA capability
rebaseline. It is intended to make the next qualification slice resumable from
another host without reopening the accepted wear-leveling and fault-tolerance
decisions.

This document is a dated evidence snapshot and a source of candidate work. It
is not implementation authorization and does not define current exit criteria.
On every resume, run the `AGENTS.md` Task Start Gate against the current branch,
HEAD, worktree, code, tests, and accepted specifications. Preserve accepted
design decisions unless new evidence contradicts them, but re-derive the task
boundary and definition of done from the refreshed evidence before editing.

Original repository checkpoint for this handoff:

- Branch: `feat/v7-runtime-admission-foundation`
- Push target: `origin/feat/v7-runtime-admission-foundation`
- Implementation checkpoint: `a332343 feat: retire v7 retained data blocks`
- This handoff snapshot is documentation-only and makes no runtime changes.
- Later sections have accumulated progress updates beyond this original
  checkpoint. Their status and next-task wording must therefore be reconciled
  with the current checkout rather than assumed current.

Relevant implementation checkpoints:

- `f22ddac feat: add ranked v7 write locks`
- `f761df8 feat: serialize v7 global sequences`
- `02c546a feat: publish v7 journal transactions`
- `b96d4e4 test: add v7 multi-group mutation fault matrix`
- `80ad365 feat: close out v7 metadata transactions`
- `bccb26b feat: enforce cross-family lock order`
- `47367f0 docs: add v7 runtime handoff`
- `2b3a17d refactor: give v7 its own write policy`
- `b01ea23 feat: add v7 runtime transaction coordinator`
- `acdfb41 feat: add v7 data COW allocator planner`
- `a332343 feat: retire v7 retained data blocks`
- `1089fe3 feat: negotiate v7 atomic write requests`
- `13cc9a0 test: replay v7 recovery-wave effectiveness`
- `460f7b0 refactor: remove unused descriptor wire path`

## Current Runtime Boundary

The accepted v7 surface currently provides:

- accepted image creation with `mkfs.kafs --format-version 7`;
- descriptor/checkpoint/journal inspection through `kafsdump`;
- detect-only validation through `fsck.kafs`;
- common fail-closed validation of allocated-inode representation and bounded
  namespace payloads, including inline block counts/padding, disabled tails,
  v7-owned KDIR records, non-root parent records, and symlink targets, before
  offline or runtime consumers proceed;
- explicit read-only inspection admission through `kafs-v7`;
- v7-owned journal encoding, replay, metadata apply, checkpoint publication,
  and journal reclamation APIs exercised by focused regression tests;
- a mount-lifetime v7 transaction coordinator that serializes global sequence
  publication and runs the complete metadata closeout lifecycle;
- a v7-owned group-local data-block COW/allocator planner that durably stages
  and re-verifies up to twelve direct blocks before atomically publishing their
  inode references, sizes, block counts, and allocator state;
- a post-checkpoint retained-block retirement transaction that verifies the
  direct/HRL reference set, clears the allocator state, and closes out retry;
- v7-only FUSE `fsync` / `release` closeout barriers;
- explicit controlled-write admission for direct regular-file overwrite,
  contiguous growth, shrinking above the inline boundary or to zero, and
  `O_TRUNC`, including partial and multi-block COW;
- same-group empty regular-file create, inline-file write through the create
  handle, regular-file inline-to-one-direct-block promotion, and inline/direct
  directory append or growth through the twelve direct references;
- negotiated FUSE `max_write` and a startup diagnostic that distinguishes the
  per-request atomic limit from application-system-call atomicity;
- admission recovery and diagnostics for interruption after journal
  publication, metadata apply, checkpoint copy, or journal reclamation;
- v7-owned direct/single/double/triple indirect address calculation, traversal,
  and retirement guards, without admitting indirect mutation.

Runtime controlled write remains fail closed outside that allowlist. Non-zero
direct-to-inline conversion, holes, inline promotion beyond one direct block,
indirect mutation, cross-group allocation, and wider metadata mutations are
rejected. Production `kafs` remains the v4/v5 runtime and does not admit v7.
Frozen experimental v6 behavior is not a compatibility contract for v7.

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

## Milestone Snapshot

| Milestone | Scope | Status |
| --- | --- | --- |
| M0 | T1-T2: v7 format design and accepted wire contract | Complete |
| M1 | T3-T5: offline tooling, wear model, and fault model | Complete |
| M2 | T6: explicit read-only runtime inspection | Complete |
| M3 | T7-T15: journal, checkpoint, locking, and crash recovery foundation | Complete |
| M4 | T16-T17: mount-lifetime coordinator and fail-closed runtime boundary | Complete |
| M5 | T18-T19: internal full-block data COW and retained-block retirement | Complete |
| M6 | T20-T32: bounded direct overwrite, admission, diagnostics, partial/multi-block COW, interruption recovery | Complete |
| M6.1 | T33: FUSE request negotiation and supported atomic-request observability | Complete |
| M7 | T48: controlled-write RC qualification, real-media power interruption, and independent review | Selected next; destructive execution not authorized |
| M8-A | Existing-inode growth, allocation, hole policy, and truncate | Complete for bounded direct files |
| M8-B | Create and directory-record mutation | Complete for bounded same-group inline/direct directories after R1 correction |
| M8-B.1 | T49 regular-file inline-to-one-direct-block promotion | Complete with file-image normal/recovery qualification refresh |
| M8-B.2 | T50 allocated-inode representation validation | Complete for inline block count/padding and all allocated disabled tails |
| M8-B.3 | T51 namespace payload structural validation | Complete for inline and bounded direct KDIR/symlink admission |
| M8-C | Indirect-block COW, traversal, and retirement | Address/walk/retirement foundation present; mutation not admitted |
| M9 | v5-to-v7 data migration beyond destination creation | Destination creation present; full data migration/cutover not complete |
| M10 | Cross-group HRL and multi-group atomic mutation | Not started |

The bounded direct portions of M8-A and M8-B are implemented. R1 replaced the
block-count-specific create path with one direct-`N` transition model and
table-driven boundary/recovery evidence. R2 then closed the structural/static
control wave. This is still not general writable-filesystem readiness:
indirect mutation, holes, cross-group allocation, and most metadata mutations
remain outside the admitted contract.

The post-R2 Task Start and Goal And Critical Path Gate selected M7 controlled-
write RC qualification before further mutation expansion. Exact media identity
remained unavailable, so the user explicitly deferred that external dependency.
The refreshed gate selected T49 as the smallest software-only capability closure
and required the file-image and DRAFT real-media matrices to grow with it. The
following T50 and T51 safety slices aligned the common image validator with the
accepted inline-inode and bounded namespace wire contracts without widening
mutation admission. Whole-namespace graph validation and indirect payload
validation remain outside this boundary. The ordering record is maintained in
`docs/sd-card-wear-v7-capability-rebaseline-20260721.md`.

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

## T17 Closeout

T17 combines the previously separate v7 lock, sequence, journal writer, and
metadata closeout APIs behind `kafs_v7_runtime_transaction`. One service owns
the rank 1-3 lock state and global sequence state for the mount lifetime. A
commit accepts metadata patches from exactly one group and returns only after
journal publication/confirmation, metadata apply, two-copy checkpoint
publication, and covered journal reclamation.

The service rejects read-only and `O_APPEND` descriptors before publication.
The shared positional-write FD contract is in `kafs_v7_io.h` and is also used
by checkpoint publication. Focused regression proves consecutive sequence 1
and 2 commits through one service, idempotent closeout barriers, and
cross-group `EXDEV` rejection without image mutation.

Only the `kafs-v7` build receives `KAFS_V7_RUNTIME_ENTRYPOINT`. In a v7
controlled context the common legacy mutation guard returns `EOPNOTSUPP`,
including for create/write, `O_TRUNC`, control-plane open, and `fsyncdir`.
Regular-file `fsync` and `release` call the v7 closeout barrier. This wiring is
dormant in production because controlled-write admission still fails before
FUSE starts.

## T18 Closeout

T18 adds `kafs_v7_data_cow` as a v7-only allocator and full-block staging
module. It reads the authoritative bitmap and complete unpadded L1/L2 allocator
summary through the journal overlay, rejects any mismatch, and searches from a
per-group mount-lifetime cursor. A successful stage writes one full block,
flushes it, reads it back, and advances the cursor even if the later metadata
publication is aborted. This avoids repeatedly staging failed attempts on the
same physical location.

The transaction service keeps the same rank 1-3 reservation from planning
through durable data staging and journal publication. Commit re-verifies the
staged bytes and requires a caller-owned direct inode-slot patch that names the
selected block. For overwrite, the same slot's overlay before-image must name
the supplied retained block. The bitmap word and allocator summary after-images
are prepended to that metadata transaction, so no allocator state becomes
visible before data durability.

After a successful overwrite, both the new and old blocks remain allocated.
The covering two-copy checkpoint protects the pointer transition, but T18 does
not yet retire the old block. Abort or pre-publication failure leaves only
unreferenced bytes in a still-free data span. Indirect references, multi-block
writes, directory mutation, and FUSE create/write admission remain outside the
implemented boundary.

## T19 Closeout

T19 closes the retained-block lifecycle with an independent v7-owned
transaction. The allocator planner verifies the journal-overlay bitmap and
complete summary, clears one allocated group-local bit, rebuilds the summary,
and emits `free_blocks_delta=+1`. A foreign-group block returns `EXDEV`; an
already-free block returns `EALREADY` without publishing a second delta.

Runtime retirement first closes any durable journal prefix. It then holds the
rank 1-3 reservation across fresh image/replay validation, planning, and a
whole-image reference scan. Every allocated inode's direct slots and every
active HRL entry are read through the overlay. A live reference returns
`EBUSY`. Any non-zero indirect root anywhere returns `EOPNOTSUPP`, because T19
does not guess whether an untraversed indirect block references the target.
The global write gate keeps this conservative scan stable while the target
group transaction is prepared.

Successful publication runs the normal metadata apply, two-copy checkpoint,
and covered journal reclamation path. If a process stops after the retirement
journal header becomes durable, the next retirement call first completes that
prefix; replanning then observes the free bit and returns `EALREADY`. This
prevents both double free and repeated counter increments. FUSE controlled
write remains rejected before admission.

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

T17 validation completed on 2026-07-17:

- `autoreconf -fi`, `./configure`, and `make -j2`: PASS.
- Focused `v7_checkpoint_publication_smoketest v7_entrypoint_smoketest
  v6_descriptor_smoketest`: 3/3 PASS.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: all 38 tests passed.
- `v7_checkpoint_publication_smoketest` under Valgrind: 0 errors, 0 leaks;
  5,151 allocations and 5,151 frees.
- Formatting, lint, both v7 ownership checks, Git whitespace, and clangd
  diagnostics for the coordinator, policy, FUSE wiring, and focused tests:
  PASS.
- The strict clone gate is the existing 87 clones and 1,246 duplicated lines
  (2.64%). `static-checks.sh` completed with clone as its single non-passing
  step; the new coordinator and I/O helper add no clone or complexity warning.

T18 validation completed on 2026-07-17:

- `autoreconf -fi`, `./configure`, and `make -j2`: PASS.
- Focused `v7_checkpoint_publication_smoketest v7_entrypoint_smoketest
  v6_descriptor_smoketest`: 3/3 PASS.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: all 38 tests passed.
- `v7_checkpoint_publication_smoketest` under Valgrind: 0 errors, 0 leaks;
  7,439 allocations and 7,439 frees.
- Formatting, lint, both v7 ownership checks, Git whitespace, and clangd-18
  diagnostics for the COW planner, transaction service, headers, and focused
  test: PASS.
- The strict clone gate remains at 87 clones and 1,246 duplicated lines
  (2.60%). No new COW/transaction clone was reported. `static-checks.sh`
  completed with clone as its single non-passing step; the two new/modified
  modules have no lizard threshold warning.

T19 validation completed on 2026-07-17:

- `make -j2`: PASS under `-Wall -Werror`.
- Focused `v7_checkpoint_publication_smoketest`: PASS.
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 36 PASS / 2 SKIP.
  `min_git_hooks` and `stress_fs` skipped because this environment could not
  mount FUSE for those cases.
- `v7_checkpoint_publication_smoketest` under Valgrind: 0 errors, 0 leaks;
  12,471 allocations and 12,471 frees.
- Formatting, lint, both v7 ownership checks, Git whitespace, and clangd-18
  diagnostics for the allocator/retirement path, runtime service, headers,
  and focused test: PASS.
- The strict clone gate remains at 87 clones and 1,246 duplicated lines
  (2.58%). No changed module appears in a new clone. `static-checks.sh`
  completed with clone as its single non-passing step; the new retirement
  functions have no lizard threshold warning.

## Remaining Risks And Constraints

- Indirect address calculation and walking exist, but indirect mutation is not
  admitted. Do not infer indirect COW, growth, or truncate support from the
  traversal foundation.
- Retirement remains a correctness-first synchronous path and is not a
  scalable background reclaimer.
- The admitted write surface remains bounded; do not infer support for
  fallocate, unlink, rename, link, symlink, copy/reflink, control-plane write,
  hotplug delegated write, runtime TRIM, writeback cache, or delayed/background
  mutation.
- Cross-group HRL policy and multi-group atomic mutation remain later work.
- FTL/ECC correlated-failure injection is an RC media-qualification constraint,
  not an implementation blocker. RC still requires independent review and
  real-media format/mount/unmount/remount/fsck evidence. Any RC that admits
  controlled write additionally requires full-fsync and controlled
  power-interruption cycles.

## Recommended Next Slice

When the required hardware is available, complete the `SDW-V7RT-T48-B1` draft matrix described in
`docs/sd-card-wear-v7-real-media-qualification-approval.md`. T48-A has validated
the non-destructive file-image path, T49 refreshed it for regular-file promotion,
T50 hardened the shared inode admission boundary, and T48-B1 has fixed the
fail-closed matrix, destructive-impact, evidence-
retention, digest-bound approval, and independent-review contract. Supply the
exact native/passthrough host, physical card unit, reader/controller, isolated
power-cut apparatus, trigger protocol, and cycle count; validate
`READY_FOR_APPROVAL`; then approve that exact matrix digest. Do not format a real
device, add a `/dev/*` execution path, or introduce a physical power interruption
before that approval.

## Resume Checklist

1. Fetch and check out `origin/feat/v7-runtime-admission-foundation`.
2. Confirm `460f7b0` is an ancestor and inspect any commits after it.
3. Confirm `git status --short --branch` is clean.
4. Read, in order:
   - [sd-card-wear-v7-capability-rebaseline-20260721.md](sd-card-wear-v7-capability-rebaseline-20260721.md);
   - this handoff;
   - [sd-card-wear-tickets.md](sd-card-wear-tickets.md) at T45-T48;
   - [sd-card-wear-format-v7-pivot.md](sd-card-wear-format-v7-pivot.md);
   - [.github/lock-policy.md](../.github/lock-policy.md).
5. Bootstrap and establish a local baseline:

   ```sh
   autoreconf -fi
   ./configure
   make -j2
   make -C tests check TESTS='v7_entrypoint_smoketest v7_locks_smoketest v6_descriptor_smoketest'
   ```

6. Revalidate the T48-B1 draft matrix, fill only directly observed physical
   identities, and request approval for its exact digest. Preserve the current
   mutation boundary and keep real-device actions behind explicit operator
   authorization.
7. Follow the reviewed file/hunk WIP workflow in
   [github-dev-rules.md](../.github/github-dev-rules.md).
