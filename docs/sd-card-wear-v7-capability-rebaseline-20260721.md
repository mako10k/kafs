# KAFS format v7 capability rebaseline (2026-07-21)

- Branch: `feat/v7-runtime-admission-foundation`
- Baseline: `460f7b0`
- Status: baseline accepted; T49-T56 and T58 complete; T57 evidence audit selected while hardware and VHDX execution wait

## Purpose

This document closes the post-RCA R2 recovery wave, replaces milestone status
that predates the direct-mutation recovery work, and selects the next product
slice from the capability that is present in the current checkout.

The original rebaseline did not widen the admitted mutation surface. T49 later
added inline-to-direct promotion, T53 added the dense single-indirect lifecycle,
T54 extended that lifecycle through double-indirect regular files, and T55
closes triple-indirect regular files after fresh PERT and Task Start Gates.
Indirect-directory mutation, cross-group allocation, stable/GA claims, and
real-device destructive testing remain outside the boundary.

## Evidence Baseline

The following results were observed on the baseline checkout:

| Evidence | Result |
| --- | --- |
| `make -j2` | PASS under the configured `-Wall -Werror` build |
| `make check -j2` | PASS: 36 tests passed; 5 FUSE-dependent tests were not run after mount startup timed out |
| `./scripts/static-checks.sh` | PASS |
| Active-source clone gate | PASS: 41 clones, 409 duplicated lines, 0.86% against the 1% limit |
| `./scripts/check-v7-layout-ownership.sh` | PASS |
| `./scripts/check-v7-runtime-policy-ownership.sh` | PASS |
| `./scripts/deadcode.sh` | PASS as a report generator; 28 const-style diagnostics remain, with no enabled-path correctness, data-integrity, durability, portability, or unused-function diagnostic |
| `git diff --check` | PASS for the rebaseline documentation changes |

The five not-run tests are an environment limitation, not passing FUSE
evidence. The v7 non-FUSE and image-path tests in that run passed. Real media,
controller, kernel/FUSE, and controlled-power-interruption evidence has not yet
been collected.

## Current Capability Matrix

| Surface | Current capability | Boundary |
| --- | --- | --- |
| Image creation | `mkfs.kafs` creates explicit format v7 images | v7 is not the default production format |
| Offline validation | `fsck.kafs`, `kafsdump`, layout/policy checks, allocated-inode representation checks, dense direct/single/double/triple-reference and bitmap checks, bounded namespace payload checks, and wear-placement proof understand the current v7 image contract | This is software/image evidence, not physical NAND/FTL proof; duplicate ownership and whole-namespace graph semantics remain separately bounded |
| Offline migration | `kafsresize --migrate-create` can build a v7 destination image | No in-place metadata relocation or automatic cutover |
| Runtime admission | Dedicated `kafs-v7` entrypoint supports inspection and explicit controlled-write mounts | Successful admission does not route through v5/v6 public entrypoints |
| Inspection mount | Read-only FUSE view with selected recovery state and recovered `statfs` counters | Mutation fails closed |
| Controlled write | Dense same-group regular-file overwrite/growth/shrink through triple-indirect depth, empty-file creation, inline-file writes, inline-to-one-direct-block regular-file promotion, and same-group inline/direct directory append/growth through twelve direct references | Non-zero direct-to-inline conversion, holes, indirect-directory mutation, cross-group allocation, and unrelated metadata mutation are rejected |
| Request contract | One FUSE write callback is one journal transaction; negotiated `max_write` is capped at twelve v7 blocks | No application-system-call atomicity claim across kernel-split requests |
| Recovery | Admission closeout, interruption matrices, offline fsck, and readback cover enabled direct, single-, double-, and triple-indirect regular-file transitions | No real-card power-interruption qualification yet |
| Indirect foundation | Direct/single/double/triple address calculation, walk, and retirement guard exist in v7-owned code; all regular-file depths are admitted through T55 | Indirect-directory mutation remains `EOPNOTSUPP` |
| Cross-group mutation | Not enabled | Design direction remains deferred by the accepted boundary |

## R2 Closeout

R2 is closed at this baseline because:

1. aggregate static checks preserve report collection and return nonzero when a
   constituent fails;
2. the active-source clone result passes the unchanged 1% policy after removing
   unreachable descriptor-wire code and preserving the frozen-v6 ownership
   exception;
3. selector-bound, fsck portability/read, repair-result, HRL release-failure,
   and unsigned-zero findings with semantic impact were corrected;
4. the remaining cppcheck output is explicitly owned as repository-wide
   const-style hygiene and does not identify an enabled-path correctness,
   data-integrity, or durability defect;
5. build/test, format/lint/clone/complexity/cppcheck, ownership, and Git evidence
   have explicit results.

This disposition does not turn complexity or test-clone reports into zero-debt
claims. They remain measured maintenance inputs. The missing local `reuse`
executable is also a tooling prerequisite for the separate license script, not
an R2 correctness waiver.

## Goal And Critical Path Decision

The accepted product priorities are deterministic recovery/fail-closed
behavior and SD-card write distribution. The current implementation has a
substantial bounded direct-write surface with software recovery evidence, but
it has not crossed the real-device RC qualification boundary.

```text
R1 cardinality-independent direct mutation
  -> R2 structural/static closure
  -> current capability rebaseline
  -> M7 controlled-write RC qualification
  -> separately approved expansion or release decision
```

At the original rebaseline, M7 was selected before M8-C, M9, and M10:

- **M7 controlled-write RC qualification** is the shortest path from the
  enabled surface to evidence for the accepted fault-tolerance and wear goals.
- **M8-C indirect mutation** would enlarge the journal, recovery, allocator,
  and qualification surface before the currently enabled path is qualified.
- **M9 migration/cutover** already has destination-image creation, but a full
  operator cutover claim should depend on a qualified destination runtime.
- **M10 cross-group allocation** remains behind an explicit design-direction
  decision and is not inferred from the current same-group implementation.

That ordering is historical and was superseded after the user deferred physical
media preparation and required a fresh pre-proposal PERT. The resulting record,
[V7 indirect lifecycle PERT](sd-card-wear-v7-indirect-pert-20260721.md), keeps
the hardware join blocker visible and selects single-indirect lifecycle as the
runnable zero-slack software predecessor of expanded qualification.

## T49 Deferred-Media Addendum

Exact SD-card, reader/controller, and isolated power-cut identities were not yet
available, and the user explicitly deferred their preparation. This blocks the
physical part of M7 without changing its priority once hardware is ready. The
2026-07-21 refreshed Task Start Gate therefore selected
`SDW-V7RT-T49 regular-file inline-to-direct promotion` as the smallest coherent
software-only closure.

T49 reuses the existing same-group data COW transaction and the already proven
directory representation-transition pattern. Its admitted state is limited to
a regular inode with `blocks=0`, size at most 60 bytes, no hole, and a request
ending within one filesystem block. It zero-initializes and stages one new data
block, preserves the old inline bytes, applies the request, and atomically
publishes allocator state plus the inode's first direct reference, size, and
block count. There is no retained block to retire.

Observed T49 software evidence includes the low-level transition and immediate
direct-COW chaining, actual FUSE write/full-fsync/read-only-remount, offline
fsck, and journal-publication, metadata-apply, and checkpoint-copy recovery.
The refreshed non-destructive qualification report passed 26/26 required cases
with 90 digest-checked artifacts. Its RC, real-media, and
controller-independent-wear claims remain false.

This exception is smaller than M8-C because it adds no indirect reachability or
path-copy contract. M9 cutover still depends on a qualified destination runtime,
and M10 still requires an explicit cross-group design decision. When hardware is
available, M7 resumes against the expanded matrix, including the T49 promotion
workload.

## T50 Inode-Representation Safety Addendum

T49 made the inline/direct boundary a live controlled-write transition. The
accepted wire contract already required every disabled-tail byte to be zero and,
for `size <= 60`, required `blocks == 0` plus zero padding after the inline
payload. Before T50, the common image validator did not enforce the latter two
requirements and excluded the allocated root from its tail check.

T50 applies those representation checks to every allocated inode through
`kafs_v7_validate_image_fd()`. Consequently, detect-only `fsck.kafs`,
`kafsdump`, and `kafs-v7` admission reject the same malformed image before
consumer-specific processing. Corruption regressions cover an inline inode with
a block count, non-zero inline padding, and a non-zero root disabled tail.

This is validator hardening, not a new repair or mutation capability. It does
not validate the complete `size > 60` block tree/count relationship, add
namespace semantic validation, admit indirect mutation, or alter the real-media
authorization boundary.

Observed T50 evidence includes focused raw-layout and checkpoint-publication
regressions, the full 41-test Automake suite with two unrelated mount-timeout
skips, and passing format, lint, clone, and aggregate static gates. The active
source clone result remained 409 duplicated lines (0.86%).

## T51 Namespace-Payload Safety Addendum

The accepted v7 wire contract defines a v7-owned KDIR version 1 payload and a
non-empty, non-NUL symlink target. Before T51, the runtime parser checked KDIR
records only when lookup or readdir reached them, while the common image
validator admitted malformed namespace bytes to consumer-specific processing.

T51 validates every allocated directory and symlink whose payload is inline or
fits within the twelve direct references. KDIR headers and records must exactly
cover the inode payload; record lengths, flags, names, hashes, targets,
uniqueness, and header counts are checked. Root must be a directory and store no
`..`; each non-root directory must store exactly one live `..` pointing to an
allocated directory. Direct payload reads and inode reads both observe the
selected journal overlay.

This remains a bounded structural admission check. It does not prove namespace
reachability, parent/child graph consistency, link counts, or indirect payloads,
and it does not add repair or mutation capability. Corruption regressions cover
a direct KDIR hash, inline KDIR count, missing non-root parent record, and inline
symlink NUL. The representative corruption is rejected consistently by
`kafsdump`, detect-only `fsck.kafs`, and `kafs-v7` preflight.

Observed T51 evidence includes the focused raw-layout, checkpoint-publication,
FUSE-write, and inspection regressions; the full 43-test Automake suite; and
passing format, lint, v7 ownership, clone, and aggregate static gates. The
active-source clone result remained 409 duplicated lines (0.86%), and the new
validator functions remain below the complexity warning threshold after
decomposition. The refreshed non-destructive file-image qualification passed
26/26 required cases with 90 digest-checked artifacts; its RC, real-media, and
controller-independent-wear claims remain false.

## T52 Direct-Inode Reference Safety Addendum

The enabled controlled-write surface stores every non-inline payload densely in
at most twelve direct references. Before T52, common image admission trusted the
`blocks` field and direct slots for `size > 60`; it could therefore admit a
missing or out-of-range reference, a reference to a bitmap-free block, or an
unused direct/indirect slot before a consumer reached that inode.

T52 validates every allocated inode with `60 < size <= 12 * block_size` against
the selected recovered state. `blocks` must equal `ceil(size / block_size)`,
each required plus-one reference must resolve to an allocated data block, and
every remaining reference slot must be zero. Inode tables and group bitmaps are
both read through the journal overlay, so pending committed direct mutations
are checked as one recovered state. A neutral v7-owned resolver supplies the
same group-local mapping to this pass and bounded namespace reads.

This is direct-representation hardening, not whole-image ownership proof. It
does not reject duplicate references, establish namespace reachability or link
counts, validate `size > 12 * block_size` indirect trees, add repair, or widen
mutation admission. Corruption regressions cover block-count mismatch, missing,
out-of-range, and bitmap-free required references, a non-zero unused direct
slot, and a non-zero indirect root. A representative fault is rejected by
`kafsdump`, detect-only `fsck.kafs`, and `kafs-v7` preflight.

Observed T52 evidence includes the focused raw-layout, checkpoint-publication,
FUSE-write, and inspection regressions; the full 43-test Automake suite with 40
passes and three environment-limited FUSE mount-timeout skips; and passing
format, lint, v7 ownership, clone, and aggregate static gates. The active-source
clone result remains 41 clones and 409 duplicated lines (0.85%), with no new
complexity warning. The refreshed non-destructive file-image qualification
passed 26/26 required results with 90 digest-checked artifacts; its RC,
real-media, and controller-independent-wear claims remain false.

## T53 Single-Indirect Lifecycle Addendum

T53 changes the regular-file ownership graph from inode-to-data only to an inode
that may also own one COW single-indirect root. A write crossing the direct
boundary, a single-indirect overwrite, and contiguous growth stage every
requested data block plus a copied root in one same-group batch. The inode,
bitmap/summary state, size, and `blocks = data + root` value publish atomically;
old data and root blocks retire only after the new graph is checkpointed.

The batch commit guard now reconstructs before/after inodes through the journal
overlay and proves that every staged block is reachable from the after graph and
every retained block was reachable from the before graph. Common image
validation admits only dense single trees with allocated required references,
zero unused references, zero double/triple roots, and the exact data-plus-root
count. A deliberately non-zero unused root entry is rejected.

Observed focused evidence includes low-level boundary/overwrite/growth/truncate
transitions, checkpoint publication, actual FUSE full-fsync/readback/fsck and
inspection remount, single-to-direct truncate, and journal-publish,
metadata-apply, and checkpoint-copy recovery. Final aggregate gate evidence is
recorded in T53 and the closeout PERT. Real-media, RC, double/triple,
indirect-directory, sparse-file, and cross-group claims remain false.

The post-T53 closeout rebuild splits the former combined double/triple estimate
into causal depth boundaries. It selects dense same-group double-indirect
regular-file lifecycle `D2` as the next runnable zero-slack software node; `D3`
triple depth and expanded qualification follow it. Hardware identity remains an
unknown-duration join blocker rather than disappearing from the graph. This is
a PERT selection only, so D2 still requires a fresh Task Start Gate.

## T54 Double-Indirect Lifecycle Addendum

T54 ran that fresh Task Start Gate at `e79b6e8` and received `PASS`. The
regular-file ownership graph may now contain the always-full single root, one
double root, and the exact number of dense double-leaf tables needed by file
size. Crossing single to double depth and writing within double depth copies
each touched data block and leaf, the double root, the single root when the
request crosses it, and the inode in one same-group batch. Retirement remains
strictly post-publication.

Shrinking supports partial and aligned double tails, leaf pruning, and
double-to-single/direct/zero representation changes. `inode.blocks` counts data
plus the single root, double root, and populated leaf tables. Common image
validation requires every size-implied reference to be allocated and every
unused single, double-root, and leaf entry to be zero. The validator still does
not claim duplicate-reference ownership or whole-namespace reachability.

Focused evidence covers the single/double and double-child-table boundaries,
overwrite/growth, partial/aligned shrink, leaf pruning, all lower-depth
transitions, invalid unused references, actual FUSE full-fsync/readback/remount,
offline fsck, and journal-publication, metadata-apply, and checkpoint-copy
recovery. The final non-destructive qualification result is 32/32 PASS with
104 digest-checked artifacts, and the full Automake result is 42 PASS with one
environment-limited FUSE stress SKIP. Format, lint, ownership, clone, and
aggregate static gates pass; the active-source clone result is 42 clones and
422 duplicated lines (0.86%), below the 1% limit.

The T54 closeout PERT removes completed `D2` from the frontier and selects
dense same-group triple-indirect lifecycle `D3` as the next runnable zero-slack
software node. Expanded qualification `Q` follows D3; namespace graph work `N`
remains runnable but off that causal path. A fresh Task Start Gate on the
post-T54 committed checkout is still required before D3 implementation.

Triple depth, sparse files, indirect directories, cross-group
allocation, repair, RC, and physical-media execution remain outside T54.

## T55 Triple-Indirect Lifecycle Addendum

T55 ran the fresh Task Start Gate at `c62d426` and received `PASS`. The dense
regular-file graph may now own triple leaves, middle tables, and one triple root
in addition to the full lower representations. Writes load and COW only the
touched triple path, then stage data, leaf, middle, root, and inode in one
same-group transaction. Lower-depth blocks touched by a request crossing the
double/triple boundary remain in that same transaction.

Shrinking now supports partial and aligned triple tails, leaf and middle-table
pruning, and direct triple-to-double/single/direct/zero contraction. Publication
precedes retirement, and suffix retirement streams the removed tree so the
bounded mutation batch does not need to materialize the complete triple tree.
`inode.blocks` counts data and every owned single, double, and triple index
node. Common image validation traverses all three triple levels, requires every
size-implied reference to be allocated, and rejects non-zero unused root,
middle, and leaf entries. Duplicate-reference ownership and whole-namespace
reachability remain separate bounds.

Focused actual-FUSE evidence covers double-to-triple crossing, overwrite,
partial shrink, direct contraction to each lower representation, and the first
triple middle-table boundary. Detect-only fsck, negative unused-reference
checks, and journal-publication, metadata-apply, and checkpoint-copy recovery
all pass. The T55 non-destructive qualification result is 37/37 PASS with 116
digest-checked artifacts. Its DRAFT real-media contract now requires
`regular_file_triple_indirect_lifecycle` but does not authorize a real device.
The full Automake gate reports 42 PASS and one environment-limited stress SKIP.
Format, lint, both v7 ownership checks, clone, and aggregate static gates pass;
the strict source clone result is 48 clones and 490 duplicated lines (0.97%),
below the 1% limit.

The closeout PERT marks both `D3` and its expanded software qualification `Q`
complete. The accepted path to real-media recovery `R` now has no runnable
software predecessor: exact media identity and digest-bound approval `H` is the
remaining join blocker. Namespace graph validation `N` remains runnable but
off that causal path and must not be substituted merely because it is local.

Sparse files, indirect directories, cross-group allocation, repair, RC, and
physical-media execution remain outside T55.

## T56 VHDX Host-Recovery Harness Addendum

When disposable hardware remained unavailable, the machine-evaluated residual
plan added VHDX-backed host recovery as a prerequisite to later physical
execution without removing the blocked hardware-approval branch. The selected
T56 slice provides a dedicated regular-file KAFS image, four exact durability
pause points, separate arm/verify modes, a WSL ext4 safety preflight, and a
native Windows controller that requires explicit high-impact execution consent.

The active Ubuntu VHDX is never a raw KAFS target. Windows registry identity,
VHDX length, marker observation, terminate/restart exit codes, WSL context,
payload/recovery validation, full fsck, dump output, and artifact digests form
the evidence boundary. Every evidence record keeps RC, real-media, isolated
physical power, and controller-independent wear claims false.

T56 makes the host interruption run executable and resumable across WSL
restart; it does not perform that interruption while the controlling Codex
session resides inside the target distro. Hardware identity and destructive
approval are still required in parallel before any real-media execution can
start. The next-node statement at T56 closeout is superseded by the following
2026-07-22 blocker-decomposition addendum.

## 2026-07-22 Blocker-Decomposition Addendum

The user deferred the native Windows run because the target Ubuntu distro hosts
other tasks. Refreshed evidence also showed that the coarse plan hid three
goal-path capabilities that do not require either disruptive window:

- T57: a read-only aggregate audit of all four VHDX evidence directories;
- T58: the real-media execution-artifact and independent-review validation
  contract; and
- T59: disposable-file-image v5-to-v7 data migration, resume, and rollback
  rehearsal beyond destination creation.

`plans/current.pert` now models those capabilities separately. The checked
`dag next` result selects T58 as the only zero-slack `RUNNABLE NOW` node. T57
and T59 are `READY / WAITING RESOURCE` behind T58 under primary capacity one.
The actual VHDX run and exact hardware approval remain `BLOCKED NOW`; neither is
removed or treated as complete. Physical execution and production cutover still
join through audited VHDX capture, authorized media, the T58 evidence contract,
independent review, and the T59 rehearsal.

This addendum changes task granularity and ordering, not the accepted end goal
or qualification claim. T58 must pass its own Task Start Gate before code edits,
and it may not open a device, format/mount media, interrupt power, or assert RC.

## 2026-07-22 T58 Closeout Addendum

T58 passed its Task Start Gate and closed the real-media artifact and
independent-review contract. The versioned evidence/review records bind the
approved matrix and approval by byte digest, preserve exact before/after
apparatus identity, require complete workload/boundary/cycle coverage, verify
relative artifact sizes and SHA-256 values, and separate the operator from the
reviewer. `ACCEPT` is fail closed unless every result is `PASS` and every review
check is true. Synthetic positive/negative regression and the full test suite
passed without device, mount, power, or WSL operations.

After marking `REAL_MEDIA_EVIDENCE_CONTRACT_READY` reached and rebuilding the
stored network, `dag next` selects T57 `VHDX_EVIDENCE_AUDIT` as the only
zero-slack `RUNNABLE NOW` node. T59 remains `READY / WAITING RESOURCE` with
1.333 days total float. The actual VHDX run and hardware approval remain
`BLOCKED NOW`. This closeout changes the selected non-disruptive predecessor;
it does not authorize host capture or physical-media execution.

## Deferred Gate: SDW-V7RT-T48 Controlled-write RC Qualification

### Scope

- define the exact host, kernel, libfuse, card, reader/controller, and power-cut
  identity recorded for each sample;
- add a repository-run qualification procedure that formats, mounts, executes
  each currently enabled mutation class, performs full `fsync`, introduces
  controlled interruption points, remounts, and runs `fsck.kafs` plus
  `kafsdump`;
- retain raw command logs, image/sample identifiers, digests, and observed
  recovery outcomes;
- require independent review of the evidence before any RC wording is accepted;
- keep physical-device formatting and power interruption behind an explicit
  device/sample matrix and explicit operator authorization.

### Exit Criteria

1. Every enabled mutation class has a normal-path and controlled-interruption
   case on the approved matrix.
2. Each interrupted sample either recovers to an allowed state or fails closed,
   with offline fsck and dump evidence.
3. The result distinguishes filesystem-placement evidence from unverified
   NAND/FTL behavior.
4. The report records skipped, inconclusive, and environment-limited cases
   separately from PASS.
5. An independent reviewer accepts or rejects the bounded RC claim from the raw
   evidence.

### Non-goals

- enabling indirect-directory or cross-group mutation;
- claiming stable or GA readiness;
- claiming controller-independent wear behavior;
- formatting any real device without the operator's explicit authorization.
