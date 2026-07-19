# V7 recovery investigation and critical-path plan

- Date: 2026-07-19
- Baseline: `b25b09e` on `feat/v7-aligned-direct-overwrite`
- Related incident: `KAFS-INC-2026-07-19-01`
- Status: investigation complete enough to select the decision gate; product
  implementation remains blocked on the delivery-goal decision below

## Task Start Record

### Current evidence

- The worktree was clean at `b25b09e` before this investigation began.
- The accepted v7 design makes fault tolerance and SD-card write distribution
  the joint top priorities. Deterministic fsck recovery or rejection is a hard
  gate, and compatibility is provided by offline migration.
- The accepted release boundary defines an explicit-opt-in v7 RC and requires
  independent real-media qualification. Every enabled software surface must
  still pass its corresponding correctness and recovery gates.
- M6 and M6.1 are implemented. The current branch also enables bounded M8-A
  growth/truncate and an M8-B create surface through three direct directory
  blocks.
- The handoff records an unresolved product decision after M6.1: qualify the
  bounded controlled-write surface through M7, or continue writable-surface
  implementation first. Later implementation did not turn that choice into a
  separately accepted delivery goal.
- Repository-wide static analysis currently reports 97 source clone groups,
  1,351 duplicated source lines (2.69%), and 101 complexity warnings. Cppcheck
  reports 19 findings. These are escape and risk evidence, not a priority
  ordering by themselves.

### Assumption audit

- The handoff's next candidate is not treated as authorization or as the
  definition of done.
- Ticket order is not assumed to be a capability dependency order.
- The already implemented create surface is not assumed to be required for the
  nearest RC merely because it exists.
- Conversely, unreleased code is not assumed disposable merely because it can
  be removed. Removal and refactoring are alternative recovery strategies whose
  distance to the accepted delivery goal must be compared.
- Static findings outside `kafs_v7_fuse_write.c` remain owned by this repository.
  They require a recorded disposition even when they are not on the selected
  wave's critical path.

### Start decision

- `PASS`: repository-wide investigation and recovery planning.
- `BLOCKED`: corrective product edits until the delivery goal is selected as
  either the earliest bounded v7 RC or an RC that includes bounded direct-file
  create.

The block is intentional: without that decision, "shortest path" has two
different destinations and cannot determine whether removal or refactoring is
the shorter recovery.

## Accepted Goal And Capability Position

The accepted architectural goal is a v7 format that distributes SD-card writes
without weakening deterministic recovery. The nearest accepted delivery
boundary is an explicit-opt-in RC with real-media qualification, but the
enabled write surface for that RC is not yet selected.

Current capability position:

```text
accepted v7 wire format and offline recovery
  -> read-only inspection
  -> journal/checkpoint/locking transaction foundation
  -> bounded controlled overwrite and recovery
  -> FUSE atomic-request negotiation
  -> bounded direct growth/truncate
  -> bounded direct-file create (implemented, structurally non-accepted)
```

The last capability is behaviorally tested but its production decomposition is
not accepted. It cannot be carried into qualification unchanged.

## Capability Dependency Graph

```text
                         +-> remove create admission and its incomplete claims
accepted delivery goal -|                                      |
selection gate           |                                      v
                         |                              bounded-surface M7 RC
                         |
                         +-> generic direct-directory mutation primitive
                                -> direct(N) append/growth/limit recovery proof
                                -> accepted bounded M8-B create surface
                                -> create-inclusive M7 RC

both paths -> static finding disposition ledger
           -> blocking correctness/durability fixes for every enabled surface
           -> aggregate gate exit-status correction
           -> independent review and real-media qualification
```

M8-C indirect COW, M9 data migration, and M10 cross-group mutation are not
prerequisites for either bounded RC option. They remain owned future
capabilities and must not be pulled ahead of the selected RC path without a new
accepted delivery goal.

## Escaped-Impact Investigation

The incident's reasoning failure was not confined to the four create commits.
The repository-wide analysis found the following connected impact and control
weaknesses:

| Evidence | Current disposition | Path relationship |
| --- | --- | --- |
| `kafs_v7_fuse_create_in_direct_directory` is 375 NLOC with CCN 103 and contains separate 1-, 2-, and 3-block control flows | Must be removed or replaced by one semantic-transition implementation | Mandatory before any RC containing create |
| Direct-directory code uses arrays sized 2/3 and literal slot boundaries 12/15 | Centralize named direct/indirect reference roles before generic directory mutation | Prerequisite to accepted M8-B refactor |
| Create recovery fixtures and scenarios are named by cardinality and repeat orchestration | Convert to transition-class tables covering inline append, inline-to-direct, direct append, direct growth, and direct-limit rejection | Mandatory evidence for refactored M8-B |
| `kafs_v7_fuse_write_direct` has separate single and batch paths (CCN 69) | Investigate unifying on the batch primitive; retain a split only with a demonstrated semantic invariant | Adjacent same-cause candidate on every enabled write path |
| Single and batch COW lifecycle implementations in `kafs_v7_runtime_transaction.c` repeat prepare/publish/finish mechanics | Derive one internal cardinality-independent operation model, then keep compatibility wrappers only if needed | Durability-sensitive prerequisite if FUSE unification reaches this layer |
| Same-file clones are concentrated in `kafs_shared_fuse_runtime.c` | Map each clone to an operation-state pair and test whether one shared transition helper preserves lock and error semantics | Owned investigation; blocking only where it affects the selected enabled surface |
| v6/v7 layout, mount-option, runtime, entrypoint, and CLI clones cross format ownership boundaries | Perform intent-oriented BlameCheck plus invariant comparison; extract only genuinely format-neutral mechanics | Owned investigation; format ownership forbids clone-count-driven merging |
| Layout selector cppcheck warnings appear in both v6 and neutral descriptor helpers | Prove loop bounds and reorder the bound check before indexing even if currently safe | Correctness hardening before release gate |
| Unsigned `<= 0`, shadowing, const suggestions, and fsck `void *` arithmetic | Fix the portable pointer arithmetic and audit each remaining warning for semantic effect; record suppressions only with proof | Owned release-gate cleanup |
| `scripts/static-checks.sh` reports failed steps but exits zero | Return nonzero when any required step fails while preserving complete report generation | Mandatory control correction before relying on the aggregate gate |
| Source clone gate is at 2.69% against a 1% policy | Reduce confirmed semantic duplication and explicitly justify only true ownership/boilerplate exceptions | Repository release constraint, not a reason to reorder capability work by clone count |
| Test clones are 18.49% in the informational pass | Consolidate recovery orchestration after production state classes are fixed | Evidence maintainability; must not drive production abstraction |

No row is labeled unrelated or exempt. "Owned investigation" means its priority
will be recomputed against the selected goal and its final disposition will be
recorded; it does not transfer responsibility elsewhere.

## Alternative Ordering Comparison

### Option A: earliest bounded v7 RC

1. Remove create admission and the incomplete M8-B implementation from the RC
   surface while preserving accepted M6/M6.1 and explicitly selected M8-A
   behavior.
2. Fix correctness/durability findings that affect that enabled surface and
   make the aggregate static gate fail correctly.
3. Run M7 software, independent-review, and real-media qualification.

This is the shortest route if create is not required by the target workload.
It does not declare the removed work acceptable; the incident code is removed
from the delivery surface and M8-B remains an owned future capability.

### Option B: create-inclusive bounded v7 RC

1. Introduce named inode reference roles and one `direct(N)` directory mutation
   plan covering append, one-block growth, and direct-limit rejection.
2. Reuse a cardinality-independent COW lifecycle and remove the specialized
   production branches.
3. Replace cardinality-driven recovery orchestration with transition-class and
   boundary tables, then run the complete interruption matrix.
4. Fix correctness/durability findings affecting the enabled surface and the
   aggregate gate, then perform M7 qualification.

This is longer than Option A but shortest if bounded create is a required RC
capability.

### Rejected first waves

- Continuing to four-block create is a local continuation of the incident and
  does not close a capability dependency.
- Starting M8-C, M9, or M10 increases distance to either bounded RC before the
  enabled create surface is resolved.
- Reducing the largest clone count first lets a detector choose product order
  and does not establish which capability becomes releasable.
- Refactoring all v6/v7 cross-format clones first risks violating accepted
  ownership boundaries and is not a prerequisite for either bounded RC.

## Recovery Waves

### Wave 0: delivery-goal decision

Record whether the next accepted delivery is the earliest bounded RC or a
create-inclusive bounded RC, and list the workload that makes each enabled
surface necessary. Exit when one option is accepted and the other is explicitly
deferred.

### Wave 1A: bounded-RC surface rollback

Applies only to Option A. Remove create from admission, code, claims, and tests
without weakening fail-closed behavior. Recompute the graph after verification.

### Wave 1B: generic direct-directory recovery

Applies only to Option B. Replace block-count-specific production flow with the
five semantic transitions in the RCA. The wave closes the M8-B structural
correctness dependency and unlocks create-inclusive recovery qualification.

### Wave 2: enabled-path correctness and control closure

Review the single/batch COW model and all static findings touching the selected
surface, fix the aggregate static exit status, and give every repository-wide
finding a disposition. Ordering within this wave follows data-integrity and
durability dependencies, not finding counts.

### Wave 3: M7 qualification

Run full software recovery tests, independent review, and the accepted
real-media format/mount/unmount/remount/fsck evidence. A controlled-write RC
also requires write, full fsync, and controlled power-interruption cycles.

## Wave 1B Exit Criteria

These criteria are ready if Option B is selected:

1. One production control flow handles `direct(N) -> direct(N)` and
   `direct(N) -> direct(N + 1)` for every admitted `N` below the named direct
   limit.
2. Inline append and inline-to-direct conversion remain separate because their
   representation and retirement invariants differ.
3. The direct limit rejects growth without publishing a journal transaction or
   changing allocator, inode, directory, or free-space state.
4. Parent blocks, parent inode, child inode, allocator state, and counters are
   atomically published for every admitted transition; only replaced old blocks
   are retired after the covering checkpoint.
5. Recovery tests cover representative interior cardinalities and both direct
   boundaries through the common fault-point API, without creating production
   branches for fixtures.
6. The analogous regular-file single/batch path and transaction lifecycle have
   a written disposition based on invariants, not proximity or authorship.
7. Full build/tests, format, lint, ownership checks, clone/complexity checks,
   cppcheck, and Git whitespace checks have explicit PASS/FAIL results.

## Local-Optimum Check

Wave 0 is the only valid next wave because it changes the identity of the
shortest path: Option A removes a non-required surface, while Option B repairs
and qualifies it. Starting either implementation before that choice optimizes
for the current code shape. After every wave, the capability graph must be
rebuilt from current code, tests, specifications, and the accepted delivery
goal before selecting the next wave.
