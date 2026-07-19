# V7 directory block-cardinality specialization RCA

- Incident ID: `KAFS-INC-2026-07-19-01`
- Date: 2026-07-19
- Status: RCA complete; corrective refactor pending
- Classification: development-quality incident
- Released or remote impact: none observed

## Summary

The v7 create path was advanced through separate one-to-two-block growth,
two-block append, two-to-three-block growth, and three-block append slices even
though the underlying operation is parameterized by the current direct-block
count. This produced block-count-specific branches, repeated fixtures, and a
ticket sequence that would have continued with three-to-four-block growth.

The first one-to-two-block implementation was a reasonable proof of the batch
COW and recovery model. The incident began when the next task was framed as a
two-block-only append instead of extracting the already visible `N`-block
invariant. The implementation then continued for three additional commits
before the user challenged the approach.

## Impact

- No released image, remote branch, or known filesystem data was affected.
  At RCA time, no remote ref contained the incident commits.
- Four local commits from `939eaf2` through `38f9b28` added a net 156 lines to
  `kafs_v7_fuse_create_in_direct_directory` (231 to 387 lines).
- Across the same range, `src/kafs_v7_fuse_write.c` changed by 198 additions
  and 42 deletions, and the inspection mount smoke test added 201 lines.
- Current static analysis reports the create function at 375 NLOC and
  cyclomatic complexity 103.
- The clone gate identifies five clone pairs inside
  `src/kafs_v7_fuse_write.c`. The repository-wide clone gate also has older
  debt, but these target-file findings were directly actionable.
- Recovery coverage was added and all 40 tests continued to pass, so the
  incident is maintainability and defect-risk growth rather than an observed
  runtime regression.

## Timeline

| Time (JST) | Event |
| --- | --- |
| 17:08 | `939eaf2` implemented one-to-two-block directory growth. |
| 17:52 | `f05cd5c` added a separate two-block append path instead of generalizing by block count. |
| 17:57 | `4025170` added a two-to-three-block growth variant. |
| 18:04 | `38f9b28` extended the special case to three-block append. |
| After 18:04 | The user questioned why each block count required a separate implementation; feature work stopped and RCA began. |

## Root Cause

The primary root cause was incorrect task decomposition. Recovery-sensitive
states were treated as separate production implementations when they should
have been separate test cases for one parameterized direct-directory mutation
algorithm.

The controlling invariant should have been:

1. Load `N` existing direct blocks.
2. Append within `N` blocks when capacity permits.
3. Otherwise allocate and stage `N + 1` blocks when the direct limit permits.
4. Atomically publish allocator state, `N` or `N + 1` inode references, parent
   size/block count, and the child inode.
5. Retire the old `N` blocks after the covering checkpoint.

The batch COW API already accepted a variable request count, so there was no
transaction-layer limitation requiring block-count-specific code.

## Five Whys

1. Why were separate block-count paths implemented?
   The handoff's next candidate was followed literally as the next smallest
   recovery slice.
2. Why was the common operation not extracted after the first slice?
   Planning focused on proving each crash boundary, not on identifying the
   stable `N`-block production invariant.
3. Why did the same pattern continue after the two-block append?
   Each successful `make check` was treated as sufficient completion evidence,
   and the next ticket was mechanically advanced by one block.
4. Why did review not stop the repetition?
   The required clone/static gates were not run for these commits, and no
   explicit design checkpoint was triggered when the next task differed only
   by cardinality.
5. Why was the direct limit not used to force a generic design?
   The direct-reference boundary was encoded as literals in multiple places:
   COW validation scans 12 direct slots, while retirement scanning also
   interprets slots 12 through 14 as indirect roots. No shared constant or
   create-path contract made that distinction mandatory.

## Contributing Factors

- A valid emphasis on crash recovery was incorrectly coupled to production
  specialization. Recovery cases should vary independently from implementation
  structure.
- Ticket and handoff updates reinforced one-block-at-a-time progress and made
  local completion appear equivalent to milestone progress.
- The inspection fixture helper was extended by representation count, which
  increased the apparent cost of changing to a table-driven test model.
- The function was already large, but no complexity budget or growth check was
  used as a stop condition.
- The repository-wide clone gate has pre-existing failures. That reduced its
  usefulness as a binary gate, but does not explain ignoring the new target-file
  clone findings.

## Detection And Missed Signals

The user detected the issue by questioning the repeated block-count-specific
tasks. It should have been detected before `f05cd5c`, when the proposed next
work was identical to the preceding COW flow except for request count.

Additional missed signals were:

- `kafs_v7_runtime_data_cow_batch_prepare` already supported variable request
  counts up to 64.
- `AGENTS.md` requires meaningful clones to be extracted and broad changes to
  run clone/static gates.
- The handoff repeatedly described the next task by incrementing a block count
  rather than by closing a capability boundary.
- Static analysis now flags the create function at complexity 103 and the clone
  report identifies repeated transaction/patch/retirement blocks in the same
  source file.

RCA-time gate evidence:

- `./scripts/clones.sh`: FAIL, 97 repository-wide clone groups and 2.69%
  duplicated lines against the 1.0% threshold; five groups include only
  `src/kafs_v7_fuse_write.c` locations.
- `./scripts/static-checks.sh`: completed with one non-passing step, the clone
  gate. Format and lint passed.
- `git diff --check`: passed for the RCA documentation changes.

## Corrective Actions

| Action | Status | Completion gate |
| --- | --- | --- |
| Stop block-count-specific feature slices | Complete | No further `N-to-N+1` ticket before refactor |
| Define shared direct and indirect reference-count constants | Pending | No create-path literal `12` or `15` for reference roles |
| Replace the 1/2/3-block paths with one `N`-block append/growth algorithm | Pending | Supports the full admitted direct range with one control flow |
| Extract payload load, batch planning/staging, inode patch, and retirement helpers | Pending | Target-file clone findings removed or explicitly justified |
| Convert directory fixtures to table-driven boundary cases | Pending | Cover inline, 1, 2, 3, direct-limit-minus-one, and direct-limit cases |
| Preserve fault recovery by equivalence class, not every cardinality | Pending | Publish/apply/checkpoint faults cover append, growth, and max-limit rejection |
| Run and report `clones.sh` and `static-checks.sh` for the corrective refactor | Pending | No new target-file clones; repository baseline failures reported separately |
| Re-review M8-B handoff after refactor | Pending | Next task is capability-based rather than count-based |

## Prevention Rules

1. When two consecutive planned tasks differ only by a numeric cardinality,
   stop and document why parameterization is impossible before implementing the
   second task.
2. Keep recovery test matrices independent from production branch structure.
3. Do not mark broad filesystem mutation work complete without clone/static
   results, even when the repository has known baseline failures.
4. Use named format constants for direct and indirect reference roles; do not
   infer them from the raw 60-byte inode field.
5. Treat rapidly growing function complexity as a design-review trigger, not
   only as cleanup work after feature completion.

## What Went Well

- Commits were small, local, and recoverable; the incident was detected before
  any remote ref included them.
- The added normal and interruption tests are reusable as refactor regression
  coverage.
- No test, fsck, or recovery failure indicated existing data corruption.

## Follow-up Decision

Do not continue with a three-to-four-block implementation. The next change must
first consolidate direct-directory create into the parameterized algorithm,
centralize the direct/indirect boundary, and retain representative recovery
coverage. History rewriting is not required for containment; the corrective
commit must make the branch's final implementation generic and auditable.
