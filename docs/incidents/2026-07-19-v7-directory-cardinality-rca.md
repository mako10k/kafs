# V7 directory block-cardinality specialization RCA

- Incident ID: `KAFS-INC-2026-07-19-01`
- Date: 2026-07-19
- Status: RCA revised; containment and task-start controls implemented;
  corrective product refactor pending
- Classification: development-quality and task-control incident
- Released or remote impact: none observed

## Executive Summary

The v7 create path was implemented as separate one-to-two-block growth,
two-block append, two-to-three-block growth, and three-block append changes.
The direct-directory transition was already expressible as one operation over
`N` existing direct blocks, so these were examples of one capability rather
than separate production capabilities.

The root cause was not the handoff wording and was not the absence of a final
code-quality check. Work could start without a current Definition of Ready: no
control required the executor to refresh checkout evidence, reconsider inherited
assumptions, and re-derive the task boundary and exit criteria before editing.
The handoff candidate was consequently used as both the starting input and the
definition of done.

The previous RCA incorrectly treated `939eaf2` as a reasonable prototype and
placed the incident start before `f05cd5c`. Evidence available before
`939eaf2` already included a variable-cardinality batch COW API, a generic
multi-block direct-write path, a twelve-direct-block boundary, and a generic
bounded direct-growth precedent. The incident therefore began when `939eaf2`
started without re-evaluating that evidence.

## Impact

- No released image, remote branch, or known filesystem data was affected. At
  RCA time, no remote ref contained the incident commits.
- Four commits from `939eaf2` through `38f9b28` added a net 156 lines to
  `kafs_v7_fuse_create_in_direct_directory` (231 to 387 lines).
- Across that range, `src/kafs_v7_fuse_write.c` changed by 198 additions and 42
  deletions, and the inspection mount smoke test added 201 lines.
- Static analysis reported the create function at 375 NLOC and cyclomatic
  complexity 103.
- The clone report identified five clone pairs within
  `src/kafs_v7_fuse_write.c`.
- All 40 tests passed. The observed impact is excess complexity, duplication,
  misleading progress records, and increased future defect risk rather than a
  demonstrated runtime or data-integrity failure.

## Timeline

| Time (JST) | Event |
| --- | --- |
| Before 17:08 | Existing code and tickets already described generic multi-block COW and bounded direct growth through twelve direct blocks. |
| 17:08 | `939eaf2` implemented one-to-two-block directory growth without a fresh task-start review. This is the first incident commit. |
| 17:52 | `f05cd5c` added a separate two-block append path. |
| 17:57 | `4025170` added a two-to-three-block growth variant. |
| 18:04 | `38f9b28` extended the specialization to three-block append. |
| After 18:04 | The user questioned the cardinality-specific sequence; feature work stopped. |
| After detection | `7ab682b` recorded an initial RCA, which was rejected as too shallow and was revised by this change. |

## Evidence Available At The First Incident Commit

The following evidence was present before `939eaf2` and should have changed the
task boundary:

- `kafs_v7_fuse_write_direct` calculated `block_count` from the request and
  passed that count through arrays and loops bounded by twelve direct slots.
- `kafs_v7_runtime_data_cow_batch_prepare` accepted a runtime
  `request_count`, with no two-block or three-block transaction semantic.
- `SDW-V7RT-T31` described one atomic multi-block direct overwrite capability
  through the twelve-slot direct boundary.
- `SDW-V7RT-T34` described bounded direct growth as one capability and used
  representative one- and two-block tests rather than separate production
  implementations per block count.
- The handoff's original branch and implementation checkpoint no longer
  identified the current checkout, while later sections contained accumulated
  updates. This made live reconciliation necessary before using its next-task
  text.

These facts establish that a generic design was discoverable from the checkout;
no new transaction primitive or format decision was required first.

## Causal Analysis

### Root Process Cause

The repository had no operational Task Start Gate for non-trivial
implementation. A handoff, backlog item, or delegated next task could transition
directly into editing without a recorded check of:

1. current branch, HEAD, worktree, and handoff freshness;
2. affected and analogous code, existing APIs, specifications, and tests;
3. inherited assumptions against current evidence;
4. states, variability dimensions, invariants, and semantic boundaries; and
5. exit criteria re-derived from that evidence.

`AGENTS.md` required evidence-based reporting and post-edit validation, but did
not require evidence refresh and reconsideration before editing. The custom
gatekeeper mentioned entry criteria, but normal implementation did not require
a start decision, custom-agent use required an explicit request, and its input
criteria did not have to be reconstructed from the checkout. The separate rule
requiring Gatekeeper approval for major work therefore had no always-available
invocation path. The task rules required ticket and milestone alignment only.

### Direct Cause

The executor treated the handoff's next candidate as the current implementation
scope and definition of done. It did not inspect the analogous generic direct
write/growth paths or reclassify block count as a variability dimension within
one state transition. Production code was therefore partitioned by the same
examples used to increment recovery coverage.

### Contributing Conditions

- Instructions to keep changes small, focused, and testable were applied to
  diff size instead of semantic capability boundaries.
- The orchestrator role asked for discrete tasks but did not first require a
  state/invariant model.
- The progress-manager role could recommend a next task from plans, tickets,
  and recent changes without marking that recommendation as unready or stale.
- The handoff accumulated current progress beneath an older checkpoint and
  presented next work without an explicit requirement to rebaseline.
- Repeated successful behavior tests reinforced the incorrect task boundary
  because recovery examples and production branches evolved together.

These conditions increased the likelihood of the failure but did not compel
it. The implementation remained responsible for reconciling them with current
code evidence before starting.

### Escape And Detection

The reviewed-scope WIP workflow was not followed for the incident commits, and
the required clone/static checks were not run. The commits therefore escaped
without capturing the growing same-file duplication and complexity.
`make check` demonstrated behavioral compatibility only; it was not evidence
that the implementation boundary was correct.

The aggregate `scripts/static-checks.sh` records failed steps but exits zero
after reporting them. CI runs clone detection separately and can still fail,
but the aggregate local command is not itself a blocking control. This weakens
escape detection and makes explicit result review necessary; it does not cause
the pre-implementation reasoning failure.

The user detected the incident by comparing the repeated tasks with the
expected generic capability. Clone and complexity analysis later quantified
the escaped structural impact. These are detection controls, not prevention of
the root cause.

## Correct State Model

Production behavior should be divided by semantic transition, not fixture
cardinality:

1. inline directory append: `inline -> inline`;
2. inline representation growth: `inline -> direct(1)`;
3. direct append: `direct(N) -> direct(N)`;
4. direct growth: `direct(N) -> direct(N + 1)` while `N` is below the direct
   limit; and
5. direct-limit rejection: `direct(DIRECT_LIMIT) -> ENOSPC`.

Recovery tests may select representative cardinalities within these transition
classes. Those test partitions do not create additional production states.

## Corrective And Preventive Actions

### Correction Of Escaped Impact

| Action | Status | Completion evidence |
| --- | --- | --- |
| Stop further block-count-specific slices | Complete | No three-to-four-block ticket is authorized |
| Replace the 1/2/3-block branches with one bounded `direct(N)` algorithm | Pending | One control flow covers the admitted direct range |
| Centralize direct and indirect reference-count constants | Pending | Reference roles no longer use unexplained `12`/`15` literals |
| Replace fixture-by-cardinality growth with transition-class tests | Pending | Inline, append, growth, limit, and recovery classes cover representative boundaries |

### Prevention At Task Start

| Action | Status | Completion evidence |
| --- | --- | --- |
| Add the Task Start Gate to `AGENTS.md` | Complete in this change | Editing is prohibited before a recorded `PASS` |
| Make the primary agent own the gate | Complete in this change | Start control does not depend on subagent availability |
| Require implementer, orchestrator, progress-manager, and gatekeeper roles to use the same contract | Complete in this change | Role definitions distinguish candidate, ready task, and start decision |
| Reconcile mandatory start control with optional subagent invocation | Complete in this change | Primary self-gate is mandatory; independent Gatekeeper review is conditional on permitted use |
| Define handoff next work as a candidate rather than a definition of done | Complete in this change | Handoff and ticket guidance require checkout reconciliation |
| Re-run the gate when evidence or scope changes materially | Complete in this change | `AGENTS.md` defines staleness-triggered re-evaluation |
| Select recovery waves from the accepted-goal capability dependency path | Superseded after failed effectiveness replay on 2026-07-21 | Pre-proposal PERT must calculate duration, slack, critical frontier, alternatives, and downstream unlock before a candidate is named |
| Restrict history use to intent-oriented BlameCheck | Complete in follow-up `AGENTS.md` rule | Authorship, age, and provenance cannot exclude or deprioritize findings |

The Task Start Record requires a baseline identity, current evidence, assumption
audit, state/invariant analysis, independently derived exit criteria and
non-goals, and a `PASS`, `REPLAN`, or `BLOCKED` decision. This remains a semantic
implementation-start gate; it does not select the next wave. Selection now
requires the pre-proposal PERT record in `docs/pert-task-selection.md`. A script
cannot substitute for examining current code and architecture.

### Detection And Correction After Start

| Action | Status | Completion evidence |
| --- | --- | --- |
| Preserve reviewed-scope review before final commits | Existing control, not followed | Reviewed units and validation are recorded before consolidation |
| Run clone/static checks for broad implementation | Existing control, not followed | New target-file findings are reviewed even when the repository baseline fails |
| Make repository-wide clone/complexity debt independently actionable | Pending separate work | Changed-scope regressions produce an unambiguous failing result |

These controls detect and correct impact that passes the start gate. They are
not presented as prevention of the root process cause.

## Effectiveness Test

The prevention is accepted only if it passes both directions of a retrospective
replay:

1. At the `65b73ca` checkout state, using the then-current handoff candidate,
   the gate must return `REPLAN` for a one-to-two-only production implementation
   because generic batch COW, generic direct write/growth, and the twelve-slot
   boundary are already observable.
2. The same gate must retain `inline -> direct(1)` as a separate transition
   because representation, allocation, retirement, and transaction behavior
   differ from `direct(N) -> direct(N + 1)`.

The evidence review performed for this revised RCA satisfies this retrospective
test: the first proposal is reclassified as `direct(N)` while the inline
representation transition remains distinct. Future Task Start effectiveness is
measured by the presence of a current record before the first edit. Task-order
effectiveness is measured separately: PERT must exist before the candidate is
named and must place the selected wave on the runnable critical or least-slack
frontier. Neither the number of rules nor final test results establishes those
controls.

### 2026-07-21 task-order effectiveness failure

The original follow-up control did not prevent a second form of local
optimization. After real-media qualification became externally blocked, T49
through T52 each had a Task Start Record, but the next candidate was normally
named at the preceding wave's closeout. The later gate therefore checked whether
the preselected candidate was coherent and safe instead of comparing it against
all current goal paths.

The selection criteria drifted from critical-path effect to local startability:
existing direct-COW and validator work received concrete, low-uncertainty scope,
while M8-C indirect mutation was represented mainly by its journal, allocator,
path-copy, and recovery cost. Successful validator slices then supplied the next
nearby validator candidate. The error became visible when T52 closeout proposed
direct-only T53 directory-graph validation while indirect remained repeatedly
excluded.

This replay refutes the claim that a current Task Start Record alone is an
effective task-order control. The corrective breakpoint is before candidate
generation: construct a capability-level PERT network, calculate expected
duration and slack, retain blocked critical nodes, and select from the runnable
critical frontier. Only after that selection may the Task Start Gate assess the
implementation boundary.

## Follow-up Decision

Do not continue cardinality-specific directory work. After this process/RCA
change is committed, run a fresh Task Start Gate for the corrective product
refactor. Its exit criteria must be derived from the then-current checkout and
must not be inherited solely from this RCA or the handoff.
