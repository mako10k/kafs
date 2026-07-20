# V7 RCA countermeasure detail and recovery waves

- Date: 2026-07-19
- Baseline: `e76a0d1` on `feat/v7-aligned-direct-overwrite`
- Related incident: `KAFS-INC-2026-07-19-01`
- Status: R1 closed; R2 active

## Scope And Sequence

The RCA is complete. This document details and orders its countermeasures; it
does not reopen the causal analysis or select a new product goal.

Use this sequence:

1. Keep the accepted v7 goal as an input.
2. Inventory the RCA actions and plausible same-cause impact without narrowing
   the search to the incident function or commits.
3. Use the minimum capability context needed to evaluate which countermeasures
   block or shorten the path to that goal. Do not require a comprehensive
   current-capability inventory first.
4. Derive recovery waves from those dependencies. Existing wave boundaries are
   candidates and may be merged, split, removed, or supplemented.
5. After each wave, re-evaluate the remaining RCA impact and its distance to the
   goal. Keep deferred findings owned with an explicit disposition.
6. After the necessary RCA countermeasures close, rebaseline current capability
   comprehensively and derive subsequent product work.

## Fixed Inputs

- The accepted v7 priorities remain fault tolerance and SD-card write
  distribution, with deterministic recovery or rejection as a hard constraint.
- The RCA established that block cardinality was incorrectly treated as a
  production-state boundary even though generic batch COW, generic multi-block
  direct write/growth, and the twelve-slot direct boundary were already present.
- The correct directory states are `inline -> inline`, `inline -> direct(1)`,
  `direct(N) -> direct(N)`, `direct(N) -> direct(N + 1)`, and direct-limit
  rejection.
- Recovery fixtures may select cardinalities, but fixture cardinality must not
  create production branches or implementation waves.
- Static analysis is detection evidence. It does not determine priority, but
  every finding remains owned and correctness, data-integrity, and durability
  findings constrain every applicable goal path.

## Countermeasure Inventory

| RCA action or same-cause evidence | Required disposition |
| --- | --- |
| 1/2/3-block branches in `kafs_v7_fuse_create_in_direct_directory` | Replace with the semantic transition model over bounded `N` |
| Literal 12/15 reference roles and arrays sized for 2/3 cases | Introduce named direct/indirect roles and size runtime work from the direct limit |
| Cardinality-named create recovery setup and repeated orchestration | Replace with transition-class tables and boundary representatives |
| Single versus batch paths in `kafs_v7_fuse_write_direct` | Use one cardinality-independent production path unless an invariant proves a distinct state |
| Single versus batch COW lifecycle duplication in `kafs_v7_runtime_transaction.c` | Provide one internal operation model; wrappers may remain only as adapters |
| Same-file clones in `kafs_shared_fuse_runtime.c` | Map to semantic states and close those touching the recovered direct-write/create path; retain the rest with a disposition |
| v6/v7 cross-format clones | Use BlameCheck and invariant comparison; preserve format ownership unless mechanics are genuinely neutral |
| Cppcheck layout-selector index-order warnings | Prove bounds and move the bound check before indexing |
| Remaining cppcheck warnings, including fsck `void *` arithmetic | Fix semantic/portability findings; record proof for any retained diagnostic |
| `scripts/static-checks.sh` exits zero after a failed constituent step | Preserve full report collection and return a failing aggregate status |
| Source clone gate at 2.69% against 1% | Remove semantic duplication and justify only true ownership or boilerplate exceptions |
| Informational test clones at 18.49% | Consolidate after production transitions are fixed so tests do not dictate abstractions |

No item is classified as unrelated. Deferred means owned and scheduled after a
closer dependency, not exempt.

## Dependency And Goal-Proximity Analysis

The nearest RCA obstruction to the accepted v7 goal is not one function's CCN.
It is the absence of a single cardinality-independent direct COW mutation
capability shared by the already enabled direct write/growth and create paths.
That obstruction spans transaction mechanics, FUSE adapters, inode reference
roles, and recovery evidence. Treating those as separate cleanup waves would
repeat the local file-oriented decomposition that the RCA rejects.

```text
cardinality-independent direct COW operation model
  -> generic direct write and directory mutation adapters
  -> semantic-transition recovery matrix
  -> enabled-path correctness/durability closure
  -> remaining RCA finding dispositions
  -> comprehensive capability rebaseline and next product plan
```

The aggregate static exit status improves detection but does not enable the
generic mutation capability, so it must not precede the primary correction.
Conversely, correctness or durability evidence found in the underlying COW,
journal, checkpoint, allocator, or retirement path is pulled into the primary
wave because the recovered capability cannot close without it.

## Re-Derived Recovery Waves

### R1: Cardinality-independent direct mutation

This wave merges the previously separated production refactor, adjacent
single/batch review, named-boundary cleanup, and recovery-test rewrite because
they close one capability and must agree on one state model.

Scope:

- define named direct and indirect inode-reference roles;
- consolidate single and batch COW lifecycle mechanics behind one internal
  operation model capable of `N >= 1`;
- route regular-file direct write/growth and direct-directory append/growth
  through cardinality-independent adapters;
- preserve `inline -> direct(1)` as a separate representation transition;
- replace cardinality-driven create tests with transition-class tables covering
  representative interior and boundary values;
- inspect and fix any enabled-path correctness or durability finding exposed by
  this consolidation.

Exit criteria:

1. One production control flow covers every admitted direct `N`; no 1/2/3-block
   branch or 2/3-sized production array remains.
2. Single-item APIs, if retained, are adapters to the common lifecycle rather
   than separate prepare/publish/finish implementations.
3. Direct-limit rejection publishes no transaction and changes no allocator,
   inode, directory, counter, or retirement state.
4. Inline representation growth remains separate and has explicit allocation,
   publication, and retirement invariants.
5. Transition-class recovery tests cover inline append, inline growth, direct
   append, direct growth, and direct-limit rejection at representative minimum,
   interior, limit-minus-one, and limit values.
6. The full interruption matrix converges to one transaction generation and
   passes offline fsck/readback.
7. The RCA effectiveness replay rejects a one-to-two-only task boundary and
   retains inline-to-direct as a valid distinct transition.

Downstream unlock: the direct write/create surface has one evidence-backed
semantic model, allowing remaining findings to be assessed against a stable
implementation rather than against cardinality-specific branches.

### R2: Recovery-surface structural and control closure

This wave combines the findings that must be resolved before the RCA recovery
can be considered operationally enforced, but that do not define the R1
production state model.

Scope:

- disposition same-cause clones in the shared FUSE runtime and format-owned
  v6/v7 surfaces, extracting only invariant-preserving neutral mechanics;
- close remaining cppcheck semantic and portability findings;
- make aggregate static-check failure machine-detectable while retaining all
  reports;
- reduce or explicitly justify source clones under the repository policy;
- consolidate test orchestration only after production transitions are stable.

Exit criteria:

1. Every repository-wide static finding has an owner, evidence, and disposition.
2. No enabled-path correctness, data-integrity, or durability finding remains
   deferred.
3. A deliberately failing constituent static step makes the aggregate command
   nonzero after all reports are produced.
4. Source clone policy passes or every retained exception is narrowly justified
   by an accepted ownership/invariant boundary.
5. Full build, tests, format, lint, ownership checks, clone/complexity checks,
   cppcheck, and Git checks have explicit results.

Downstream unlock: RCA countermeasures and their detection controls are closed
well enough to perform a clean capability rebaseline.

## R1 Closeout Evidence

R1 closed on 2026-07-19 at `13cc9a0` with the following current-checkout
evidence:

- `kafs_v7_fuse_write_direct` and direct-directory create use batch COW for
  every admitted direct cardinality; no 1/2/3-block production branch remains.
- The retained single-item COW API is an adapter over the batch lifecycle and is
  used only where one-block representation semantics are explicit.
- Direct inode roles and work arrays use
  `KAFS_V7_INODE_DIRECT_REFERENCE_COUNT`; indirect slots are rejected before
  direct mutation.
- The normal-path transition table covers direct append, growth, limit-minus-one,
  limit append, and full-limit rejection. The rejection preserves the parent
  inode and allocator counters.
- The interruption table crosses inline/direct append/growth transitions with
  journal-publish, metadata-apply, and checkpoint-copy faults, including
  limit-minus-one and limit representatives. Recovery readback and offline fsck
  pass.
- Boundary testing exposed and corrected batch allocation accounting across
  bitmap words; free-block deltas are now emitted per bitmap patch.
- The effectiveness replay rejects a one-to-two-only implementation boundary:
  fixture cardinality is table data, while production control flow is driven by
  semantic representation transitions.

The R2 dependency graph starts with aggregate static exit status because every
subsequent static remediation relies on a trustworthy machine-detectable gate.
Clone and warning counts remain comparison evidence, not the ordering rule.

R2 clone-scope evidence was first refreshed after the neutral mount-option
filtering extraction. Format v6 is closed and frozen, retained only for
existing experimental tests and images, so `src/kafs_v6*` is not an active
clone remediation surface. It remains covered by build, tests, lint,
complexity, and cppcheck. With only that accepted ownership boundary excluded,
that checkpoint reported 41 clones / 408 duplicated lines / 0.89%.

Moving descriptor validation from the excluded v6 header into the active
neutral `kafs_descriptor_layout.h` later changed the measured population to 49
clones / 799 duplicated lines / 1.67%. The increase exposed a retained
`kafs_descriptor_*_wire` implementation that had no callers after v7 adopted
its owned raw-layout implementation. Removing that unreachable 432-line path,
without changing the population or threshold, produces 41 clones / 409
duplicated lines / 0.86% and passes the unchanged 1% gate. The remaining active
production, v7, and neutral helper clones remain owned by R2.

## Deferred Until RCA Countermeasure Closure

Do not use this document to choose among M7, M8-C, M9, or M10. After R1 and R2,
re-read current code, tests, specifications, and accepted decisions, establish
the actual capability position, and derive the next product plan through a new
Task Start and Goal And Critical Path Gate.

## Local-Optimum Check

- R1 is capability-shaped, not file-shaped: it joins the operation model,
  adapters, boundaries, and recovery proof that must change together.
- R2 does not precede R1 merely because its scripts or warnings are smaller.
- R1 may pull a finding forward only when it blocks the same mutation capability
  or its correctness/durability proof.
- At each closeout, re-evaluate the remaining RCA inventory. Split or merge the
  next work if current evidence changes the dependency structure.
