# V7 indirect lifecycle PERT and Task Start record (2026-07-21)

## Selection record

- Record ID/time: `KAFS-V7-PERT-20260721-INDIRECT-S1`, 2026-07-21 JST
- Resource assumption: one primary implementation stream; external hardware
  preparation does not consume that stream but remains a join blocker.
- Current branch and HEAD at selection:
  `feat/v7-runtime-admission-foundation`, `effa023`
- Worktree state at selection: clean; branch eight commits ahead of its tracked
  remote.
- Evidence freshness: current checkout, refreshed immediately before the Task
  Start Gate.
- Accepted end goal: a v7 production path whose deterministic recovery and
  fail-closed behavior cover the existing regular-file block-tree capability,
  followed by approved real-media wear/power-interruption qualification and
  migration/cutover evidence.
- Current capability position: inline and twelve-direct-block regular files
  were writable; direct/single/double/triple address calculation, walking, and
  retirement guards existed; indirect mutation did not.
- Change since the prior ordering: exact card/reader/power-cut identity remained
  externally blocked and the user deferred SD-card preparation. The tentative
  direct-adjacent directory-graph task was discarded before selection because
  it did not shorten the indirect-to-qualified-runtime path.

Duration unit is one agent-day. `TE = (O + 4M + P) / 6`. External calendar wait
is `UNKNOWN` rather than hidden in an implementation estimate.

| ID | Capability or mandatory outcome | Causal predecessors | O | M | P | TE | Confidence | State/blocker | Downstream unlock |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| C | Bounded direct v7 controlled write and software recovery |  | 0 | 0 | 0 | 0.00 | High | Complete | Indirect lifecycle work |
| S | Dense same-group single-indirect regular-file write/truncate/recovery | C | 2 | 4 | 7 | 4.17 | Medium | Runnable at selection | Multi-level indirect path-copy |
| D | Dense double/triple-indirect regular-file lifecycle | S | 4 | 8 | 14 | 8.33 | Low | Blocked by S at selection | Full regular-file block-tree surface |
| Q | Expanded software recovery, fsck, and non-destructive qualification | D | 2 | 4 | 7 | 4.17 | Medium | Blocked by D | Stable software candidate for media qualification |
| H | Exact card/reader/power-cut identity and digest-bound approval | C | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | Low | External blocker; explicitly deferred | Authorized real-media execution |
| R | Real-media recovery and wear qualification for the expanded surface | Q, H | 3 | 5 | 9 | 5.33 | Low | Blocked by Q and H | Bounded RC evidence |
| M | Migration/cutover evidence for the qualified destination runtime | R | 5 | 10 | 18 | 10.50 | Low | Blocked by R | Accepted production cutover decision |
| N | Whole-namespace graph/link-count validation | C | 2 | 4 | 7 | 4.17 | Low | Runnable but off this causal path | Separate corruption-admission closure |

```text
C -> S -> D -> Q --+
                    +-> R -> M
C -> H -------------+

C -> N   (separate correctness path; no current edge to R or M)
```

Because `H` has unknown external calendar duration, an exact end-to-end backward
pass would be false precision. The calculated software subnetwork to the `R`
join is:

| ID | ES | EF | LS | LF | Slack | Runnable at selection | Critical or near-critical |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| C | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | No | Critical |
| S | 0.00 | 4.17 | 0.00 | 4.17 | 0.00 | Yes | Critical |
| D | 4.17 | 12.50 | 4.17 | 12.50 | 0.00 | No | Critical |
| Q | 12.50 | 16.67 | 12.50 | 16.67 | 0.00 | No | Critical |
| H | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | No | External join blocker |
| R | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | 0.00 after join | No | Critical join |
| M | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | 0.00 after R | No | Critical |
| N | 0.00 | 4.17 | N/A | N/A | N/A | Yes | Off the accepted product path |

- Critical software path: `C -> S -> D -> Q -> R -> M`.
- Uncertainty-overlapping path: `C -> H -> R -> M` because the hardware wait is
  unbounded.
- External blocker retained in the graph: `H`; it was not replaced by a locally
  easy validator task.
- Runnable zero-slack frontier at selection: `S`.

| Ordering | Critical-path effect | Capability unlocked | Future rework or qualification debt | Evidence and uncertainty |
| --- | --- | --- | --- | --- |
| `S -> D -> Q`, then media join | Shortens the runnable software leg to `R` | Full regular-file block tree | One expanded qualification wave | Address/walk/retirement already existed; path-copy effort remained uncertain |
| Direct-only media qualification first | Can use hardware earlier if `H` clears | Direct-only RC evidence | Indirect expansion repeats software and physical recovery qualification | Hardware was not available and was explicitly deferred |
| `N` directory graph validation first | Does not shorten either join leg | Separate corruption rejection | Delays S and preserves the indirect gap | It was adjacent to T50-T52 but had no causal edge to R/M |

The earlier directory-graph preference was proposed before this network and is
discarded. `S` is not selected because it is easy: it is the only runnable
zero-slack predecessor of `D` and `Q`. The selection decision is `SELECT`.

## Task Start record for S / SDW-V7RT-T53

- Baseline: branch/HEAD/worktree recorded above; source is the fresh PERT
  selection, not the stale handoff ordering.
- Directly observed implementation evidence:
  - v7-owned direct COW already stages data outside the journal, then publishes
    bitmap/summary/inode after-images;
  - v7 address/walk and retirement code already understands single roots;
  - the shared read path resolves v7 physical references and walks existing
    indirect trees;
  - one COW batch accepts up to 64 staged blocks, while negotiated FUSE
    `max_write` is twelve filesystem blocks.
- Assumptions:
  - valid: staged data need no new journal record type; same-group allocation,
    dense files, full fsync, and post-checkpoint retirement remain required;
  - contradicted: direct-slot-only reachability proof and direct-only common
    inode validation are insufficient once a staged root owns staged data;
  - unknown at start: actual FUSE availability and the exact mutation count for
    a single-root recovery transaction.
- State/variability matrix: root absent/present; direct-to-single crossing,
  single overwrite, and contiguous growth; aligned and partial shrink;
  single-to-direct and zero transitions; one/multiple data blocks; journal
  publish, metadata apply, and checkpoint-copy interruption.
- Invariants: every staged block is reachable from the after-image; every
  retained block was reachable from the before-image; `inode.blocks` counts
  data plus the single root; root and requested data are COW-published in one
  transaction; retirement occurs only after publication; unused references are
  zero and allocated references resolve through recovered bitmaps.
- Exit criteria: normal FUSE write/read/full-fsync, single and boundary
  truncate, offline fsck, inspection remount, three interruption recoveries,
  counter/reference validation, and unchanged fail-closed behavior for holes,
  double/triple mutation, cross-group allocation, and indirect directories.
- Explicit non-goals: sparse files, double/triple mutation, indirect-directory
  creation/growth, cross-group transactions, repair, raw-device execution, and
  physical power interruption.
- Tooling: `clangd` was unavailable (`ENOENT`), so semantic LSP checks could not
  run; repository search, compiler diagnostics, and full reference enumeration
  are the fallback.
- Start decision: `PASS`. The slice closes one coherent lifecycle and is the
  selected runnable critical predecessor.

## Closeout refresh

### S closeout evidence

- Refresh time: 2026-07-21 JST, after the final working-tree validation.
- Dependency result: `S` is complete. Dense same-group single-indirect regular
  files now cover direct-to-single growth, overwrite, aligned/partial shrink,
  single-to-direct shrink, zero truncate, offline validation, inspection
  remount, and journal-publish/metadata-apply/checkpoint-copy recovery.
- Estimate result: the implementation closed within one 2026-07-21 execution
  wave. That wall-clock result is not converted into agent-days, so it does not
  justify shrinking the remaining low-confidence depth estimates.
- Validation result: `make check -j2` passed all 43 tests; format, lint, v7
  layout/runtime ownership, clone, and aggregate static gates passed. Active
  source remained 41 clones and 409 duplicated lines (0.84%), with no new
  clone. The non-destructive qualification passed 29/29 required results and
  verified 97 artifacts.
- Claims not closed: real-media, RC, controller-independent wear, sparse-file,
  cross-group, double/triple, and indirect-directory claims remain false.
- External state: exact card/reader/power-cut identity and approval are still
  unavailable and explicitly deferred, so `H` remains an unknown-duration join
  blocker.

### Rebuilt network after S

- Record ID/time: `KAFS-V7-PERT-20260721-INDIRECT-D2`, 2026-07-21 JST.
- Evidence baseline: current T53 working tree derived from `effa023`, after the
  final 43-test and repository-gate pass; estimates use the observed ownership
  boundaries in that tree.

The old `D` aggregate is split from fresh implementation evidence. A double
root introduces a second path-copy level and is an independently usable size
class; triple introduces another level only after that invariant is established.
This is a capability/dependency split, not a test-example or file split.

| ID | Capability or mandatory outcome | Causal predecessors | O | M | P | TE | Confidence | Current state/blocker | Downstream unlock |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| CS | Bounded direct and dense single-indirect controlled write/recovery |  | 0 | 0 | 0 | 0.00 | High | Complete through T53 | Multi-level path-copy |
| D2 | Dense same-group double-indirect regular-file lifecycle | CS | 3 | 6 | 11 | 6.33 | Low | Runnable | Triple-depth path-copy |
| D3 | Dense same-group triple-indirect regular-file lifecycle | D2 | 4 | 8 | 15 | 8.50 | Low | Blocked by D2 | Full regular-file block-tree surface |
| Q | Expanded software recovery, fsck, and non-destructive qualification | D3 | 2 | 4 | 7 | 4.17 | Medium | Blocked by D3 | Stable software candidate for media qualification |
| H | Exact card/reader/power-cut identity and digest-bound approval | CS | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | Low | External blocker; explicitly deferred | Authorized real-media execution |
| R | Real-media recovery and wear qualification for the expanded surface | Q, H | 3 | 5 | 9 | 5.33 | Low | Blocked by Q and H | Bounded RC evidence |
| M | Migration/cutover evidence for the qualified destination runtime | R | 5 | 10 | 18 | 10.50 | Low | Blocked by R | Accepted production cutover decision |
| N | Whole-namespace graph/link-count validation | CS | 2 | 4 | 7 | 4.17 | Low | Runnable but off this causal path | Separate corruption-admission closure |

```text
CS -> D2 -> D3 -> Q --+
                       +-> R -> M
CS -> H ---------------+

CS -> N   (separate correctness path; no current edge to R or M)
```

`H` still prevents a truthful end-to-end backward pass. The rebuilt software
subnetwork to the `R` join is:

| ID | ES | EF | LS | LF | Slack | Runnable now | Critical or near-critical |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| CS | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | No | Critical/complete |
| D2 | 0.00 | 6.33 | 0.00 | 6.33 | 0.00 | Yes | Critical |
| D3 | 6.33 | 14.83 | 6.33 | 14.83 | 0.00 | No | Critical |
| Q | 14.83 | 19.00 | 14.83 | 19.00 | 0.00 | No | Critical |
| H | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | No | External join blocker |
| R | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | 0.00 after join | No | Critical join |
| M | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | 0.00 after R | No | Critical |
| N | 0.00 | 4.17 | N/A | N/A | N/A | Yes | Off the accepted product path |

- Critical software path: `CS -> D2 -> D3 -> Q -> R -> M`.
- Uncertainty-overlapping path: `CS -> H -> R -> M`.
- Runnable zero-slack frontier: `D2`.
- Runnable off-path work: `N`; selecting it would consume the implementation
  stream without shortening either leg of the `R` join.

| Ordering | Critical-path effect | Capability unlocked | Future rework or qualification debt | Evidence and uncertainty |
| --- | --- | --- | --- | --- |
| `D2 -> D3 -> Q` | Shortens the runnable software leg to `R` | Full regular-file block tree | One final expanded qualification wave | Single-level path-copy is proven; additional depth remains low-confidence |
| `Q` immediately after single | Delays D2 by 4.17 | Stronger single-only evidence | Q must be repeated after D2/D3 | No hardware is available to consume the interim evidence |
| Combine D2 and D3 as one implementation wave | Same nominal path duration | Full tree in one wave | Less intermediate requalification | Larger review/recovery state space and no independently reviewed double boundary |
| `N` directory graph first | Delays D2 by 4.17 | Separate corruption rejection | Preserves the indirect gap | No current causal edge from N to R/M |

### Refreshed selection

- Selected runnable capability node: `D2`, dense same-group double-indirect
  regular-file lifecycle.
- Why it is on the frontier: it is the only runnable zero-slack software node
  that shortens the expanded-runtime leg to the `R` join.
- Predecessor closed by this wave: `CS`, including single-root path-copy,
  reachability proof, retirement, and recovery.
- Downstream nodes unlocked: `D3`, then `Q`.
- Opportunity cost versus the strongest alternative: doing `Q` now spends an
  estimated 4.17 agent-days on evidence that must be repeated after depth
  expansion; combining D2/D3 increases the first wave's review and recovery
  surface without shortening the calculated path.
- PERT decision: `SELECT D2`. A fresh Task Start Gate is still required before
  its implementation; this closeout selection is not implementation approval.

## D2 Task Start Gate

- Record ID/time: `KAFS-V7-START-20260721-INDIRECT-D2`, 2026-07-21 JST.
- Baseline identity: branch `feat/v7-runtime-admission-foundation`, HEAD
  `e79b6e8`; the worktree contained only ignored/generated test executables and
  no tracked edits when the gate ran.
- Current evidence: T53 is the HEAD ancestor; the focused block-tree,
  checkpoint-publication, FUSE-write, and inspection tests passed; the rebuilt
  PERT above still has `D2` as the only runnable zero-slack software frontier.
- Assumptions checked: format v7 remains a breaking ownership boundary;
  controlled write remains dense and same-group; hardware identity/approval is
  unavailable and deliberately remains external blocker `H`; no evidence
  invalidates the single-root path-copy/reachability/retirement contract.
- State space to cover: single-to-double crossing; overwrite and contiguous
  growth within double depth; double-child-table crossing; partial and aligned
  shrink; child-table pruning; double-to-single, double-to-direct, and zero;
  journal-publication, metadata-apply, and checkpoint-copy interruption.
- Required invariants: each data/leaf/root/inode transition publishes in one
  same-group transaction; every staged block is reachable from the after graph
  and every retained block from the before graph; `inode.blocks` equals data
  plus every owned index block; required references are allocated and unused
  references are zero; old data and index blocks retire only after publication.
- Exit criteria: low-level boundary/negative tests, actual FUSE
  full-fsync/readback/remount, detect-only fsck, three-point recovery, updated
  non-destructive qualification contract, and repository gates all pass.
- Explicit non-goals: triple-indirect mutation, sparse files, indirect
  directories, cross-group allocation/transactions, repair, real-media
  formatting, and physical power interruption.
- Tooling note: `compile_commands.json` and `lsp-cli` were present, but `clangd`
  was unavailable, so semantic LSP checks could not run.
- Decision: `PASS`. The selected `D2` capability and its exit criteria remain
  coherent on the current checkout; implementation may start without
  substituting a locally easier off-path task.

## D2 Closeout And Rebuilt PERT

- Record ID/time: `KAFS-V7-PERT-20260721-INDIRECT-D3`, 2026-07-21 JST.
- Evidence baseline: T54 working tree derived from `e79b6e8`, after focused
  low-level and actual-FUSE lifecycle/recovery tests, the 32/32 non-destructive
  qualification gate with 104 digest-checked artifacts, and the full Automake
  result of 42 PASS and one environment-limited SKIP.
- Closed capability: `D2` now proves dense same-group double-indirect regular
  files through write, shrink, lower-depth contraction, image validation,
  remount, and three interruption points. The explicit T54 non-goals remain
  open and do not silently become predecessors of the software path.
- External state: exact card/reader/power-cut identity and digest-bound approval
  remain unavailable, so `H` is still an unknown-duration join blocker.

The completed `D2` node is removed from the runnable frontier. No evidence from
this wave adds a predecessor between double and triple depth or creates a causal
edge from namespace validation `N` to the real-media join.

| ID | Capability or mandatory outcome | Causal predecessors | O | M | P | TE | Confidence | Current state/blocker | Downstream unlock |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| CS | Bounded direct and dense single-indirect controlled write/recovery |  | 0 | 0 | 0 | 0.00 | High | Complete through T53 | Complete predecessor |
| D2 | Dense same-group double-indirect regular-file lifecycle | CS | 0 | 0 | 0 | 0.00 | High | Complete through T54 | Triple-depth path-copy |
| D3 | Dense same-group triple-indirect regular-file lifecycle | D2 | 4 | 8 | 15 | 8.50 | Low | Runnable | Full regular-file block-tree surface |
| Q | Expanded software recovery, fsck, and non-destructive qualification | D3 | 2 | 4 | 7 | 4.17 | Medium | Blocked by D3 | Stable software candidate for media qualification |
| H | Exact card/reader/power-cut identity and digest-bound approval | CS | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | Low | External blocker; explicitly deferred | Authorized real-media execution |
| R | Real-media recovery and wear qualification for the expanded surface | Q, H | 3 | 5 | 9 | 5.33 | Low | Blocked by Q and H | Bounded RC evidence |
| M | Migration/cutover evidence for the qualified destination runtime | R | 5 | 10 | 18 | 10.50 | Low | Blocked by R | Accepted production cutover decision |
| N | Whole-namespace graph/link-count validation | CS | 2 | 4 | 7 | 4.17 | Low | Runnable but off this causal path | Separate corruption-admission closure |

```text
CS -> D2 (complete) -> D3 -> Q --+
                                   +-> R -> M
CS -> H ---------------------------+

CS -> N   (separate correctness path; no current edge to R or M)
```

`H` still prevents a truthful end-to-end backward pass. The rebuilt software
subnetwork from the T54 closeout to the `R` join is:

| ID | ES | EF | LS | LF | Slack | Runnable now | Critical or near-critical |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| D3 | 0.00 | 8.50 | 0.00 | 8.50 | 0.00 | Yes | Critical |
| Q | 8.50 | 12.67 | 8.50 | 12.67 | 0.00 | No | Critical |
| H | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | No | External join blocker |
| R | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | 0.00 after join | No | Critical join |
| M | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | 0.00 after R | No | Critical |
| N | 0.00 | 4.17 | N/A | N/A | N/A | Yes | Off the accepted product path |

- Critical software path: `D3 -> Q -> R -> M` after completed `CS -> D2`.
- Uncertainty-overlapping path: `CS -> H -> R -> M`.
- Runnable zero-slack frontier: `D3`.
- Runnable off-path work: `N`; choosing it would not shorten either leg of the
  `R` join.
- PERT decision: `SELECT D3`, the dense same-group triple-indirect
  regular-file lifecycle. `Q` must follow it so qualification is not repeated
  for an interim depth, while `N` remains a separate correctness path.
- Start condition: this is a closeout selection, not implementation approval.
  Re-run a fresh Task Start Gate on the post-T54 committed checkout before any
  D3 edit; retain triple state-space, invariants, exit criteria, and non-goals
  only if that gate returns `PASS`.

## D3 Task Start Gate

- Record ID/time: `KAFS-V7-START-20260721-INDIRECT-D3`, 2026-07-21 JST.
- Baseline identity: branch `feat/v7-runtime-admission-foundation`, HEAD
  `c62d426`; there were no tracked edits, while generated test executables
  remained untracked and outside the reviewed scope.
- PERT check: the post-T54 network above still has `D3` as the only runnable
  zero-slack software node. `Q` remains causally blocked by D3, namespace node
  `N` remains off-path, and hardware/approval node `H` remains an
  unknown-duration external join blocker.
- Foundation check: v7-owned address calculation, recursive walk, and
  retirement already accept three indirect levels; the T54 data/leaf/root/inode
  COW and publish-before-retire invariant is the immediate predecessor.
- Feasibility check: format v7 rejects a 512-byte block image but accepts and
  passes detect-only fsck at 1 KiB. With 256 references per index block, a
  one-group 256 MiB sparse image crosses double-to-triple near 67 MiB and the
  next triple middle-table boundary near 135 MiB, so both triple index
  boundaries can be exercised without weakening the production 4 KiB logic.
- State space to cover: double-to-triple crossing; overwrite and contiguous
  growth within triple depth; triple leaf and middle-table crossing; partial
  and aligned shrink; leaf and middle-table pruning; triple-to-double,
  triple-to-single, triple-to-direct, and zero; journal-publication,
  metadata-apply, and checkpoint-copy interruption.
- Required invariants: every touched data/leaf/middle/root/inode transition is
  one same-group transaction; `inode.blocks` equals data plus all owned index
  nodes; required references are allocated and unused entries at every level
  are zero; no staged block is unreachable from the after graph; old data and
  all replaced/pruned index nodes retire only after publication.
- Exit criteria: actual FUSE full-fsync/readback at both triple boundaries,
  lower-depth contraction, detect-only fsck and negative unused-reference
  validation, the three-point recovery matrix, updated non-destructive and
  DRAFT real-media qualification contracts, full Automake regression, and all
  repository gates pass.
- Explicit non-goals: sparse files, indirect-directory mutation, cross-group
  allocation/transactions, repair, real-media formatting, and physical power
  interruption.
- Tooling note: `lsp-cli` and `compile_commands.json` are present, but `clangd`
  remains unavailable; semantic LSP checks cannot run in this environment.
- Decision: `PASS`. D3 is feasible on the current checkout with its complete
  causal state space and without substituting a locally easier task.

## D3 And Q Closeout: Rebuilt PERT

- Record ID/time: `KAFS-V7-PERT-20260721-INDIRECT-POST-D3`, 2026-07-21 JST.
- Evidence baseline: T55 working tree derived from `c62d426`; actual-FUSE
  double/triple crossing, overwrite/shrink, every lower-depth contraction, the
  first triple middle-table boundary, detect-only fsck, unused-reference
  rejection, and all three recovery interruption points passed.
- Qualification closure: the non-destructive runner and gate require 37 results
  and passed 37/37 with 116 digest-checked artifacts. The DRAFT real-media
  contract now requires `regular_file_triple_indirect_lifecycle`; it does not
  authorize formatting or power interruption.
- Repository closure: the full Automake gate passed 42 tests with one
  environment-limited stress SKIP; format, lint, v7 ownership, clone, and
  aggregate static gates passed. The strict source clone result is 48 clones
  and 490 duplicated lines (0.97%), below the 1% limit.
- Closed capabilities: `D3` closes the final dense same-group regular-file
  block-tree depth. Its immediately dependent expanded software recovery,
  fsck, and non-destructive qualification node `Q` is also complete in T55.
- External state: exact card, reader/controller, isolated power-cut apparatus,
  cycle count, and digest-bound approval remain unavailable. Node `H` therefore
  remains an unknown-duration external predecessor of `R`.

No new evidence creates an edge from whole-namespace graph validation `N` to
real-media recovery `R` or migration/cutover `M`. Removing completed `D3` and
`Q` leaves the following network:

| ID | Capability or mandatory outcome | Causal predecessors | O | M | P | TE | Confidence | Current state/blocker | Downstream unlock |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| CS | Bounded direct and dense single-indirect controlled write/recovery |  | 0 | 0 | 0 | 0.00 | High | Complete through T53 | Complete predecessor |
| D2 | Dense same-group double-indirect regular-file lifecycle | CS | 0 | 0 | 0 | 0.00 | High | Complete through T54 | Complete predecessor |
| D3 | Dense same-group triple-indirect regular-file lifecycle | D2 | 0 | 0 | 0 | 0.00 | High | Complete through T55 | Complete regular-file block tree |
| Q | Expanded software recovery, fsck, and non-destructive qualification | D3 | 0 | 0 | 0 | 0.00 | High | Complete through T55 | Software leg at real-media join complete |
| H | Exact card/reader/power-cut identity and digest-bound approval | CS | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | Low | External blocker; explicitly deferred | Authorized real-media execution |
| R | Real-media recovery and wear qualification for the expanded surface | Q, H | 3 | 5 | 9 | 5.33 | Low | Blocked only by H | Bounded RC evidence |
| M | Migration/cutover evidence for the qualified destination runtime | R | 5 | 10 | 18 | 10.50 | Low | Blocked by R | Accepted production cutover decision |
| N | Whole-namespace graph/link-count validation | CS | 2 | 4 | 7 | 4.17 | Low | Runnable but off the accepted product path | Separate corruption-admission closure |

```text
CS -> D2 (complete) -> D3 (complete) -> Q (complete) --+
                                                       +-> R -> M
CS -> H ------------------------------------------------+

CS -> N   (separate correctness path; no current edge to R or M)
```

Because `H` is unknown, a truthful end-to-end backward pass still cannot assign
finite ES/EF/LS/LF values past the join:

| ID | ES | EF | LS | LF | Slack | Runnable now | Critical or near-critical |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| D3 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | Complete | Closed critical predecessor |
| Q | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | Complete | Closed critical predecessor |
| H | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | No | External join blocker |
| R | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | 0.00 after join | No | Critical join |
| M | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | 0.00 after R | No | Critical |
| N | 0.00 | 4.17 | N/A | N/A | N/A | Yes | Off the accepted product path |

- Completed software critical path: `CS -> D2 -> D3 -> Q`.
- Remaining uncertainty path: `CS -> H -> R -> M`.
- Runnable zero-slack software frontier: none.
- Runnable off-path work: `N`; selecting it would not shorten the remaining
  path and would repeat the local-ease ordering error this PERT gate prevents.
- PERT decision: `BLOCKED-ON-H` for the accepted product path. When SD-card
  preparation resumes, the next start gate is the digest-bound hardware and
  approval gate for `H`, followed by `R`. Until then, report the blocker rather
  than substituting `N` or inventing another local task.

## `perttool` transition record

- Record ID/time: `KAFS-PERTTOOL-20260721-POST-T55`, 2026-07-21 JST.
- Product baseline: branch `feat/v7-runtime-admission-foundation`, product HEAD
  `5d53faf`; the only tracked working-tree changes during this refresh were the
  task-selection control itself. Generated test executables remained untracked.
- Plan and tool: `plans/current.pert`, `perttool 0.1.0-dev.0`.
- Evidence freshness: the T55 closeout evidence immediately above; no product
  capability, hardware identity, or approval-state change was observed during
  the control transition.
- Accepted residual goal: approved real-media recovery/wear qualification,
  followed by migration and production-cutover evidence.
- Estimate confidence: `HARDWARE_APPROVAL` uses a provisional low-confidence
  1/1/2-day work estimate after unblocking; its external calendar wait remains
  unknown. The 3/5/9 and 5/10/18 estimates for the two downstream tasks retain
  the prior record's low confidence. Because this residual graph is serial,
  the provisional first estimate does not change task order.
- Command: `./scripts/pert-next-task.sh plans/current.pert`.
- `dag analyze` result: all three remaining tasks are on the precedence and
  resource critical path; the 17-day implementation-effort schedule is
  explicitly conditional on the external block being resolved and excludes its
  calendar wait.
- `dag next` result: `RUNNABLE NOW` is empty; `HARDWARE_APPROVAL` is
  `BLOCKED NOW` with total float zero; `REAL_MEDIA_QUALIFICATION` and
  `MIGRATION_CUTOVER_EVIDENCE` are `UPCOMING`.
- Alternative ordering: whole-namespace validation `N` remains owned by this
  narrative but has no causal edge to the accepted residual finish. It was not
  inserted into the `.pert` plan through a false finish edge.
- Decision: `BLOCKED`. This machine result replaces the hand-calculated table
  as the current selection authority and forbids an off-goal substitute.

## VHDX-backed host-recovery replan

- Record ID/time: `KAFS-PERTTOOL-20260721-VHDX-HARNESS`, 2026-07-21 JST.
- Trigger: disposable SD-card hardware remains unavailable, and the user offered
  the local Windows-host WSL VHDX as an interim interruption target.
- Accepted goal: unchanged. Approved real-media recovery/wear qualification and
  production cutover remain the finish; virtual-media evidence is an additional
  predecessor and never substitutes for physical-media authorization.
- Current baseline at selection: branch
  `feat/v7-runtime-admission-foundation`, HEAD `60fb869`; generated test
  executables were untracked, and `plans/current.pert` was the only tracked
  selection-control edit.
- Current backing evidence: the repository is on `/dev/sdd`, `ext4`, under
  WSL2. Windows registry discovery identifies Ubuntu's current backing as
  `ext4.vhdx`, allocated length `120680611840` bytes. No separate registered
  file named `WSLHOME.vhdx` was observed.
- Safety correction: the active Ubuntu VHDX is not disposable and cannot be
  raw-formatted or raw-written. The permissible target is a new regular-file
  KAFS image inside that ext4 filesystem. Windows may terminate and restart the
  Ubuntu distro only after a durable exact-boundary marker is observed.

The updated `plans/current.pert` adds these nodes without removing the external
hardware branch:

```text
SOFTWARE_QUALIFIED -> VHDX_HARNESS -> VHDX_HOST_RECOVERY_RUN --+
                                                               +-> REAL_MEDIA_QUALIFICATION
SOFTWARE_QUALIFIED -> HARDWARE_APPROVAL ------------------------+
```

`./scripts/pert-next-task.sh plans/current.pert` passed DSL validation and both
schedule analyses. Its `dag next` result classified `VHDX_HARNESS` as the only
`RUNNABLE NOW` task, with zero total float and both precedence/resource critical
status. `HARDWARE_APPROVAL` remained `BLOCKED NOW`; `VHDX_HOST_RECOVERY_RUN` was
`UPCOMING`. The decision was `SELECT VHDX_HARNESS`.

### VHDX_HARNESS Task Start Gate

- Baseline identity: branch and HEAD above; evidence refreshed before product
  edits.
- Directly observed state: the four deterministic process-fault points already
  exist in v7-owned journal/checkpoint/transaction code; the inspection smoke
  test already verifies recovery diagnostics, fsck, and persisted payloads for
  those points; the active ext4 filesystem is VHDX-backed.
- State space: journal publication, checkpoint copy, metadata apply, and journal
  reclaim; fresh versus stale state; marker absent/present; arm process active,
  WSL terminated, WSL restarted, verification incomplete/complete; host
  preflight versus explicit execute.
- Invariants: normal process-fault exit statuses remain unchanged; the marker is
  created exclusively and fsynced before `SIGSTOP`; only a dedicated new
  regular-file image is formatted; Windows records exact distro/VHDX identity
  and successful terminate/restart exits; recovery validates payload,
  diagnostic, full fsck, and dump artifacts; all real-media, physical-power,
  wear, and RC claims remain false.
- Exit criteria: exact-boundary pause contract, separate arm/verify modes,
  Linux ext4/same-filesystem preflight, native Windows discovery and explicit
  high-impact execution gate, digest-sealed evidence, proportional regression,
  and unchanged deterministic process-fault behavior.
- Explicit non-goals: executing `wsl.exe --terminate Ubuntu` from the active
  Codex/WSL session, raw access to the VHDX, physical power interruption,
  real-media/wear qualification, a background service, or changing IPC and
  security boundaries.
- Tooling: `lsp-cli` was present but `clangd` was unavailable, so compiler,
  exhaustive reference search, shell analysis, and focused behavioral tests are
  the semantic fallback.
- Decision: `PASS`. This slice prepares the selected critical harness only; the
  actual Windows-host interruption matrix remains a distinct next node.

### VHDX_HARNESS closeout and next frontier

- Closeout evidence: all four pause points reached a durable marker and stopped
  the KAFS process. A process-kill substitute then resumed each saved image and
  passed recovery diagnostic, persisted-payload validation, full fsck,
  `kafsdump`, and artifact digest verification. This validates the harness but
  is not Windows-host terminate/restart evidence.
- Safety evidence: native Windows and Linux preflight passed without termination;
  DrvFs, state outside the WSL home, stale state, and false real-media claims
  were rejected. The controller rediscovered the registered `ext4.vhdx` rather
  than relying on the tentative `WSLHOME.vhdx` name.
- Regression evidence: the complete inspection smoke passed. The full Automake
  run passed 42 tests including every affected v7 test; the unrelated existing
  `e2e_hotplug` case timed out once and passed its immediate isolated rerun.
  Format, lint, strict clone, complexity, both v7 ownership checks, shell
  analysis, Autotools regeneration, and warning-clean build passed. The new
  pause code adds no complexity warning.
- Claim boundary: actual `wsl.exe --terminate Ubuntu` was deliberately not run
  from the Codex session hosted inside that same distro. Real-media, physical
  power, controller-independent wear, and RC claims remain false.

`plans/current.pert` now marks `VHDX_HARNESS_READY` reached and removes the
completed harness task. A fresh
`./scripts/pert-next-task.sh plans/current.pert` run passed and classified
`VHDX_HOST_RECOVERY_RUN` as the only `RUNNABLE NOW` node, with total float zero
and both precedence/resource critical status. `HARDWARE_APPROVAL` remains a
blocked zero-slack critical join branch. The next-task decision is therefore
`SELECT VHDX_HOST_RECOVERY_RUN`, executable only from native Windows outside the
target Ubuntu distro.
