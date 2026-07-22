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

## VHDX host-recovery scheduling deferral

- Record ID/time: `KAFS-PERTTOOL-20260722-VHDX-DEFER`, 2026-07-22 10:20 JST.
- Baseline: branch `feat/v7-runtime-admission-foundation`, HEAD `1d77098`;
  upstream matched HEAD, tracked worktree state was clean, and the known test
  executables remained untracked build outputs.
- Trigger: the user confirmed that the active Ubuntu distro runs other tasks,
  making `wsl.exe --terminate Ubuntu` and `wsl.exe --shutdown` unsuitable until
  the user identifies a safe maintenance window.
- Accepted goal and capability position: unchanged. The VHDX harness remains
  ready, host-level terminate/restart evidence remains incomplete, and both the
  virtual recovery and physical approval branches remain mandatory predecessors
  of real-media qualification and production-cutover evidence.
- Dependency and estimate treatment: no edge or O/M/P estimate changed. The
  unknown external wait for a maintenance window is excluded from the existing
  low-confidence 1/1/2-day implementation-effort estimate.
- Plan change: `VHDX_HOST_RECOVERY_RUN` is now explicitly blocked until the user
  resumes it. It is neither completed nor removed from the network.
- Plan and machine result: `plans/current.pert` checked by
  `./scripts/pert-next-task.sh plans/current.pert` with
  `perttool 0.1.0-alpha.1` passes validation. The two precedence critical paths
  are `HARDWARE_APPROVAL -> HARDWARE_APPROVAL_REQUIRED ->
  REAL_MEDIA_QUALIFICATION -> MIGRATION_CUTOVER_EVIDENCE` and
  `VHDX_HOST_RECOVERY_RUN -> VHDX_RECOVERY_REQUIRED ->
  REAL_MEDIA_QUALIFICATION -> MIGRATION_CUTOVER_EVIDENCE`. The conditional
  resource critical path is `HARDWARE_APPROVAL -> VHDX_HOST_RECOVERY_RUN ->
  REAL_MEDIA_QUALIFICATION -> MIGRATION_CUTOVER_EVIDENCE` under capacity one.
  `ACTIVE` and `RUNNABLE NOW` are empty; both join predecessors are `BLOCKED
  NOW`; and the downstream tasks are `UPCOMING`.
- Alternative ordering: whole-namespace validation and other locally available
  work still have no causal edge to the accepted finish. Selecting one would
  consume capacity without closing either blocked join predecessor and would
  leave the same qualification work for later.
- Decision: `BLOCKED`. Do not substitute an off-goal task. When the user names a
  safe window, rediscover host/VHDX identity, rerun the Task Start Gate and
  `perttool`, and only then consider the four-point matrix.

## Blocker-decomposition replan

- Record ID/time: `KAFS-PERTTOOL-20260722-BLOCKER-DECOMPOSITION`,
  2026-07-22 10:36 JST.
- Baseline: branch `feat/v7-runtime-admission-foundation`, HEAD `1d77098`;
  upstream matched HEAD. The tracked worktree contained only the preceding
  planning/handoff updates, and the known generated test executables remained
  untracked.
- Plan and tool: `plans/current.pert`, `perttool 0.1.0-alpha.1`.
- Accepted goal: unchanged. The finish remains accepted evidence for a
  qualified v7 migration and production-cutover decision.
- Trigger: both disruptive execution predecessors were blocked, but the prior
  residual plan bundled evidence validation into those executions and bundled
  destination creation with the final migration/cutover wave. The resulting
  empty runnable frontier therefore did not represent all currently observed
  goal-path capabilities.

### Refreshed evidence and decomposition

Directly observed gaps are:

- the VHDX harness emits per-fault manifests and hashes, but no read-only gate
  proves that one run contains the exact four faults with consistent host,
  distro, VHDX, claim, controller, fsck, dump, payload, diagnostic, and digest
  evidence;
- the real-media approval gate validates only the matrix and approval. No
  versioned execution-artifact schema or independently signed review-decision
  gate exists;
- T29 creates and validates a v7 migration destination, but does not rehearse
  v5 data copy, interruption/resume, rollback, or namespace/payload comparison.

Those gaps are registered as goal-relevant capabilities, not as files or test
fixtures:

| PERT ID | Ticket | Capability | O | M | P | TE | Confidence | Current classification |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| `VHDX_EVIDENCE_AUDIT` | T57 | Read-only four-point VHDX evidence audit | 1 | 2 | 4 | 2.167 | Medium | `READY / WAITING RESOURCE` |
| `REAL_MEDIA_EVIDENCE_CONTRACT` | T58 | Real-media artifact and independent-review validation | 2 | 4 | 7 | 4.167 | Low | `RUNNABLE NOW` |
| `V5_V7_MIGRATION_REHEARSAL` | T59 | v5-to-v7 data migration, resume, and rollback rehearsal | 3 | 6 | 10 | 6.167 | Low | `READY / WAITING RESOURCE` |
| `REAL_MEDIA_EXECUTION` | existing physical run | Approved destructive evidence capture | 2 | 4 | 7 | 4.167 | Low | `UPCOMING` |
| `INDEPENDENT_REAL_MEDIA_REVIEW` | existing approval contract | Independent bounded-evidence decision | 1 | 1 | 2 | 1.167 | Medium | `UPCOMING` |
| `CUTOVER_EVIDENCE_DECISION` | existing migration/cutover outcome | Final qualified cutover evidence | 2 | 4 | 8 | 4.333 | Low | `UPCOMING` |

External calendar waits are excluded from O/M/P. `HARDWARE_APPROVAL` and
`VHDX_HOST_RECOVERY_RUN` retain low-confidence 1/1/2-day post-unblock effort
estimates and explicit blocked reasons. `PRIMARY_STREAM` capacity is one;
`INDEPENDENT_REVIEW_STREAM` capacity is one and may be used only by a reviewer
separate from the execution operator.

### Machine result and ordering decision

`./scripts/pert-next-task.sh plans/current.pert` passed `dsl check`, precedence
and resource scheduling, and `dag next` in order.

- Precedence makespan: 13.833 implementation days, conditional on external
  blocks resolving.
- Precedence critical path:
  `REAL_MEDIA_EVIDENCE_CONTRACT -> REAL_MEDIA_CONTRACT_REQUIRED ->
  REAL_MEDIA_EXECUTION -> INDEPENDENT_REAL_MEDIA_REVIEW ->
  QUALIFIED_MEDIA_REQUIRED -> CUTOVER_EVIDENCE_DECISION`.
- Resource makespan: 23.333 implementation days under primary capacity one.
- Resource critical path:
  `REAL_MEDIA_EVIDENCE_CONTRACT -> VHDX_EVIDENCE_AUDIT ->
  HARDWARE_APPROVAL -> VHDX_HOST_RECOVERY_RUN -> REAL_MEDIA_EXECUTION ->
  V5_V7_MIGRATION_REHEARSAL -> CUTOVER_EVIDENCE_DECISION`.
- `ACTIVE`: none.
- `RUNNABLE NOW`: `REAL_MEDIA_EVIDENCE_CONTRACT`, total float 0 days,
  precedence and resource critical.
- `READY / WAITING RESOURCE`: `VHDX_EVIDENCE_AUDIT`, total float 2 days; and
  `V5_V7_MIGRATION_REHEARSAL`, total float 3.333 days. Both wait because the
  selected node consumes the only primary stream.
- `BLOCKED NOW`: `HARDWARE_APPROVAL` and `VHDX_HOST_RECOVERY_RUN`, each with
  total float 3 days. The schedule is conditional on both blocks resolving at
  time zero and is not a calendar forecast.
- `UPCOMING`: `REAL_MEDIA_EXECUTION`, `INDEPENDENT_REAL_MEDIA_REVIEW`, and
  `CUTOVER_EVIDENCE_DECISION`.

Credible alternative orderings were compared as follows:

| Ordering | Immediate effect | Opportunity cost and later debt |
| --- | --- | --- |
| T58 first | Closes the only runnable zero-slack contract before destructive evidence can be collected | Selected; execution evidence will not need a retrofitted schema or review boundary |
| T57 first | Prepares audit of the deferred VHDX capture | Delays zero-slack T58 by 2.167 days and leaves destructive evidence collection without its contract |
| T59 first | Advances the migration leg without hardware | Delays zero-slack T58 by 6.167 days; a rehearsed destination still cannot be qualified or accepted |
| Whole-namespace validation first | Closes a separately owned correctness gap | Has no current edge to the accepted finish and does not reduce either external blocker |

The old `RUNNABLE NOW`-empty conclusion depended on the coarse bundled model
and is invalidated as the current selection result. It remains valid only as a
historical statement about that prior model. The refreshed decision is
`SELECT REAL_MEDIA_EVIDENCE_CONTRACT` (T58). T57 and T59 are registered and
ready but not selected while primary capacity is occupied.

### Task Start Record for replan registration

- Evidence baseline: branch/HEAD/worktree above; affected scripts, approval and
  qualification contracts, migration destination behavior, tests, and current
  handoffs were read before changing the plan.
- Assumptions: the VHDX and hardware windows remain blocked; independent
  evidence validation and disposable file-image migration rehearsal do not
  require either window; whether their detailed implementation boundaries pass
  remains unknown until their own start gates.
- Semantic boundaries: evidence validation never performs the event it audits;
  authorization, execution, and independent review remain distinct states;
  migration rehearsal may use disposable file images but cannot assert physical
  qualification or production cutover.
- Exit criteria for this registration wave: T57-T59 have owned ticket records,
  estimates, causal edges, resource classifications, non-goals, and matching
  handoff text; the repository plan validates and selects T58 mechanically.
- Non-goals: implementing T57-T59, opening a device, formatting or mounting
  media, terminating WSL, claiming VHDX/real-media qualification, or selecting
  off-path namespace work.
- Start decision: `PASS` for plan reconstruction and task registration only.
  T58 implementation still requires a fresh Task Start Gate on its actual
  baseline.

## Post-T58 closeout and rebuilt PERT (2026-07-22)

- Record ID: `KAFS-PERTTOOL-20260722-POST-T58`
- Calculation time: 2026-07-22 11:27 JST
- Baseline: branch `feat/v7-runtime-admission-foundation`, start HEAD
  `173ef72`, plus the reviewed T58 working-tree unit
- Accepted goal: unchanged; produce qualified v7 migration and production
  cutover evidence without collapsing hardware, VHDX, or review boundaries

### Closed capability and current evidence

T58 added versioned real-media evidence and independent-review records plus a
validate-only gate. The gate binds the exact ready matrix and approval at the
run start time, checks before/after apparatus identity, requires the complete
sample/workload/boundary/cycle product, verifies relative artifact sizes and
SHA-256 values, separates operator and reviewer identities, and permits
`ACCEPT` only when all results are `PASS` and all review checks are true. Claims
remain false. No device, mount, format, power, VHDX, or WSL operation occurred.

Direct and focused approval/evidence/qualification regression passed. Full
`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` passed 43 tests with only the
environment-limited `stress_fs` FUSE skip. Format, lint, clone/static checks,
build, and `make dist` passed; the strict source clone baseline remained 48
clones, 490 duplicated lines, and 0.97%.

The plan marks `REAL_MEDIA_EVIDENCE_CONTRACT_READY` reached and removes its
completed task. The former reached `SOFTWARE_QUALIFIED` root was also removed
because, after its only outgoing task closed, it was disconnected from the
accepted finish and made the residual DAG invalid. This changes no capability
claim; the closed evidence-contract milestone remains the explicit predecessor
of real-media execution.

### Machine result

`./scripts/pert-next-task.sh plans/current.pert` again passed `dsl check`,
`dag analyze --schedule both`, and `dag next`, in that order. The residual plan
contains 14 milestones, 7 tasks, 7 gates, and 2 resources.

- Precedence makespan: 11.833 implementation days, conditional on both external
  blockers resolving.
- Precedence critical path:
  `VHDX_EVIDENCE_AUDIT -> VHDX_AUDIT_REQUIRED ->
  VHDX_QUALIFICATION_REQUIRED -> REAL_MEDIA_EXECUTION ->
  INDEPENDENT_REAL_MEDIA_REVIEW -> QUALIFIED_MEDIA_REQUIRED ->
  CUTOVER_EVIDENCE_DECISION`.
- Resource makespan: 19.167 implementation days under primary capacity one.
- Resource critical path:
  `VHDX_EVIDENCE_AUDIT -> HARDWARE_APPROVAL ->
  VHDX_HOST_RECOVERY_RUN -> REAL_MEDIA_EXECUTION ->
  V5_V7_MIGRATION_REHEARSAL -> CUTOVER_EVIDENCE_DECISION`.

| Classification | Node | TE | ES/EF | LS/LF | TF | Capability unlocked or wait reason |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `RUNNABLE NOW` | T57 `VHDX_EVIDENCE_AUDIT` | 2.167d | 0d/2.167d | 0d/2.167d | 0d | Closes the offline audit predecessor without capture or WSL interruption |
| `READY / WAITING RESOURCE` | T59 `V5_V7_MIGRATION_REHEARSAL` | 6.167d | 0d/6.167d | 1.333d/7.5d | 1.333d | T57 occupies the only primary stream |
| `BLOCKED NOW` | `HARDWARE_APPROVAL` | 1.167d | 0d/1.167d | 1d/2.167d | 1d | Exact apparatus and digest-bound approval remain unavailable |
| `BLOCKED NOW` | `VHDX_HOST_RECOVERY_RUN` | 1.167d | 0d/1.167d | 1d/2.167d | 1d | The user has not supplied a safe terminate/restart window |
| `UPCOMING` | `REAL_MEDIA_EXECUTION` | 4.167d | 2.167d/6.333d | 2.167d/6.333d | 0d | Waits for hardware approval and audited VHDX capture |
| `UPCOMING` | `INDEPENDENT_REAL_MEDIA_REVIEW` | 1.167d | 6.333d/7.5d | 6.333d/7.5d | 0d | Waits for real-media evidence capture |
| `UPCOMING` | `CUTOVER_EVIDENCE_DECISION` | 4.333d | 7.5d/11.833d | 7.5d/11.833d | 0d | Waits for qualified media and migration rehearsal |

The resource schedule assumes blocked work becomes available at time zero and
is not a calendar forecast. `ACTIVE` remains empty.

### Ordering decision

| Ordering | Immediate effect | Opportunity cost and qualification debt |
| --- | --- | --- |
| T57 first | Closes the only runnable zero-slack audit predecessor without any disruptive operation | Selected; future host capture can be judged without rerunning it merely to inspect completeness |
| T59 first | Advances disposable-image migration rehearsal | Delays T57 by 6.167 days, exceeds T59's 1.333-day float, and leaves the zero-slack VHDX audit debt open |
| Wait for VHDX or hardware | Performs no current capability work | Both windows remain external blockers and no shorter critical predecessor closes |
| Whole-namespace validation first | Closes a separately owned correctness gap | It still has no causal edge to the accepted finish and consumes the primary stream |

The closeout decision is `SELECT VHDX_EVIDENCE_AUDIT` (T57). Selection permits
only a fresh Task Start Gate and the read-only audit implementation. It does
not authorize VHDX write/mount/raw access, `wsl.exe --terminate` or
`--shutdown`, host capture, real-media access, or a qualification claim.

## Post-T57 closeout and rebuilt PERT (2026-07-22)

- Record ID: `KAFS-PERTTOOL-20260722-POST-T57`
- Calculation time: 2026-07-22 12:08 JST
- Baseline: branch `feat/v7-runtime-admission-foundation`, start HEAD
  `17d8820`, plus the reviewed T57 working-tree unit
- Accepted goal: unchanged; produce qualified v7 migration and production
  cutover evidence without collapsing hardware, VHDX, migration, or review
  boundaries

### Closed capability and current evidence

T57 added a validate-only aggregate gate over the exact current T56 producer
schemas. It binds a controller-shaped run ID to all four durability points,
checks stable distro/VHDX/WSL filesystem/Git identity and non-overlapping host
timestamps, validates recovery/full-fsck/kafsdump/payload facts and the clean
format-v7 dump, and requires the SHA-256 inventory to cover every retained
top-level regular file. Process-kill substitute evidence and every tested
missing, drifted, tampered, incomplete, or claim-escalated state fail closed.

Focused T57/inspection regression passed. Full
`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` passed 44 tests;
`min_git_hooks` was the only environment-limited FUSE skip. Format, lint,
clone/static checks, build, and `make dist` passed; the strict source clone
baseline remained 48 clones, 490 duplicated lines, and 0.97%. No PowerShell,
VHDX, mount, image-write, device, or WSL lifecycle operation occurred.

The plan marks `VHDX_EVIDENCE_AUDIT_READY` reached and removes the completed
implementation task. It does not mark `VHDX_HOST_RECOVERY_CAPTURED` or
`VHDX_HOST_RECOVERY_QUALIFIED` reached because no actual four-point host run was
performed.

### Machine result

`./scripts/pert-next-task.sh plans/current.pert` passed `dsl check`,
`dag analyze --schedule both`, and `dag next`, in that order. The residual plan
contains 14 milestones, 6 tasks, 7 gates, and 2 resources.

- Precedence makespan: 10.833 implementation days, conditional on both external
  blockers resolving.
- There are two zero-slack precedence paths into `REAL_MEDIA_EXECUTION`: one
  through `HARDWARE_APPROVAL -> HARDWARE_APPROVAL_REQUIRED`, and one through
  `VHDX_HOST_RECOVERY_RUN -> VHDX_CAPTURE_REQUIRED ->
  VHDX_QUALIFICATION_REQUIRED`. Both continue through
  `INDEPENDENT_REAL_MEDIA_REVIEW -> QUALIFIED_MEDIA_REQUIRED ->
  CUTOVER_EVIDENCE_DECISION`.
- Resource makespan: 17 implementation days under primary capacity one.
- Resource critical path:
  `HARDWARE_APPROVAL -> VHDX_HOST_RECOVERY_RUN -> REAL_MEDIA_EXECUTION ->
  V5_V7_MIGRATION_REHEARSAL -> CUTOVER_EVIDENCE_DECISION`.

| Classification | Node | TE | ES/EF | LS/LF | TF | Capability unlocked or wait reason |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `RUNNABLE NOW` | T59 `V5_V7_MIGRATION_REHEARSAL` | 6.167d | 0d/6.167d | 0.333d/6.5d | 0.333d | Closes disposable-image migration, resume, and rollback evidence |
| `BLOCKED NOW` | `HARDWARE_APPROVAL` | 1.167d | 0d/1.167d | 0d/1.167d | 0d | Exact apparatus and digest-bound approval remain unavailable |
| `BLOCKED NOW` | `VHDX_HOST_RECOVERY_RUN` | 1.167d | 0d/1.167d | 0d/1.167d | 0d | The user has not supplied a safe terminate/restart window |
| `UPCOMING` | `REAL_MEDIA_EXECUTION` | 4.167d | 1.167d/5.333d | 1.167d/5.333d | 0d | Waits for both blocked predecessors |
| `UPCOMING` | `INDEPENDENT_REAL_MEDIA_REVIEW` | 1.167d | 5.333d/6.5d | 5.333d/6.5d | 0d | Waits for real-media evidence capture |
| `UPCOMING` | `CUTOVER_EVIDENCE_DECISION` | 4.333d | 6.5d/10.833d | 6.5d/10.833d | 0d | Waits for qualified media and migration rehearsal |

`READY / WAITING RESOURCE` and `ACTIVE` are empty. The resource schedule assumes
blocked work becomes available at time zero and is not a calendar forecast.

### Ordering decision

| Ordering | Immediate effect | Opportunity cost and qualification debt |
| --- | --- | --- |
| T59 first | Closes the only runnable residual goal-path capability using disposable file images | Selected; it is resource critical, but has only 0.333 days precedence float |
| Wait for VHDX capture | Preserves the primary stream for a disruptive critical node | The user has not supplied a safe window, so no capability closes while waiting |
| Wait for hardware | Preserves the primary stream for the parallel critical node | Exact apparatus and approval remain unavailable |
| Whole-namespace validation first | Closes a separately owned correctness gap | It has no edge to the accepted finish and leaves T59 open |

The closeout decision is `SELECT V5_V7_MIGRATION_REHEARSAL` (T59). It is the
only runnable node while all zero-slack tasks are externally blocked. If either
blocker resolves during T59, refresh resource capacity and `dag next` before
continuing because the remaining T59 duration may delay newly runnable critical
work. Selection does not authorize production migration, in-place relocation,
physical media, v6 compatibility, or production cutover.

## T59 Task Start replan and rebuilt PERT (2026-07-22)

- Record ID: `KAFS-PERTTOOL-20260722-T59-START-REPLAN`
- Calculation time: 2026-07-22 13:11 JST
- Baseline: branch `feat/v7-runtime-admission-foundation`, HEAD `7a5c750`,
  tracked worktree clean before the planning edit
- Accepted goal: unchanged; produce qualified v7 migration and production
  cutover evidence without collapsing import, rehearsal, media, or review
  boundaries

### Evidence that invalidated the inherited T59 boundary

T29 proves only that `kafsresize --migrate-create` can precheck a clean v5
source and create a valid empty v7 destination. Current `kafs-v7` controlled
write admits regular-file create/write/truncate and direct-directory append as
part of file creation. Its policy rejects `mkdir`, symlink creation, ownership
and timestamp mutation, indirect-directory mutation, cross-group mutation, and
other metadata changes. The inspection regression's nested directory and
symlink are constructed by test-owned raw fixture seeding, not by a production
importer.

Therefore a direct jump from destination creation to full
namespace/metadata/payload rehearsal would either test only a convenient
fixture or silently broaden the controlled runtime surface. Neither closes the
accepted migration capability. The inherited implementation boundary is
`REPLAN`, while the accepted migration goal remains valid.

The residual migration leg is decomposed as:

| PERT ID | Ticket | Capability | O | M | P | TE | Confidence |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| `MIGRATION_EVIDENCE_CONTRACT` | T59-A | Versioned source/destination inventory, phase, resume, rollback, and false-claim validation | 2 | 4 | 7 | 4.167 | Low |
| `V7_MIGRATION_IMPORT_SURFACE` | T59-B | V7-owned offline directory, regular-file, symlink, metadata, and payload import | 5 | 10 | 18 | 10.5 | Low |
| `V5_V7_MIGRATION_REHEARSAL` | T59-C | Normal/resume/rollback/idempotence rehearsal over the contract and importer | 3 | 6 | 10 | 6.167 | Low |

All three tasks use `PRIMARY_STREAM` capacity one. T59-A follows the reached
`V7_MIGRATION_TARGET_READY` milestone; T59-B follows T59-A, and T59-C follows
T59-B. This preserves source freeze, representability, destination admission,
and lifecycle evidence as causal predecessors rather than test details.

### Machine result

`./scripts/pert-next-task.sh plans/current.pert` passed `dsl check`,
`dag analyze --schedule both`, and `dag next`, in that order. The plan contains
16 milestones, 8 tasks, 7 gates, and 2 resources.

- Precedence makespan: 25.167 implementation days, conditional on both external
  blockers resolving.
- Precedence critical path:
  `MIGRATION_EVIDENCE_CONTRACT -> V7_MIGRATION_IMPORT_SURFACE ->
  V5_V7_MIGRATION_REHEARSAL -> MIGRATION_REHEARSAL_REQUIRED ->
  CUTOVER_EVIDENCE_DECISION`.
- Resource makespan: 32.833 implementation days under primary capacity one.
- Resource critical path begins with all three migration tasks, then continues
  through the conditional hardware/VHDX/media work and cutover decision.

| Classification | Node | TE | TF | Capability unlocked or wait reason |
| --- | --- | ---: | ---: | --- |
| `RUNNABLE NOW` | T59-A `MIGRATION_EVIDENCE_CONTRACT` | 4.167d | 0d | Freezes lifecycle and evidence semantics before importer work |
| `BLOCKED NOW` | `HARDWARE_APPROVAL` | 1.167d | 14.333d | Exact apparatus and digest-bound approval remain unavailable |
| `BLOCKED NOW` | `VHDX_HOST_RECOVERY_RUN` | 1.167d | 14.333d | No safe terminate/restart window has been supplied |
| `UPCOMING` | T59-B `V7_MIGRATION_IMPORT_SURFACE` | 10.5d | 0d | Waits for the migration contract |
| `UPCOMING` | T59-C `V5_V7_MIGRATION_REHEARSAL` | 6.167d | 0d | Waits for the offline importer |
| `UPCOMING` | `CUTOVER_EVIDENCE_DECISION` | 4.333d | 0d | Waits for migration rehearsal and qualified media |

`READY / WAITING RESOURCE` and `ACTIVE` are empty. The resource schedule is
conditional on blocked work becoming available at time zero and is not a
calendar forecast.

### Ordering decision

| Ordering | Immediate effect | Opportunity cost and rework |
| --- | --- | --- |
| T59-A contract first | Closes the only runnable zero-slack migration predecessor | Selected; importer and rehearsal share one versioned lifecycle boundary |
| T59-B importer first | Starts data construction sooner | Invalid ordering because source representability, partial state, resume, and acceptance semantics are not fixed |
| Original T59 rehearsal first | Produces a quick fixture demonstration | Cannot prove general namespace/metadata copy and would leave the missing importer hidden |
| Expand FUSE mutation first | Could reuse mounted copy tools later | Broadens runtime durability and qualification scope although an offline importer is the accepted boundary |

The decision is `SELECT MIGRATION_EVIDENCE_CONTRACT` (T59-A). It authorizes
only a versioned validate-only contract plus synthetic regression. It does not
authorize actual import, mount, production source access, v5/v6 entrypoint
reuse, physical media, or a cutover claim.

## T59-A closeout and next-task refresh (2026-07-22)

- Record ID: `KAFS-PERTTOOL-20260722-T59A-CLOSEOUT`
- Calculation time: 2026-07-22 13:37 JST
- Baseline: branch `feat/v7-runtime-admission-foundation`, start HEAD
  `7a5c750`, tracked changes limited to the reviewed T59-A wave
- Plan: `plans/current.pert`
- Tool: `perttool 0.1.0-alpha.1`
- Accepted goal: unchanged; produce qualified v7 migration and production
  cutover evidence without collapsing importer, rehearsal, media, or review
  boundaries

### Closed dependency

The validate-only T59-A gate now binds exact plan, source inventory, copy
ledger, destination inventory, and decision bytes. It rejects source mutation,
identity drift, incomplete or divergent destination semantics, illegal resume
transitions, artifact tampering, and cutover-claim escalation. Synthetic
regression proves the contract's ACCEPT, RESUME_REQUIRED, and ROLLBACK states;
it does not claim an importer or actual migration.

Build, focused regression, all 46 Automake tests, format, lint, clone/static
checks, and distribution passed. No PowerShell, VHDX, WSL termination, device,
production source, or cutover action ran. This closes
`MIGRATION_EVIDENCE_CONTRACT`; `MIGRATION_CONTRACT_READY` is reached.

The residual plan removes the completed task and its superseded
`V7_MIGRATION_TARGET_READY` starting milestone. The first closeout check
correctly rejected that old milestone when it no longer reached the residual
finish; removing the superseded start point made
`MIGRATION_CONTRACT_READY` the truthful current migration position.

### Machine result

`./scripts/pert-next-task.sh plans/current.pert` then passed `dsl check`,
`dag analyze --schedule both`, and `dag next`, in that order. The residual plan
contains 15 milestones, 7 tasks, 7 gates, and 2 resources.

- Precedence makespan: 21 implementation days, conditional on both external
  blockers resolving.
- Precedence critical path:
  `V7_MIGRATION_IMPORT_SURFACE -> V5_V7_MIGRATION_REHEARSAL ->
  MIGRATION_REHEARSAL_REQUIRED -> CUTOVER_EVIDENCE_DECISION`.
- Resource makespan: 28.667 implementation days under primary capacity one.
- Resource critical path starts with T59-B and T59-C, then conditionally
  schedules the two blocked primary-stream tasks, real-media execution,
  independent review, and cutover decision.

| Classification | Node | TE | TF | Capability unlocked or wait reason |
| --- | --- | ---: | ---: | --- |
| `RUNNABLE NOW` | T59-B `V7_MIGRATION_IMPORT_SURFACE` | 10.5d | 0d | Supplies the v7-owned offline construction path required by rehearsal |
| `BLOCKED NOW` | `HARDWARE_APPROVAL` | 1.167d | 10.167d | Exact apparatus and digest-bound approval remain unavailable |
| `BLOCKED NOW` | `VHDX_HOST_RECOVERY_RUN` | 1.167d | 10.167d | No safe terminate/restart window has been supplied |
| `UPCOMING` | T59-C `V5_V7_MIGRATION_REHEARSAL` | 6.167d | 0d | Waits for the offline importer |
| `UPCOMING` | `CUTOVER_EVIDENCE_DECISION` | 4.333d | 0d | Waits for rehearsal and qualified media |
| `UPCOMING` | `REAL_MEDIA_EXECUTION` | 4.167d | 10.167d | Waits for both external qualification predecessors |
| `UPCOMING` | `INDEPENDENT_REAL_MEDIA_REVIEW` | 1.167d | 10.167d | Waits for real-media evidence capture |

`READY / WAITING RESOURCE` and `ACTIVE` are empty. The resource schedule is
conditional on blocked work becoming available at time zero and is not a
calendar forecast.

### Ordering decision

| Ordering | Immediate effect | Opportunity cost and rework |
| --- | --- | --- |
| T59-B importer next | Closes the only runnable zero-slack predecessor | Selected; the contract is fixed and T59-C directly consumes this capability |
| T59-C rehearsal next | Starts lifecycle testing sooner | Invalid because no general importer exists and the predecessor is unsatisfied |
| Wait for VHDX or hardware | Preserves primary capacity | Both remain externally blocked, so waiting closes no capability |
| Expand mounted FUSE writes | Makes generic copy tools more convenient | Broadens runtime durability scope and repeats qualification instead of supplying the selected offline boundary |

The closeout decision is `SELECT V7_MIGRATION_IMPORT_SURFACE` (T59-B). It is
the only `RUNNABLE NOW` zero-slack task and requires its own fresh Task Start
Gate before edits. Selection does not authorize production data access,
in-place migration, v5/v6 runtime reuse, physical media, VHDX interruption, or
cutover.

## T59-B closeout and next-task refresh (2026-07-22)

- Record ID: `KAFS-PERTTOOL-20260722-T59B-CLOSEOUT`
- Calculation time: 2026-07-22 15:12 JST
- Baseline: branch `feat/v7-runtime-admission-foundation`, start HEAD
  `4bebbaa`, tracked changes limited to the reviewed T59-B wave
- Plan: `plans/current.pert`
- Tool: `perttool 0.1.0-alpha.1`
- Accepted goal: unchanged; produce qualified v7 migration and production
  cutover evidence without collapsing importer, rehearsal, media, or review
  boundaries

### Closed dependency

The offline `kafsresize --migrate-import-v7` surface now validates a frozen
clean v5 image, computes exact v7 data and indirect-index capacity, preserves
the supported namespace, inode identity, hardlinks, metadata, and dense
payload, constructs a private v7 work image, applies the full v7 validator,
and only then publishes the final path without replacement. Unsupported,
sparse, pending, changed, undersized, and injected-partial cases fail closed.

Build, the final import regression repeated ten times, all 47 Automake tests,
format, lint, clone/static checks, and distribution passed. No PowerShell,
VHDX, WSL termination, device, production-source, or cutover action ran. This
closes `V7_MIGRATION_IMPORT_SURFACE`; `MIGRATION_IMPORT_READY` is reached. The
owned `SDW-V7RT-T59-B-F1` pending-reference and `SDW-V7RT-T59-B-F2`
indirect-index bitmap findings remain off this importer edge and remain T59-C
rejection cases.

The first closeout calculation correctly rejected the superseded
`MIGRATION_CONTRACT_READY` residual start because it no longer reached the
finish after T59-B was removed. Removing that superseded starting milestone
made `MIGRATION_IMPORT_READY` the truthful current migration position.

### Machine result

`./scripts/pert-next-task.sh plans/current.pert` passed `dsl check`,
`dag analyze --schedule both`, and `dag next`, in that order. The residual plan
contains 14 milestones, 6 tasks, 7 gates, and 2 resources.

- Precedence makespan: 10.833 implementation days, conditional on both
  external blockers resolving.
- Precedence critical paths contain the hardware/VHDX qualification branches,
  real-media execution, independent review, and cutover decision.
- Resource makespan: 17 implementation days under primary capacity one.
- T59-C is resource critical; the conditional resource schedule places it
  after blocked primary-stream work and real-media execution.

| Classification | Node | TE | TF | Capability unlocked or wait reason |
| --- | --- | ---: | ---: | --- |
| `RUNNABLE NOW` | T59-C `V5_V7_MIGRATION_REHEARSAL` | 6.167d | 0.333d | Proves lifecycle resume, rollback, idempotence, and evidence over the completed importer |
| `BLOCKED NOW` | `HARDWARE_APPROVAL` | 1.167d | 0d | Exact apparatus and digest-bound approval remain unavailable |
| `BLOCKED NOW` | `VHDX_HOST_RECOVERY_RUN` | 1.167d | 0d | No safe terminate/restart window has been supplied |
| `UPCOMING` | `REAL_MEDIA_EXECUTION` | 4.167d | 0d | Waits for both external qualification predecessors |
| `UPCOMING` | `INDEPENDENT_REAL_MEDIA_REVIEW` | 1.167d | 0d | Waits for real-media evidence capture |
| `UPCOMING` | `CUTOVER_EVIDENCE_DECISION` | 4.333d | 0d | Waits for migration rehearsal and qualified media |

`READY / WAITING RESOURCE` and `ACTIVE` are empty. The resource schedule is
conditional on blocked work becoming available at time zero and is not a
calendar forecast.

### Ordering decision

| Ordering | Immediate effect | Opportunity cost and rework |
| --- | --- | --- |
| T59-C rehearsal next | Closes the only runnable least-slack residual migration capability | Selected; it directly consumes the completed contract and importer |
| Wait for VHDX or hardware | Preserves primary capacity | Both remain externally blocked, so waiting closes no capability |
| Expand mounted v7 writes | Makes generic copy tools more convenient | Repeats runtime durability qualification and bypasses the accepted offline lifecycle boundary |
| Investigate T59-B-F1/F2 first | Could explain rejected v5 source states and a full-fsck detection gap | They remain owned but are off the accepted goal path because pending and bitmap-invalid sources already fail closed |

The closeout decision is `SELECT V5_V7_MIGRATION_REHEARSAL` (T59-C). It is the
only `RUNNABLE NOW` least-slack task and requires its own fresh Task Start Gate.
Selection does not authorize production data access, automatic repair of an
ineligible v5 source, runtime mutation expansion, physical media, VHDX
interruption, or cutover.

## T59-C closeout and next-task refresh (2026-07-22)

- Record ID: `KAFS-PERTTOOL-20260722-T59C-CLOSEOUT`
- Calculation time: 2026-07-22 16:19 JST
- Baseline: branch `feat/v7-runtime-admission-foundation`, start HEAD
  `d46cefd`, tracked changes limited to the reviewed T59-C wave
- Plan: `plans/current.pert`
- Tool: `perttool 0.1.0-alpha.1`
- Accepted goal: unchanged; produce qualified v7 migration and production
  cutover evidence without collapsing rehearsal, media, or independent-review
  boundaries

### Closed dependency

The disposable-only rehearsal runner now creates and retains real clean v5,
normal v7, injected-partial, replayed v7, and rollback-preserved images. It
binds mounted semantic inventories, source identity and SHA-256 immutability,
fsck/dump, no-replace idempotence, and four T59-A lifecycle bundles into one
artifact manifest. Attempt 2 deliberately preserves the failed attempt-1 image
and performs a full replay from the same frozen source; it does not claim an
in-place continuation algorithm.

The focused workload, all 48 Automake tests, format, lint, clone/static, and
distribution gates passed. No PowerShell, VHDX, WSL termination, device,
production source, or cutover action ran. This closes
`V5_V7_MIGRATION_REHEARSAL`;
`MIGRATION_REHEARSAL_READY` is reached. The residual plan removes the completed
task and its superseded `MIGRATION_IMPORT_READY` starting milestone.

### Machine result

`./scripts/pert-next-task.sh plans/current.pert` passed `dsl check`,
`dag analyze --schedule both`, and `dag next`, in that order. The residual plan
contains 13 milestones, 5 tasks, 7 gates, and 2 resources.

- Precedence makespan: 10.833 implementation days, conditional on both
  external blockers resolving.
- Precedence has two zero-slack critical paths, one through hardware approval
  and one through VHDX host recovery; both join real-media execution,
  independent review, and the cutover decision.
- Resource makespan: 12 implementation days under primary capacity one, with
  1.167 days conditional contention between the two blocked primary tasks.
- The resource schedule assumes both blockers resolve at time zero and is not
  a calendar forecast.

| Classification | Node | TE | TF | Capability unlocked or wait reason |
| --- | --- | ---: | ---: | --- |
| `BLOCKED NOW` | `HARDWARE_APPROVAL` | 1.167d | 0d | Exact apparatus, cycle count, and digest-bound destructive approval remain unavailable |
| `BLOCKED NOW` | `VHDX_HOST_RECOVERY_RUN` | 1.167d | 0d | The active Ubuntu distro still has no safe terminate/restart window |
| `UPCOMING` | `REAL_MEDIA_EXECUTION` | 4.167d | 0d | Waits for both blocked qualification predecessors |
| `UPCOMING` | `INDEPENDENT_REAL_MEDIA_REVIEW` | 1.167d | 0d | Waits for real-media evidence capture |
| `UPCOMING` | `CUTOVER_EVIDENCE_DECISION` | 4.333d | 0d | Waits for qualified media; migration rehearsal is already reached |

`RUNNABLE NOW`, `READY / WAITING RESOURCE`, and `ACTIVE` are empty.

### Ordering decision

| Ordering | Immediate effect | Opportunity cost and rework |
| --- | --- | --- |
| Wait for an explicitly available VHDX window | Makes the zero-slack host-recovery task runnable after refresh | Selected disposition; no terminate/restart is authorized before the user names that window |
| Prepare and approve exact physical hardware | Makes the other zero-slack external task runnable after refresh | Equally valid blocker-resolution path, but exact apparatus and destructive approval are absent |
| Investigate T59-B-F1/F2 | Could explain owned source-preparation findings | Does not shorten the accepted residual finish; pending/bitmap-invalid sources already fail closed |
| Continue indirect-directory or cross-group runtime work | Advances a separate product capability | Creates qualification debt and does not reduce either current critical blocker |

The closeout decision is `BLOCKED`: the valid plan has no runnable critical or
least-slack task. No next implementation wave is selected. Resume only after a
safe WSL maintenance window or exact physical-media apparatus/approval is
available, then refresh current evidence, the plan, and the Task Start Gate.
