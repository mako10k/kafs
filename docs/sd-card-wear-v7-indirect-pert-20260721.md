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
