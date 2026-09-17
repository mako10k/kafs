# KAFS v7 WIP handoff — 2026-09-17

## Preservation boundary

- Branch: `fix/arm32-portability`; resolve the handoff commit with
  `git rev-parse HEAD` after checkout. The remote branch was
  `159af1097cfeb7e2d807a6919f9a24e3dc2f63b7` before this WIP push.
- This WIP preserves the v7 local-validation source/test edits, the scoped
  local-validation and non-power real-media plans/selection records, the
  guarded real-media runner sources, and this handoff. It does not claim
  implementation completion, real-media qualification, release, or Moxa
  installation of these changes.
- The earlier local commits `2ead576` (v7 fsstat and global-operations
  audit) and `ec3ba2d` (current goal PERT) also precede this handoff on
  the branch. The latter is a reviewed-scope WIP commit.
- The extracted `kafs-0.4.0/` distribution tree, nine generated test
  executables, and `scripts/__pycache__/` remain local and untracked.
  They are deliberately not part of the push; do not discard them merely
  to make the worktree clean.

## Accepted goal and current evidence

- `plans/current.pert` now models v7 implementation 100 percent,
  integrated software recovery/fsck/wear proof, non-power real-media
  qualification, and an authorized release with physical power-interruption
  behavior explicitly untested. Physical power-interruption testing is
  retained as non-goal `Work`, not an executable predecessor. This is a
  planning contract, not permission to release or write a card.
- The v7 runtime remains bounded. The accepted v7 inception/raw-layout
  contracts still require HRL metadata and software recovery correctness.
  HRL runtime integration, delayed scanning, full accepted operation coverage,
  and normal-write locality are not complete merely because the PERT parses.
- The earlier KINGMAX 2 GB/UGREEN/Moxa normal run stopped after five of
  twelve passing cases when the remaining workload appeared impractically
  slow. That is incomplete non-power evidence, not a card pass or power-loss
  result. This closeout did not access the SD card or Moxa; their current
  physical/runtime state was not rechecked.
- The scoped `plans/v7-real-media-nonpower.pert` and its selection record
  predate the stopped run and the new release goal. Their task state and
  description are historical selection evidence until refreshed; do not run
  `dag next` on them as a current execution instruction.

## Local-validation WIP preserved here

- `src/kafs_v7_journal_writer.c` adds readback of published journal
  header/data; layout, sequence, and transaction files add a local recovery
  state/sequence confirmation path for an already fully admitted mount.
  `tests/tests_v7_journal_replay_smoketest.c` adds a damaged-header poison
  case. These seven changed files are WIP, not a completed validator removal.
- On this worktree, `make -C tests v7_journal_replay_smoketest` and
  `make -j2` passed warning-clean. The direct
  `tests/v7_journal_replay_smoketest` run exited 0. `./scripts/format.sh`
  passed after formatting the one reported source file. No full `make check -j2`,
  real-size read-work comparison,
  Moxa rebuild, or renewed card run was performed for this WIP.
- `docs/kafs-v7-local-validation-selection-20260917.md` and
  `plans/v7-local-validation-20260917.pert` record the earlier scoped
  `LOCAL_PUBLICATION` selection. They state that no absolute latency
  threshold was accepted and that the structural correction was runnable.
  The newly committed `plans/current.pert` instead marks
  `V7_LOCAL_WRITE_PATH` blocked for a missing latency threshold and
  incidental corruption-detection decision. This is a material planning
  contradiction, not a settled product requirement. Reconcile it against
  the accepted v7 contract and current code before selecting the next wave;
  do not infer a blocker solely from the new PERT or silently discard the
  scoped selection.

## Real-media runner sources preserved here

- `scripts/v7-real-media-nonpower-normal.sh` and
  `scripts/v7-real-media-normal-workload.py` are guarded raw-card rehearsal
  sources. The shell wrapper requires an exact `/dev/disk/by-id` target,
  size, major:minor, and explicit execution/confirmation arguments. Its
  fresh mode reformats once; resume mode is narrower. These scripts are not
  evidence that their twelve-case or process-stop scope passed.
- Do not rerun them from this handoff. Before any future destructive run,
  independently identify the current card and process/mount state, disclose
  the exact target and write/reformat scope, and use the then-current
  authorization and test contract. Physical power cutting remains outside
  the release goal.

## Resume point

1. Check `worktimectl agent`, the branch/HEAD/worktree and the remote SHA;
   read this handoff, `plans/current.pert`, and both scoped selection
   records. Treat all dated host/card observations as stale until refreshed.
2. Reconcile the current-PERT local-write blocker with the scoped selection,
   and refresh `perttool document check`, `dag analyze --schedule both`,
   and `dag next` before choosing any implementation wave. Exact v7
   operation scope and release identity/channel are still owner decisions.
3. Review the seven-file local-validation WIP by file/hunk; complete its
   remaining checkpoint/closeout path, crash/fsck proof, and real-size
   read-work comparison before claiming the scoped capability. Run the
   repository's proportionate build/test/static gates.
4. Refresh the non-power real-media plan and runner evidence only after the
   write path is qualified. The stopped five-of-twelve run is diagnostic
   evidence, not a passing qualification. A future release requires its own
   exact candidate authorization and disclosure of untested physical
   power-loss behavior.

No PR, merge, release, production cutover, remote installation, card write,
cleanup, or workday stop/end is authorized by this handoff or WIP push.
