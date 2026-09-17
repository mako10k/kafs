# v7 local runtime validation selection — 2026-09-17

- Scope authority: owner requested implementation of the previously identified
  transaction-local validation design after committing the `fsstat`/audit slice.
  The later architecture question does not expand this work to enabling v7 HRL
  deduplication, direct/HRL pool policy, background scanning, or Moxa deployment.
- Checkout: `fix/arm32-portability`, HEAD `2ead5767b06909a61bcc03f05ebc08e1baa6a3f7`,
  linked worktree `.worktree/arm32-portability`. The prior slice is committed
  locally, not pushed. Existing untracked card-test scripts, source bundle and
  binaries are outside this scope and remain untouched.
- Accepted end goal: ordinary enabled v7 writes do not repeatedly run the
  full-metadata image validator, while journal sequence, durability,
  checkpoint redundancy, crash recovery, admission, and offline fsck contracts
  remain conformant. There is no accepted absolute latency threshold; the
  observed 2 GB versus 128 MB file-image read-work comparison is a structural
  baseline, not a real-media latency target.
- Current capability: full validation is performed at admission; the normal
  mutation and closeout paths also call it repeatedly. The accepted v7 inception
  deck prefers admission-time validation and cached shard maps on the runtime
  path. V7 background dedup is disabled, and current FUSE writes use direct COW.
- Plan: `plans/v7-local-validation-20260917.pert`; `perttool 0.11.0`.
  Estimates are low-confidence effort points, not calendar time. One observed
  implementation stream of capacity one is encoded; no parallel capacity or
  point-to-day velocity is claimed. External media access is excluded.

`./scripts/pert-next-task.sh plans/v7-local-validation-20260917.pert` passes
document check and reports one precedence/resource critical path:
`LOCAL_PUBLICATION -> LOCAL_CLOSEOUT -> CRASH_FSCK_PERF_PROOF`.
Expected durations are `13p`, `13p`, and `8.667p`, respectively, with zero
total float; makespan `34.667p`. `LOCAL_PUBLICATION` alone is in `RUNNABLE NOW`
and the recommended set; no task is active or blocked. `LOCAL_CLOSEOUT` and
`CRASH_FSCK_PERF_PROOF` are upcoming. No calendar forecast is available.

Selection: `LOCAL_PUBLICATION` (SELECT), to make the enabled transaction
publication transition locally checkable before closeout consumes it.
Changing checkpoint closeout first would depend on an unproved post-publication
state and create repeat qualification. Running only perf tests first can refine
the cost attribution but cannot remove the known full-validator call path;
the existing controlled image comparison is enough to justify beginning the
structural correction, while exact SD-card wall-time contribution remains
unknown. No priority value or false dependency was used to force this order.

The earlier audit's request for a decision on per-mutation discovery of
unrelated post-mount corruption treated incidental implementation behavior as
though it were an accepted v7 requirement. No such clause was found in the
accepted v7 design/raw-layout documents inspected here. That question was
withdrawn. Full admission/recovery and offline fsck obligations still apply;
the implementation must fail closed on ambiguous local transitions rather
than bypassing validation.
