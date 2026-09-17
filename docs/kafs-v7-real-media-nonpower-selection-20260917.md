# Bounded v7 non-power real-media task selection — 2026-09-17

- Selection record: `KAFS-V7-NONPOWER-20260917-A`
- Branch and start HEAD: `fix/arm32-portability`, `159af1097cfeb7e2d807a6919f9a24e3dc2f63b7`
- Worktree: no tracked changes at selection; retained untracked distribution and
  test binaries from the ARM32 work are not part of this scope.
- Remote readback: `origin/fix/arm32-portability` matched start HEAD through
  `secdat exec git ls-remote` on 2026-09-17.
- Scoped plan: `plans/v7-real-media-nonpower.pert`, selected in place of
  `plans/current.pert` because the owner-approved non-power experiment is not
  the full physical-interruption and production-cutover goal.
- Tool: `perttool 0.11.0`; command:
  `./scripts/pert-next-task.sh plans/v7-real-media-nonpower.pert`.

## Accepted outcome and current capability

The owner approved one run of the twelve normal controlled-write workload
classes and one software process-stop recovery run at each of four boundaries
on the current KINGMAX 2 GB card in the UGREEN reader on Moxa. The run must
retain fsck, dump, and logs. Physical power interruption, long-duration wear,
full real-media qualification, and RC acceptance are excluded. Before another
card write, disclose the exact case commands, target, and reformat count.

The 2026-09-17 raw-card smoke passed one v7 format, one 4 KiB fsync write,
normal unmount, offline fsck, read-only remount, and matching SHA-256 readback.
The existing qualification runner accepts temporary file images only. The
real-media approval and evidence scripts validate records but do not execute
the raw-device workloads. `plans/current.pert` still describes the broader
goal and a July hardware-blocked state, so it cannot select this scoped wave.
Code inspection found that a 4096-byte block makes the triple-indirect
threshold 4,299,210,752 bytes, above this card's 2,013,265,920-byte
capacity. The accepted 1024-byte block geometry reduces the threshold to
67,383,296 bytes. Whether the complete raw-card workload can run within
acceptable time and wear remains unverified; the exact one-time reformat and
write volume must be disclosed before card execution.

## Model changes and calculation

This plan adds a directly observed smoke milestone and five residual
capability outcomes: guarded normal runner, normal card evidence, process-stop
runner, process-stop card evidence, and bounded result. It has one primary
operator stream at capacity one. O/M/P estimates are implementation-effort
points, not elapsed calendar promises. Confidence is low for both runner
estimates because the raw-device adaptation and fixture requirements have not
yet passed the Task Start Gate; the card-run estimates have medium-low
confidence because card latency and health are unknown. No external waiting
time is included.

The successful tool output reported `document check` OK with 6 milestones,
5 tasks, and 1 resource; precedence and resource makespan `14.167p`; the
representative critical path was `NORMAL_RAW_RUNNER -> NORMAL_CARD_RUN ->
PROCESS_STOP_RUNNER -> PROCESS_STOP_CARD_RUN -> NONPOWER_RESULT`. All five
tasks had `TF=0p`. `ACTIVE`, `READY / WAITING RESOURCE`, and `BLOCKED NOW`
were empty. `RUNNABLE NOW` contained only `NORMAL_RAW_RUNNER` (`4.333p`,
`TF=0p`); the other four tasks were `UPCOMING` behind their stated
milestones. The `1.771d` velocity forecast is a tool projection, not a
delivery date or promise within this workday.

## Selection and alternative ordering

Decision: `SELECT NORMAL_RAW_RUNNER`, subject to the Task Start Gate. Its
capability is an exact-target, fail-closed raw-card workload runner with a
reviewable write manifest and evidence capture. Running ad hoc card commands
first might yield individual observations sooner, but it would not cover the
accepted twelve-case contract consistently and would create qualification
debt or repeated card writes. Starting with process-stop injection before
normal raw-card execution would add an unverified recovery layer to the
untested real-media workload and increase diagnosis/rework cost. The selected
order preserves the current smoke result and delays further card mutation
until a bounded protocol is visible.

This selection authorizes no card operation by itself. The producing cause of
the earlier 4 GB card USB disconnect remains unknown and is not treated as a
KAFS failure or as proof that the current card is durable.
