# KAFS v7 VHDX host-recovery WIP handoff (2026-07-21)

## Closeout status

- Closeout time: 2026-07-21 21:14 JST
- Branch: `feat/v7-runtime-admission-foundation`
- Push target: `origin/feat/v7-runtime-admission-foundation`
- Completed implementation checkpoint:
  `224de59 feat: add VHDX-backed host recovery harness`
- Publication state before this handoff: local branch was 13 commits ahead of
  upstream `460f7b0`; this handoff is intentionally published as a subsequent
  `WIP:` documentation commit.
- Tracked worktree state before this handoff: clean.
- Untracked state: existing generated test executables remain under `tests/`.
  They are build outputs, are not staged, and must not be mistaken for tomorrow's
  product diff.

This handoff freezes today's scope. The harness is complete; the actual native
Windows terminate/restart matrix has not run.

## What completed today

The committed slice provides:

- an opt-in durability-point hook that creates and fsyncs an exclusive marker,
  fsyncs its parent directory, and stops `kafs-v7` without changing the normal
  deterministic process-fault exit path;
- resumable `--vhdx-arm` and `--vhdx-verify` modes for a dedicated regular-file
  KAFS image on the active Ubuntu ext4 filesystem;
- a Linux runner that confines state below the current WSL home on the same ext4
  source as the repository, and rejects DrvFs, stale state, mismatched evidence,
  and claim escalation;
- a native Windows controller that rediscovers the registered distro VHDX and
  requires both `-Execute` and high-impact `ShouldProcess` confirmation before
  calling `wsl.exe --terminate`;
- host/WSL identity, recovery diagnostic, payload, full-fsck, dump, manifest,
  and SHA-256 evidence sealing;
- a PERT closeout that marks `VHDX_HARNESS_READY` reached without removing the
  blocked physical-hardware branch.

Observed preflight identity on this host was Ubuntu on `/dev/sdd` ext4, backed
by the registered `ext4.vhdx` at allocated length `120680611840` bytes. This is
a per-run discovery fact, not a value to hard-code tomorrow.

## Validation carried into tomorrow

- Warning-clean `autoreconf -fi`, `./configure`, and `make -j2`: PASS.
- Complete `v7_inspection_mount_smoketest`: PASS.
- All four fault points (`journal_publish`, `checkpoint_copy`,
  `metadata_apply`, `journal_reclaim`) reached the durable stop marker and
  passed substitute process-kill arm/verify, recovery diagnostic, payload,
  full-fsck, dump, and artifact-digest checks.
- Linux and Windows preflight without termination: PASS.
- DrvFs, state outside the WSL home, and false real-media claim rejection: PASS.
- Full `make check -j2`: 42/43 on the first run; the unrelated existing
  `e2e_hotplug` timed out once and passed its immediate isolated rerun.
- Format, lint, strict source clone (48 clones, 490 duplicated lines, 0.97%),
  complexity, both v7 ownership checks, shellcheck, `make distdir`, and
  `perttool`: PASS.

The substitute process-kill matrix validates the harness only. It is not
Windows-host interruption evidence.

## Machine-selected next task

Current `./scripts/pert-next-task.sh plans/current.pert` result:

- `RUNNABLE NOW`: `VHDX_HOST_RECOVERY_RUN`, total float `0d`, precedence and
  resource critical;
- `BLOCKED NOW`: `HARDWARE_APPROVAL`, total float `0d`, parallel critical join
  branch;
- `UPCOMING`: real-media qualification and migration/cutover evidence.

Tomorrow's restart point is therefore **the fresh Task Start Gate for
`VHDX_HOST_RECOVERY_RUN`, followed by native Windows read-only preflight**. Do
not select namespace validation or another locally easy task.

## Tomorrow's Task Start Gate

Before any terminate command, refresh these facts:

1. Confirm the fetched branch contains `224de59` and this handoff WIP commit.
2. Run `git status --short --branch`. The only expected untracked files are the
   known generated test executables; any tracked diff requires `REPLAN`.
3. Run `./scripts/pert-next-task.sh plans/current.pert` and require
   `VHDX_HOST_RECOVERY_RUN` to remain the runnable zero-slack frontier.
4. Confirm the target is the registered Ubuntu distro and that the controller
   rediscovers a `.vhdx`; do not reuse today's path or size without discovery.
5. From native Windows PowerShell, run the preflight command below without
   `-Execute`. Require both Linux and host preflight PASS markers and false RC,
   real-media, physical-power, and controller-independent-wear claims.
6. Confirm no prior state directory is being reused. Arm must create a fresh run
   ID and fresh per-fault directory.

Gate decision:

- `PASS`: all six checks hold; execute the four-point matrix.
- `REPLAN`: branch, plan frontier, distro/VHDX identity, or tracked state differs.
- `BLOCKED`: native Windows control is unavailable or the preflight cannot prove
  the safe regular-file boundary.

## Exact native Windows commands after PASS

Run from **native Windows PowerShell outside Ubuntu**, never from Codex or a
shell hosted inside the target distro:

```powershell
$runner = '\\wsl.localhost\Ubuntu\home\katsumata-m\kafs\scripts\v7-vhdx-host-recovery.ps1'

# Task Start evidence: read-only preflight; does not terminate WSL.
& $runner -Distro Ubuntu

# Start only after recording PASS.
& $runner -Distro Ubuntu -Execute -Confirm
```

The second command intentionally terminates and restarts Ubuntu four times. The
native controller must remain alive outside the distro to perform verification.

## Exit criteria for `VHDX_HOST_RECOVERY_RUN`

The task is complete only when all four fresh fault directories contain valid:

- `pause.marker` with the matching point;
- `host-controller.json` with successful terminate/restart exit codes and the
  exact rediscovered VHDX identity;
- `vhdx-verify.ok`, recovery log, and persisted-payload PASS;
- `fsck-full-check.stdout`/`stderr` and `kafsdump.json`/`stderr`;
- `verification-manifest.json` with all out-of-scope claims false;
- `artifacts.sha256` passing `sha256sum -c`;
- controller output `KAFS_V7_VHDX_HOST_RECOVERY PASS run_id=<run-id>`.

After those checks, update `plans/current.pert` to mark the host recovery
milestone reached, rerun the three-command perttool gate, and record the new
frontier. Physical `HARDWARE_APPROVAL` must remain in the graph.

## Stop and preserve conditions

On timeout, mismatched marker, failed terminate/restart, failed recovery, failed
fsck/dump, or digest mismatch:

- do not reuse or delete the failed state directory;
- preserve controller output and every artifact already written;
- report the exact failed fault and phase;
- keep all RC, real-media, physical-power, and wear claims false;
- do not proceed to the next fault or physical-media work until the failure is
  diagnosed and the Task Start Gate is rerun.

## Explicit do-not-do list

- Do not raw-open, format, mount, compact, or otherwise mutate `ext4.vhdx` as a
  KAFS device.
- Do not launch `-Execute` from the Ubuntu distro that will be terminated.
- Do not treat VHDX evidence as SD-card, flash-controller, physical-power, wear,
  or RC evidence.
- Do not remove `HARDWARE_APPROVAL` from PERT or substitute off-path namespace
  work because it is locally easier.
- Do not stage the existing generated `tests/v7_*` and `tests/v6_fixture_mkfs`
  executables.

## First files to read tomorrow

1. This handoff.
2. [VHDX host-recovery procedure](sd-card-wear-v7-vhdx-host-recovery.md).
3. [Current residual PERT](../plans/current.pert).
4. [V7 indirect PERT and Task Start records](sd-card-wear-v7-indirect-pert-20260721.md).
5. [Canonical v7 runtime handoff](sd-card-wear-v7-runtime-handoff-20260716.md).
