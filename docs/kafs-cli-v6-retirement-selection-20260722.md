# KAFS migration CLI and format-v6 retirement selection record

- Record ID: `KAFS-CLI-V6-RETIRE-SEL-20260722-01`
- Recorded at: `2026-07-22T16:57:56+09:00`
- Branch: `feat/v7-runtime-admission-foundation`
- HEAD: `f75521cf7625b0f27bffd1bfe0dab917462c5c56`
- Worktree: clean before plan authoring; this scoped plan and this record are the
  only planning changes
- Scoped plan: `plans/cli-v6-retirement.pert`
- Repository-default plan: `plans/current.pert` (unchanged)
- `perttool`: `0.1.0-alpha.1`
- Decision: `SELECT V6_RESIDUAL_CONTRACT` for the first implementation wave
- Execution authorization: granted by the user on `2026-07-22`; Task Start
  Record passed at HEAD `0d4b5d80d7f50e330a3bc75d043a9c3479aea8db`

## Why this is a scoped plan

`plans/current.pert` continues to represent the accepted production-cutover
goal. Its refreshed result has no runnable work and correctly reports
`HARDWARE_APPROVAL` and `VHDX_HOST_RECOVERY_RUN` in `BLOCKED NOW`. The user has
separately accepted a remediation goal for the current migration-command UI and
format-v6 retirement findings. Connecting those findings to production cutover
with invented causal edges would make the default critical path false, so this
record names `plans/cli-v6-retirement.pert` as the scoped replacement for this
remediation selection only.

## Accepted goal and selection-time capability

The accepted scoped end goal is:

> Migration commands reject malformed or inapplicable input, expose truthful
> and stable human and automation contracts, and the repository contains no
> supported v6 operational workflow or v6-owned dependency in active common or
> v7 build paths. Bounded offline v6 evidence remains only until its consumers
> are removed, after which the final placeholder and packaged command surface
> are retired. Clearly labeled historical records may remain as history.

Directly observed capabilities at initial selection (later closeout sections
supersede this snapshot):

- `CONFIRMED`: v5-to-v7 importer and disposable lifecycle rehearsal are
  complete at current HEAD.
- `CONFIRMED`: `kafs-v6` is a fail-closed placeholder and no current tracked C
  source assigns a nonzero value to `c_v6_controlled_write_enabled`.
- `CONFIRMED`: migration CLI parsing accepts partially numeric values,
  mode-inapplicable options, and stray operands instead of returning a usage
  error.
- `CONFIRMED`: current manuals, Unreleased notes, the cutover playbook, and v6
  operator scripts still describe operational paths rejected by current
  binaries.
- `CONFIRMED`: `kafs_shared_fuse_runtime.c`, `kafs_context.h`, and related
  common sources retain v6-owned runtime policy, state, and names while being
  compiled into active binaries.
- `CONFIRMED`: bounded v6 offline diagnostics and test fixture creation remain
  intentionally available under the accepted staged retirement plan.
- `INFERRED`: the remaining v6 controlled-write branches are unreachable in
  shipped runtime binaries. Source assignment and entrypoint searches support
  this, but each consumer still requires enumeration before deletion.
- `UNKNOWN`: whether any external experimental v6 image creates a recovery
  obligation beyond the repository's accepted recreate-as-v7 policy. Discovery
  of such an obligation requires replanning before offline diagnostics are
  removed.

## Model changes from the default plan

- Added a scoped remediation finish rather than changing the blocked
  production-cutover goal.
- Encoded one truthful primary implementation capacity: `PRIMARY_STREAM=1`.
- Added the CLI path for strict parsing followed by a stable lifecycle and
  automation contract.
- Added the staged v6 path in the accepted retirement order: truthful residual
  boundary, active-common decoupling, offline/fixture retirement, then final
  entrypoint removal.
- Added integrated validation after both paths close.
- Added no arbitrary `priority`; every task has priority zero.
- Added no external blocked task. External wait is excluded from estimates.

## Capability waves and boundaries

### `V6_RESIDUAL_CONTRACT`

Make the current support boundary truthful before deleting implementation:

- remove or replace instructions that tell operators to create or mount v6;
- retire repository operator scripts whose success path requires the rejected
  v6 runtime;
- correct `kafsresize` help/man, current playbook text, Unreleased notes, and
  indexed current guidance;
- distinguish current recovery evidence from historical-only records; and
- publish an owned inventory and disposition for every retained v6 surface.

It does not remove common runtime structures, offline diagnostics, fixtures, or
the placeholder.

### `V6_ACTIVE_COMMON_DECOUPLING`

Remove v6 runtime-policy ownership from active shared code. This includes the
shared FUSE policy calls, v6-owned context/state aliases where active code still
sees them, and coverage of shared sources in the v7 ownership guard. It may
extract genuinely neutral behavior but may not route v7 through a v6 facade.

### `V6_OFFLINE_RETIREMENT`

After enumerating all consumers, retire v6-specific fsck/dump behavior,
test-only image creation, descriptor fixtures and regression wiring, obsolete
static-analysis exclusions, and v6-only current documentation. If an external
recovery obligation is discovered, this wave returns `REPLAN` rather than
silently deleting the last inspection path.

### `V6_FINAL_ENTRYPOINT_RETIREMENT`

Only after all other v6 surfaces close, remove `kafs-v6`, its build/install
rules, manual, completion, and final placeholder-only tests.

### `CLI_FAIL_CLOSED_CONTRACT`

Reject malformed numeric values, missing option values, stray operands, and
mode-inapplicable options. Synchronize help, man, completion, and exit semantics
for supported modes. The v6 guidance sweep belongs to
`V6_RESIDUAL_CONTRACT`, avoiding overlapping ownership.

### `MIGRATION_AUTOMATION_CONTRACT`

Provide a stable, versioned machine-readable contract for the importer,
rehearsal, and evidence gate; preserve human output only where compatibility is
intentional. Use full-replay terminology, and document `/dev/fuse`, report
paths, retention, and exit 77.

### `INTEGRATED_REMEDIATION_QUALIFICATION`

Run focused CLI/import/rehearsal and offline-tool checks, packaging and
ownership checks, static/clone gates, and the full regression suite. Confirm
that any remaining v6 reference is historical and labeled, not executable
guidance or an active build dependency.

## Explicit non-goals

- WSL termination, VHDX host-recovery execution, or changing its blocked state.
- Physical-media qualification or destructive-media approval.
- Production cutover or cutover authorization.
- New v6 compatibility, migration, runtime, write, repair, or parity behavior.
- Preserving v6 wire/API behavior inside v7.
- Treating a zero-text-match repository as the goal; labeled historical records
  may remain.

## Estimate basis and confidence

Estimates cover implementation and proportional validation effort, not external
calendar wait.

| Task | O/M/P | Confidence | Main uncertainty |
|---|---:|---|---|
| `V6_RESIDUAL_CONTRACT` | 1/2/3d | high | historical versus current document disposition |
| `V6_ACTIVE_COMMON_DECOUPLING` | 2/4/8d | low | shared FUSE/context consumer breadth and regression coupling |
| `V6_OFFLINE_RETIREMENT` | 2/5/10d | low | offline descriptor consumers and recovery obligations |
| `V6_FINAL_ENTRYPOINT_RETIREMENT` | 1/2/3d | high | packaging and final test wiring |
| `CLI_FAIL_CLOSED_CONTRACT` | 1/2/4d | medium | parser compatibility cases and error-contract coverage |
| `MIGRATION_AUTOMATION_CONTRACT` | 2/3/6d | medium | schema/version compatibility across three command surfaces |
| `INTEGRATED_REMEDIATION_QUALIFICATION` | 1/2/4d | medium | full-suite and static/clone fallout after source removal |

## `perttool` calculation evidence

The following values are copied from
`./scripts/pert-next-task.sh plans/cli-v6-retirement.pert`; they are not
recalculated in this record.

```text
OK plans/cli-v6-retirement.pert project=KAFS_CLI_V6_RETIREMENT milestones=13 tasks=7 gates=6 resources=1

QUALIFIERS
CONDITIONAL_ON_BLOCKS_RESOLVED false
BLOCKED_TASKS -
PATHS_TRUNCATED false

PRECEDENCE
MAKESPAN 15.833d
ID EXPECTED ES EF LS LF TF FF CRITICAL
CLI_FAIL_CLOSED_CONTRACT 2.167d 0d 2.167d 8.167d 10.333d 8.167d 0d no
MIGRATION_AUTOMATION_CONTRACT 3.333d 2.167d 5.5d 10.333d 13.667d 8.167d 0d no
CLI_CONTRACT_REQUIRED 0d 5.5d 5.5d 13.667d 13.667d 8.167d 8.167d no
V6_RESIDUAL_CONTRACT 2d 0d 2d 0d 2d 0d 0d yes
V6_ACTIVE_COMMON_DECOUPLING 4.333d 2d 6.333d 2d 6.333d 0d 0d yes
V6_OFFLINE_RETIREMENT 5.333d 6.333d 11.667d 6.333d 11.667d 0d 0d yes
V6_FINAL_ENTRYPOINT_RETIREMENT 2d 11.667d 13.667d 11.667d 13.667d 0d 0d yes
V6_RETIREMENT_REQUIRED 0d 13.667d 13.667d 13.667d 13.667d 0d 0d yes
INTEGRATED_REMEDIATION_QUALIFICATION 2.167d 13.667d 15.833d 13.667d 15.833d 0d 0d yes

PRECEDENCE CRITICAL
TASKS V6_RESIDUAL_CONTRACT, V6_ACTIVE_COMMON_DECOUPLING, V6_OFFLINE_RETIREMENT, V6_FINAL_ENTRYPOINT_RETIREMENT, INTEGRATED_REMEDIATION_QUALIFICATION
GATES V6_RETIREMENT_REQUIRED
REPRESENTATIVE PATH V6_RESIDUAL_CONTRACT -> V6_ACTIVE_COMMON_DECOUPLING -> V6_OFFLINE_RETIREMENT -> V6_FINAL_ENTRYPOINT_RETIREMENT -> V6_RETIREMENT_REQUIRED -> INTEGRATED_REMEDIATION_QUALIFICATION
PATH COUNT 1

RESOURCE SCHEDULE
ALGORITHM parallel-sgs@1 optimal=false
PRECEDENCE LOWER BOUND 15.833d
MAKESPAN 21.333d
DELAY 5.5d
ID ELIGIBLE START FINISH WAIT REQUIREMENTS
V6_RESIDUAL_CONTRACT 0d 0d 2d 0d PRIMARY_STREAM=1
V6_ACTIVE_COMMON_DECOUPLING 2d 2d 6.333d 0d PRIMARY_STREAM=1
V6_OFFLINE_RETIREMENT 6.333d 6.333d 11.667d 0d PRIMARY_STREAM=1
V6_FINAL_ENTRYPOINT_RETIREMENT 11.667d 11.667d 13.667d 0d PRIMARY_STREAM=1
CLI_FAIL_CLOSED_CONTRACT 0d 13.667d 15.833d 13.667d PRIMARY_STREAM=1
MIGRATION_AUTOMATION_CONTRACT 15.833d 15.833d 19.167d 0d PRIMARY_STREAM=1
INTEGRATED_REMEDIATION_QUALIFICATION 19.167d 19.167d 21.333d 0d PRIMARY_STREAM=1

RESOURCE CRITICAL
TASKS V6_RESIDUAL_CONTRACT, V6_ACTIVE_COMMON_DECOUPLING, V6_OFFLINE_RETIREMENT, V6_FINAL_ENTRYPOINT_RETIREMENT, CLI_FAIL_CLOSED_CONTRACT, MIGRATION_AUTOMATION_CONTRACT, INTEGRATED_REMEDIATION_QUALIFICATION
RESOURCE ARCS resource:V6_FINAL_ENTRYPOINT_RETIREMENT:CLI_FAIL_CLOSED_CONTRACT
REPRESENTATIVE PATH V6_RESIDUAL_CONTRACT -> V6_ACTIVE_COMMON_DECOUPLING -> V6_OFFLINE_RETIREMENT -> V6_FINAL_ENTRYPOINT_RETIREMENT -> CLI_FAIL_CLOSED_CONTRACT -> MIGRATION_AUTOMATION_CONTRACT -> INTEGRATED_REMEDIATION_QUALIFICATION
PATH COUNT 1

RESOURCE UTILIZATION
ID CAPACITY AMOUNT_TIME UTILIZATION PEAK LAST_RELEASE
PRIMARY_STREAM 1 21.333d 1 1 21.333d
```

## `dag next` classification and selection

```text
ACTIVE
-

RUNNABLE NOW
V6_RESIDUAL_CONTRACT priority=0 expected=2d TF=0d precedence_critical=true schedule_critical=true owner=KAFS implementation stream resources=PRIMARY_STREAM=1

READY / WAITING RESOURCE
CLI_FAIL_CLOSED_CONTRACT priority=0 expected=2.167d TF=8.167d precedence_critical=false schedule_critical=true owner=KAFS implementation stream resources=PRIMARY_STREAM=1
  PRIMARY_STREAM capacity=1 used=1 required=1 available=0 deficit=1 occupants=V6_RESIDUAL_CONTRACT

BLOCKED NOW
-

UPCOMING
V6_ACTIVE_COMMON_DECOUPLING expected=4.333d TF=0d
V6_OFFLINE_RETIREMENT expected=5.333d TF=0d
V6_FINAL_ENTRYPOINT_RETIREMENT expected=2d TF=0d
INTEGRATED_REMEDIATION_QUALIFICATION expected=2.167d TF=0d
MIGRATION_AUTOMATION_CONTRACT expected=3.333d TF=8.167d
```

`V6_RESIDUAL_CONTRACT` is selected because it is the only `RUNNABLE NOW` task,
has zero total float, is on both the precedence and resource critical paths,
and fits `PRIMARY_STREAM=1`. It unlocks safe removal of active v6 source by
first defining what is unsupported, what remains temporarily for recovery, and
which historical records are not current instructions.

The user subsequently authorized implementation. The fresh Task Start Record at
HEAD `0d4b5d80d7f50e330a3bc75d043a9c3479aea8db` re-enumerated the exact current
documents, scripts, package surfaces, and tests and returned `PASS` for
`V6_RESIDUAL_CONTRACT`. The wave-close record below supersedes its temporary
active state.

## Credible alternative orderings

### CLI parsing first

This would reduce malformed-input risk earlier. It is ready but has `TF=8.167d`
and is waiting for the single primary resource. Starting there would leave the
zero-slack v6 retirement chain unchanged and risks editing help/man boundaries
again after the v6 support contract is corrected. It remains owned, not
discarded.

### Shared-runtime deletion first

Deleting policy or context state before defining the retained recovery boundary
could remove evidence needed by offline consumers or create a v7 compatibility
facade to keep tests building. The accepted retirement plan requires consumer
enumeration and a truthful replacement boundary first.

### Placeholder removal first

This makes the tree smaller but removes the deterministic retirement diagnostic
while offline and documentation surfaces still refer to `kafs-v6`. It violates
the accepted final-phase ordering and increases operator ambiguity.

### Retain offline v6 diagnostics indefinitely

This avoids short-term deletion risk but preserves fixture creation, tests,
static-analysis population, and neutral-source ownership debt without an end
condition. The plan instead makes discovery of a real recovery obligation a
`REPLAN` trigger and otherwise closes the accepted retirement path.

## `V6_RESIDUAL_CONTRACT` closeout

- Closed at: `2026-07-22T17:18:38+09:00`
- Closeout HEAD: `3840ccd3c4099df8a24e7e478bfc822ddcf6c811`
  (two reviewed-scope WIP commits; plan closeout changes were still unstaged)
- Edge result: `V6_RESIDUAL_SCOPE_BOUND` is `state reached` and
  `V6_RESIDUAL_CONTRACT` is `status done`
- Decision: `PASS`; the selected capability edge is closed

Direct closeout evidence:

- `scripts/test-cli-surface.sh` rejects retired v6 workflows in current
  migration guidance and confirms both controlled-write operator scripts are
  absent.
- The residual query returns 81 current files, including its own inventory;
  every result has an owner and disposition in
  `sd-card-wear-v6-retirement-inventory-20260722.md`.
- `kafsresize` help and its manual no longer advertise v6 migration creation.
- Historical v6 records are visibly labeled and routed out of the current
  design path in `docs/INDEX.md`.
- `make -C src kafsresize`, `shellcheck scripts/test-cli-surface.sh`,
  `bash -n scripts/test-cli-surface.sh`, `./scripts/test-cli-surface.sh`, and the
  isolated `make -C tests check TESTS=kafsresize` all passed.

The first attempted milestone-only plan update failed validation because a
reached milestone cannot retain an unsatisfied incoming task. Adding the
truthful `status done` task state resolved that plan-model error. The successful
closeout run of
`./scripts/pert-next-task.sh plans/cli-v6-retirement.pert` reported:

```text
OK plans/cli-v6-retirement.pert project=KAFS_CLI_V6_RETIREMENT milestones=13 tasks=7 gates=6 resources=1

PRECEDENCE
MAKESPAN 13.833d
PRECEDENCE CRITICAL
TASKS V6_ACTIVE_COMMON_DECOUPLING, V6_OFFLINE_RETIREMENT, V6_FINAL_ENTRYPOINT_RETIREMENT, INTEGRATED_REMEDIATION_QUALIFICATION
GATES V6_RETIREMENT_REQUIRED
REPRESENTATIVE PATH V6_ACTIVE_COMMON_DECOUPLING -> V6_OFFLINE_RETIREMENT -> V6_FINAL_ENTRYPOINT_RETIREMENT -> V6_RETIREMENT_REQUIRED -> INTEGRATED_REMEDIATION_QUALIFICATION
PATH COUNT 1

RESOURCE SCHEDULE
ALGORITHM parallel-sgs@1 optimal=false
PRECEDENCE LOWER BOUND 13.833d
MAKESPAN 19.333d
DELAY 5.5d

RESOURCE CRITICAL
TASKS V6_ACTIVE_COMMON_DECOUPLING, V6_OFFLINE_RETIREMENT, V6_FINAL_ENTRYPOINT_RETIREMENT, CLI_FAIL_CLOSED_CONTRACT, MIGRATION_AUTOMATION_CONTRACT, INTEGRATED_REMEDIATION_QUALIFICATION
RESOURCE ARCS resource:V6_FINAL_ENTRYPOINT_RETIREMENT:CLI_FAIL_CLOSED_CONTRACT
REPRESENTATIVE PATH V6_ACTIVE_COMMON_DECOUPLING -> V6_OFFLINE_RETIREMENT -> V6_FINAL_ENTRYPOINT_RETIREMENT -> CLI_FAIL_CLOSED_CONTRACT -> MIGRATION_AUTOMATION_CONTRACT -> INTEGRATED_REMEDIATION_QUALIFICATION
PATH COUNT 1

ACTIVE
-

RUNNABLE NOW
V6_ACTIVE_COMMON_DECOUPLING priority=0 expected=4.333d TF=0d precedence_critical=true schedule_critical=true owner=KAFS implementation stream resources=PRIMARY_STREAM=1

READY / WAITING RESOURCE
CLI_FAIL_CLOSED_CONTRACT priority=0 expected=2.167d TF=6.167d precedence_critical=false schedule_critical=true owner=KAFS implementation stream resources=PRIMARY_STREAM=1
  PRIMARY_STREAM capacity=1 used=1 required=1 available=0 deficit=1 occupants=V6_ACTIVE_COMMON_DECOUPLING

BLOCKED NOW
-

UPCOMING
V6_OFFLINE_RETIREMENT expected=5.333d TF=0d
V6_FINAL_ENTRYPOINT_RETIREMENT expected=2d TF=0d
INTEGRATED_REMEDIATION_QUALIFICATION expected=2.167d TF=0d
MIGRATION_AUTOMATION_CONTRACT expected=3.333d TF=6.167d
```

The refreshed frontier therefore makes `V6_ACTIVE_COMMON_DECOUPLING` the only
`RUNNABLE NOW` zero-slack task. `CLI_FAIL_CLOSED_CONTRACT` remains ready but
waits for the single primary resource and has `TF=6.167d`; it is not promoted
ahead of the critical retirement path.

## `V6_ACTIVE_COMMON_DECOUPLING` Task Start and closeout

- Started from: branch `feat/v7-runtime-admission-foundation`, HEAD
  `fcb7fa632436250be4d21f3a8e1991e85cffdf53`, clean worktree
- Task source: freshly rerun
  `./scripts/pert-next-task.sh plans/cli-v6-retirement.pert`; the task was the
  only `RUNNABLE NOW` zero-slack node
- Start decision: `PASS`
- Closed at: `2026-07-22T17:56:51+09:00`
- Edge result: `V6_ACTIVE_COMMON_DECOUPLED` is `state reached` and
  `V6_ACTIVE_COMMON_DECOUPLING` is `status done`

The start gate confirmed that shared runtime branches and context fields still
carried retired v6 policy ownership, while v7 already had an independent
runtime-view and policy state. The neutral descriptor API remained required by
bounded offline fixtures, but its context fields and shard types were still
named as v6-owned. `lsp-cli` was installed but `clangd` was unavailable, so the
review used explicit reference searches plus warning-clean compilation and
regression tests rather than claiming an LSP-backed rename.

The wave preserved these boundaries:

- v4/v5 runtime behavior and the explicit v6 rejection diagnostic remain;
- v7 admission and worker suppression use only v7-owned validation;
- neutral descriptor mapping remains an offline diagnostic boundary and is
  not a v7 compatibility facade;
- offline v6 dump/fsck/fixtures and the final `kafs-v6` placeholder remain for
  their ordered successor waves.

Direct closeout evidence:

- `src/kafs_v6_fuse_init_policy.h` and `src/kafs_v6_fuse_policy.h` and their
  build references are deleted.
- Shared FUSE callbacks no longer contain v6 controlled-write gates or v6
  worker-policy state. The guarded v7 initialization path validates the
  v7-owned runtime view before suppressing background workers.
- Descriptor mapping fields, shard types, and helpers in common code now have
  neutral ownership; explicit v7 runtime view state remains separate.
- `scripts/check-v7-runtime-policy-ownership.sh` covers common/shared/v7 build
  paths, asserts the retired headers remain absent, and rejects v6 policy/state
  dependencies.
- The residual inventory query now returns 75 files including its own record,
  down from 81. Two policy headers were deleted and four active files left the
  result through neutral or v7-owned naming; the remaining matches keep an
  explicit negative, offline, historical, or final-placeholder disposition.

Validation passed:

- focused compile: `make -C src -j2 kafs kafs-v7 kafsctl kafs-back`
- focused tests: `make -C tests check TESTS='v7_entrypoint_smoketest v6_descriptor_validation journal_boundary'`
  (`3/3` passed)
- Autotools/build: `autoreconf -fi`, `./configure`, `make -j2`
- full regression: `make check -j2` (`41` passed, `7` FUSE-dependent tests not
  run by the existing harness)
- repository gates: `git diff --check`, `./scripts/format.sh`,
  `./scripts/lint.sh`, `./scripts/check-v7-runtime-policy-ownership.sh`,
  `./scripts/clones.sh`, and `./scripts/static-checks.sh`
- the unchanged 80-file strict clone population improved from 48 to 45 clones,
  490 to 466 duplicated lines, and 3546 to 3393 duplicated tokens; complexity
  NLOC/warnings moved from 46342/118 to 46011/116

The successful closeout run of
`./scripts/pert-next-task.sh plans/cli-v6-retirement.pert` reported:

```text
OK plans/cli-v6-retirement.pert project=KAFS_CLI_V6_RETIREMENT milestones=13 tasks=7 gates=6 resources=1

PRECEDENCE
MAKESPAN 9.5d
PRECEDENCE CRITICAL
TASKS V6_OFFLINE_RETIREMENT, V6_FINAL_ENTRYPOINT_RETIREMENT, INTEGRATED_REMEDIATION_QUALIFICATION
GATES V6_RETIREMENT_REQUIRED
REPRESENTATIVE PATH V6_OFFLINE_RETIREMENT -> V6_FINAL_ENTRYPOINT_RETIREMENT -> V6_RETIREMENT_REQUIRED -> INTEGRATED_REMEDIATION_QUALIFICATION

RESOURCE SCHEDULE
MAKESPAN 15d
DELAY 5.5d

RUNNABLE NOW
V6_OFFLINE_RETIREMENT priority=0 expected=5.333d TF=0d precedence_critical=true schedule_critical=true owner=KAFS implementation stream resources=PRIMARY_STREAM=1

READY / WAITING RESOURCE
CLI_FAIL_CLOSED_CONTRACT priority=0 expected=2.167d TF=1.833d precedence_critical=false schedule_critical=true owner=KAFS implementation stream resources=PRIMARY_STREAM=1
  PRIMARY_STREAM capacity=1 used=1 required=1 available=0 deficit=1 occupants=V6_OFFLINE_RETIREMENT

BLOCKED NOW
-
```

The refreshed frontier selects `V6_OFFLINE_RETIREMENT` as the sole runnable
zero-slack successor. This closeout records that result but does not authorize
implementation of the successor wave.

## `V6_OFFLINE_RETIREMENT` Task Start and closeout

- Started from: branch `feat/v7-runtime-admission-foundation`, HEAD
  `58ac5635741e140cdc034b45088801d2daf3d2ac`, clean worktree
- Task source: freshly rerun
  `./scripts/pert-next-task.sh plans/cli-v6-retirement.pert`; the task was the
  only `RUNNABLE NOW` zero-slack node
- Start decision: `PASS`
- Closed at: `2026-07-22T18:48:10+09:00`
- Edge result: `V6_OFFLINE_SURFACES_RETIRED` is `state reached` and
  `V6_OFFLINE_RETIREMENT` is `status done`

The start inventory found one descriptor implementation shared only by retired
v6 offline consumers, a test-only v6 mkfs target, two v6 descriptor suites,
descriptor journal/mapping branches, v6 fsck/dump reporting, and temporary
clone exclusions. V7 already owned its raw layout, recovery, runtime view, and
wire contract independently. No repository evidence established an external
recovery obligation beyond the accepted recreate-as-v7 policy; external image
holdings remain unverified and are still a replan trigger if contrary evidence
appears.

The completed boundary is:

- `kafs_descriptor_layout.h`, v6 fixture creation, two v6-only tests, and their
  build/helper wiring are deleted;
- common context, bitmap, inode, HRL, and journal code no longer owns a neutral
  descriptor compatibility path;
- production `kafs`, `kafs-v7`, `fsck.kafs`, and `kafsdump` explicitly reject a
  minimal v6 marker; v7 fsck detect-only output and usage-error semantics remain
  unchanged;
- the v5 metadata heatmap workload remains available while its v6 JSON mode is
  removed;
- historical documents remain labeled history, and current manuals no longer
  advertise v6 offline inspection;
- `kafs-v6` remains only as the final fail-closed packaged placeholder owned by
  `V6_FINAL_ENTRYPOINT_RETIREMENT`.

Review caught and corrected three boundary escapes before closeout: the v7 fsck
policy line was restored, the prior v7 repair rejection exit code `2` was
preserved and tested, and an over-broad deletion of the v5 metadata heatmap
script was reduced to removal of its v6-only mode.

Validation passed:

- Autotools/build: `autoreconf -fi`, `./configure`, `make -j2`
- focused regression:
  `make -C tests check TESTS='journal_boundary kafsresize v7_entrypoint_smoketest'`
  (`3/3` passed)
- full regression: `make check -j2` (`46/46` passed)
- ownership/UI: `./scripts/check-v7-layout-ownership.sh`,
  `./scripts/check-v7-runtime-policy-ownership.sh`, and
  `./scripts/test-cli-surface.sh`
- retained script: `bash -n scripts/metadata-heatmap-report.sh` and its help
  path
- repository gates: `git diff --check`, `./scripts/format.sh`, and
  `./scripts/static-checks.sh` (format, lint, clones, complexity)
- strict clone population: 80 files; 45 to 37 clones, 466 to 382 duplicated
  lines, and 3393 to 2812 duplicated tokens
- complexity: NLOC/warnings moved from 46011/116 to 42327/104

The closeout run of
`./scripts/pert-next-task.sh plans/cli-v6-retirement.pert` reported:

```text
OK plans/cli-v6-retirement.pert project=KAFS_CLI_V6_RETIREMENT milestones=13 tasks=7 gates=6 resources=1

PRECEDENCE
MAKESPAN 7.667d
PRECEDENCE CRITICAL
TASKS CLI_FAIL_CLOSED_CONTRACT, MIGRATION_AUTOMATION_CONTRACT, INTEGRATED_REMEDIATION_QUALIFICATION
GATES CLI_CONTRACT_REQUIRED

RESOURCE SCHEDULE
MAKESPAN 9.667d
DELAY 2d

RUNNABLE NOW
CLI_FAIL_CLOSED_CONTRACT priority=0 expected=2.167d TF=0d precedence_critical=true schedule_critical=true owner=KAFS implementation stream resources=PRIMARY_STREAM=1

READY / WAITING RESOURCE
V6_FINAL_ENTRYPOINT_RETIREMENT priority=0 expected=2d TF=3.5d precedence_critical=false schedule_critical=true owner=KAFS implementation stream resources=PRIMARY_STREAM=1

BLOCKED NOW
-
```

The refreshed frontier selects `CLI_FAIL_CLOSED_CONTRACT` as the sole runnable
zero-slack task. The final v6 packaged-placeholder retirement is ready but waits
for the single implementation resource and has 3.5 days precedence float; it
must not displace the calculated critical task.

## `CLI_FAIL_CLOSED_CONTRACT` Task Start and closeout

- Recorded at: `2026-07-22T19:13:35+09:00`
- Started from: branch `feat/v7-runtime-admission-foundation`, HEAD
  `9839b5cd3b35f27f13c8dc57b08f8c3ac6adc790`, clean worktree
- Task source: freshly rerun
  `./scripts/pert-next-task.sh plans/cli-v6-retirement.pert`; the task was the
  only `RUNNABLE NOW` zero-slack node
- Start decision: `PASS`
- Closed at: `2026-07-22T19:26:55+09:00`
- Edge result: `CLI_INPUT_CONTRACT_STRICT` is `state reached` and
  `CLI_FAIL_CLOSED_CONTRACT` is `status done`

Direct checks reproduced three accepted review findings before implementation:

- `--inodes 4junk` reached a successful migrate-create dry-run because its
  parser did not check the numeric suffix;
- `--grow` accepted `--inodes` and entered the runtime path; and
- a second positional image silently replaced the first and entered the
  runtime path.

Source review also found that path-valued options in the lifecycle rehearsal
and evidence gate checked only the remaining argument count. A following
option token could therefore be consumed as the missing value. This refuted the
initial assumption that those two shell parsers needed tests only, so the wave
was rebaselined to include their value boundary while preserving the same
capability scope.

The implementation boundary is strict numeric syntax and range detection,
single-mode option masks, exact positional arity, missing-value rejection,
mode-aware help/manual/completion, and negative regression for `kafsresize`,
the disposable rehearsal, and the validate-only evidence gate. All input
validation must finish before an image or report path is created. Machine
output schema/versioning, replay/report semantics, exit 77 documentation, the
final v6 placeholder, VHDX/WSL work, and physical-media qualification remain
non-goals of this wave.

The completed boundary is:

- `kafsresize` strictly parses nonzero sizes, bounded positive integer fields,
  finite ratios, and suffix multiplication without overflow;
- exactly one mode and one grow image operand are required, and each mode has
  an explicit allowed/required option mask evaluated before runtime work;
- path-valued options reject missing or option-like values in the disposable
  rehearsal and validate-only evidence gate before report directories or
  evidence are accessed;
- help, `kafsresize(8)`, and bash completion now expose mode-specific behavior;
  and
- regression covers malformed and overflowing numbers, missing values, extra
  operands, mode-inapplicable options, absence of destination creation, shell
  value boundaries, and mode-aware completion.

Validation passed:

- build: `make -j2`
- focused `kafsresize`, migration evidence gate, and disposable migration
  rehearsal regressions
- full regression: `make check -j2` (`46/46` passed)
- semantic checks: `lsp-cli` symbols plus zero diagnostics for
  `src/kafsresize.c` and `tests/tests_kafsresize.c`
- CLI and shell checks: `./scripts/test-cli-surface.sh` and `bash -n` for both
  modified migration scripts
- repository gates: `git diff --check` and `./scripts/static-checks.sh`
  (format, lint, clones, complexity)
- strict clone population stayed at 80 files, 37 clones, 382 duplicated lines,
  and 2812 duplicated tokens; complexity moved from 42327/104 to 42493/104
  NLOC/warnings

The closeout run of
`./scripts/pert-next-task.sh plans/cli-v6-retirement.pert` reported:

```text
OK plans/cli-v6-retirement.pert project=KAFS_CLI_V6_RETIREMENT milestones=13 tasks=7 gates=6 resources=1

PRECEDENCE
MAKESPAN 5.5d
PRECEDENCE CRITICAL
TASKS MIGRATION_AUTOMATION_CONTRACT, INTEGRATED_REMEDIATION_QUALIFICATION
GATES CLI_CONTRACT_REQUIRED

RESOURCE SCHEDULE
MAKESPAN 7.5d
DELAY 2d

RUNNABLE NOW
MIGRATION_AUTOMATION_CONTRACT priority=0 expected=3.333d TF=0d precedence_critical=true schedule_critical=true owner=KAFS implementation stream resources=PRIMARY_STREAM=1

READY / WAITING RESOURCE
V6_FINAL_ENTRYPOINT_RETIREMENT priority=0 expected=2d TF=1.333d precedence_critical=false schedule_critical=true owner=KAFS implementation stream resources=PRIMARY_STREAM=1

BLOCKED NOW
-
```

The refreshed frontier selects `MIGRATION_AUTOMATION_CONTRACT` as the sole
runnable zero-slack task. Final v6 placeholder retirement remains ready but has
1.333 days precedence float and waits for the single implementation resource.

## `MIGRATION_AUTOMATION_CONTRACT` Task Start and closeout

The task-start refresh ran on branch `feat/v7-runtime-admission-foundation` at
HEAD `3af5ce0`. The tracked worktree was clean; the focused preflight had left
only its untracked test executable. The branch was five commits ahead of its
configured upstream. The candidate came from the same-day migration command UI
review, but was accepted only after the current plan passed
`./scripts/pert-next-task.sh plans/cli-v6-retirement.pert` and selected
`MIGRATION_AUTOMATION_CONTRACT` as its sole runnable zero-slack task.

The refreshed implementation evidence showed three automation consumers with
different, unversioned result surfaces: importer success was human text,
rehearsal status was spread across text and retained artifacts, and evidence
validation exposed only human diagnostics. Existing regressions depended on
those strings. The inherited need for a stable machine-readable contract and
for retaining human-readable defaults therefore remained valid. Partial-resume
wording was contradicted by the implementation, which can only replay from the
frozen source. Production acceptance, physical-media behavior, and native
Windows recovery remained unknown and outside this repository-only wave.

The relevant states were PASS, FAIL, and rehearsal SKIP; the variability
dimensions were human versus JSON output, dry-run versus execution, report
allocation, cleanup success, prerequisite availability, and validation
success. The invariants were fail-closed parsing, one versioned schema per
surface, truthful exit status and retention claims, diagnostics on stderr, no
partial resume, and no claim of production cutover or lifecycle acceptance.
Exit required schema-parsing regressions for all three surfaces, preserved
default human output where justified, documented prerequisites/report
location/retention/exit 77, full-replay terminology, proportional repository
validation, and a refreshed PERT frontier. V6 entrypoint removal, WSL or VHDX
operations, physical-media qualification, production migration, and cutover
were explicit non-goals. The Task Start Gate decision was `PASS`.

The completed boundary adds these stable result contracts:

- `KAFS.V5V7MigrationImportResult.v1` for `kafsresize --migrate-import-v7
  --json`;
- `KAFS.V5V7MigrationRehearsalResult.v1`, retained as `result.json` after a
  rehearsal report is allocated; and
- `KAFS.V5V7MigrationEvidenceValidation.v1` for evidence validation with
  `--json`.

Importer and evidence-validation human output remains the default. Rehearsal
reports now describe full replay from the frozen source, expose actual report
and work-directory retention, distinguish PASS/FAIL/SKIP with exit status
0/1/77, and retain the importer and evidence-validator JSON results. CLI usage
errors remain exit 2 before a result exists. The manual, completion, README,
cutover playbook, and migration evidence contract document these semantics; the
playbook is now included in `make dist`.

Validation passed:

- `make -j2` and the focused `kafsresize`, v5-to-v7 importer, evidence-gate,
  and disposable-rehearsal regressions;
- `bash -n` for the modified shell scripts and regressions;
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 45 tests passed and
  `stress_fs` self-reported SKIP after its mount attempt failed;
- `lsp-cli` diagnostics for `src/kafsresize.c`: none;
- `./scripts/format.sh`, `./scripts/lint.sh`, `./scripts/clones.sh`, and
  `./scripts/static-checks.sh`: PASS;
- strict clone population: 80 files, 37 clones, 382 duplicated lines, and
  2812 duplicated tokens; complexity: 42582 NLOC and 104 warnings, with
  `cmd_migrate_import_v7` at CCN 15;
- `make dist`: PASS, including the scripts, tests, source, manual, completion,
  README, migration evidence document, and cutover playbook; and
- `git diff --check`: PASS.

No PowerShell, VHDX, WSL terminate/shutdown, physical-media, production-source,
production-mount, or cutover action ran.

After marking `MIGRATION_AUTOMATION_CONTRACT_READY` reached and the task done,
the closeout run reported:

```text
OK plans/cli-v6-retirement.pert project=KAFS_CLI_V6_RETIREMENT milestones=13 tasks=7 gates=6 resources=1

PRECEDENCE MAKESPAN 4.167d
PRECEDENCE CRITICAL
TASKS V6_FINAL_ENTRYPOINT_RETIREMENT, INTEGRATED_REMEDIATION_QUALIFICATION
GATES V6_RETIREMENT_REQUIRED

RESOURCE SCHEDULE MAKESPAN 4.167d
DELAY 0d

RUNNABLE NOW
V6_FINAL_ENTRYPOINT_RETIREMENT priority=0 expected=2d TF=0d precedence_critical=true schedule_critical=true owner=KAFS implementation stream resources=PRIMARY_STREAM=1

READY / WAITING RESOURCE
-

BLOCKED NOW
-

UPCOMING
INTEGRATED_REMEDIATION_QUALIFICATION priority=0 expected=2.167d TF=0d precedence_critical=true schedule_critical=true owner=KAFS implementation stream resources=PRIMARY_STREAM=1
```

The migration automation dependency is closed. The new sole runnable critical
frontier is `V6_FINAL_ENTRYPOINT_RETIREMENT`; this closeout does not implement
that separate wave.

## Refresh triggers

Refresh this plan and rerun all three `perttool` commands when any of the
following changes:

- HEAD or relevant UI/runtime/offline consumer evidence;
- discovery of an external v6 recovery obligation;
- output schema or compatibility decision;
- resource capacity;
- completion or scope change of any wave; or
- the accepted remediation goal.
