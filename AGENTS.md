# AGENTS.md

## Repository Profile

- Default response language is Japanese unless the user asks otherwise.
- This is `kafs`, a C/Autotools FUSE filesystem with image tools, fsck, resize/migration utilities, hotplug control, and regression tests.
- Prefer repository scripts and Makefile targets over ad hoc commands.
- Keep reports evidence-based: separate directly observed facts from inference, and state uncertainty for unverified claims.

## Project Map

- `src/`: filesystem runtime and command-line tools.
- `tests/`: Automake regression tests. Tests should create temporary workdirs under `${TMPDIR:-/tmp}` and avoid polluting the repo tree.
- `scripts/`: validation, formatting, clone detection, lint, benchmarks, and reproduction helpers.
- `docs/`: design notes, operator guidance, validation reports, and release notes.
- `man/` and `completions/`: user-facing command documentation and shell completions.
- `.github/`: CI workflows, GitHub development rules, lock policy, and legacy Copilot/agent instructions.

## Diagnostic And Causal-Reasoning Gate

- Apply this gate when diagnosing a failure, reporting causality, recommending
  recovery, changing state based on a diagnosis, or writing an RCA or
  retrospective. Do not replace it with confidence wording alone.
- Maintain an evidence ledger for each non-trivial diagnostic wave:
  1. directly observed facts with source, time, process/session identity, and
     relevant scope;
  2. competing hypotheses, including a credible alternative;
  3. the discriminating check and the result that would support or weaken each
     hypothesis;
  4. the current status of each claim: `CONFIRMED`, `INFERRED`, `UNKNOWN`, or
     `REFUTED`; and
  5. which downstream conclusions and proposed actions depend on each premise.
- Do not generalize evidence across different processes, sessions, hosts,
  configuration layers, versions, or times without verifying that the relevant
  state is shared. The same error code or message in different lifecycle stages
  is correlation, not causation.
- Treat a user objection and an agent assertion by the same evidence standard.
  Convert an objection into a testable claim, identify the conflicting premise,
  and verify it. Do not preserve a claim to appear consistent, and do not
  reverse it merely to agree with the user.
- When evidence refutes or materially weakens a premise, explicitly invalidate
  every dependent conclusion and action, then rebaseline from the remaining
  evidence. Do not repair a causal narrative one sentence at a time.
- Keep diagnostic work on the shortest path to the accepted incident goal.
  Record adjacent anomalies with an owner and disposition, but do not promote
  them to causes without a discriminating check. For current, version-specific,
  or unfamiliar failures, search primary sources and upstream issue trackers
  early when local evidence cannot distinguish the hypotheses.
- Before any diagnostic mutation, publish a `Diagnostic Change Record` with:
  1. the confirmed or explicitly provisional causal link;
  2. the predicted observable result;
  3. side effects and evidence that the change may destroy;
  4. rollback or recovery; and
  5. why a narrower read-only check is insufficient.
  Do not mutate state when the primary causal link is unknown unless the user
  explicitly chooses a labeled containment experiment after seeing the risks.
  Label restart, disablement, cleanup, and bypass actions as workaround,
  containment, recovery, or root fix; do not conflate them.
- A retrospective or error-chain analysis must begin with a chronological
  ledger. For each step, record what was known then, the decision made then,
  missing evidence, and the downstream effect. Only after that ledger may the
  report group themes or discuss behavioral tendencies. Keep the technical
  failure chain separate from the reasoning/response failure chain, identify
  the exact breakpoint and control that would sever each chain, and preserve
  unresolved unknowns rather than rewriting the sequence as a success story.
- Describe psychological background only as an evidence-linked behavioral
  tendency such as anchoring, premature causal closure, confirmation bias,
  action bias, deference, or hindsight reconstruction. Do not treat either the
  user's confidence or the agent's confidence as evidence.

## Task Start Gate

- Treat a handoff, backlog item, ticket, or prior next-task recommendation as a
  candidate starting point, not as implementation authorization or a current
  definition of done.
- Before editing files for any non-trivial implementation or refactor, refresh
  the evidence from the current checkout and publish a concise Task Start Record
  in the working update. The primary agent owns this gate; it does not depend on
  a subagent being available.
- The Task Start Record must contain:
  1. the current branch, HEAD, worktree state, and the source/freshness of the
     proposed task;
  2. directly observed evidence from the affected code, analogous existing
     implementations, relevant specifications, and tests;
  3. which inherited assumptions remain valid, are contradicted, or remain
     unknown;
  4. the relevant states, variability dimensions, invariants, and semantic
     boundaries;
  5. exit criteria and explicit non-goals re-derived from the refreshed
     evidence rather than copied from the handoff; and
  6. a `PASS`, `REPLAN`, or `BLOCKED` start decision with rationale.
- Do not begin implementation on `REPLAN` or `BLOCKED`. For `REPLAN`, replace
  the candidate with a task whose boundary closes a coherent capability. Do not
  split production code by test example, numeric instance, or fixture shape
  when the state transition and invariants are shared.
- Re-run the start gate when HEAD, relevant evidence, assumptions, or requested
  scope changes materially. Narrow, self-contained fixes may use an abbreviated
  record, but must still state current evidence and exit criteria before edits.

## Goal And Critical Path Gate

- Build or refresh a capability-level `perttool` plan before naming,
  recommending, accepting, or assigning priority to the next implementation
  wave. The repository default is `plans/current.pert`; a scoped replacement
  must be named in the selection record. A plan is a selection prerequisite,
  not a justification added after a candidate has already been proposed or
  accepted. Follow `docs/pert-task-selection.md`.
- `perttool` is the calculation and task-classification authority for this
  gate. After refreshing the plan from current evidence, run
  `./scripts/pert-next-task.sh [plan.pert]`, which executes `dsl check`,
  `dag analyze --schedule both`, and `dag next` in that order. A hand-written
  table, Mermaid graph, backlog ordering, or mental calculation may explain the
  result but may not replace or override these commands.
- Do not name a selected task when `perttool` is unavailable, the plan fails
  `dsl check`, the plan is stale relative to the accepted goal or current
  evidence, or the candidate is absent from `RUNNABLE NOW`. Return `REPLAN` for
  an invalid or incomplete model and `BLOCKED` when the valid model has no
  runnable critical or least-slack task.
- Define PERT nodes as goal-relevant capability or mandatory correctness and
  durability outcomes. Do not use files, functions, findings, tickets, test
  examples, or small convenient diffs as nodes unless they independently unlock
  a downstream capability.
- The `.pert` input and its companion selection record must contain:
  1. the accepted end goal and current capability position;
  2. every credible goal path and causal predecessor edge known from current
     evidence;
  3. optimistic, most-likely, and pessimistic implementation-effort estimates
     (`O`, `M`, `P`), estimate confidence, and truthful resource capacities;
  4. `perttool`-calculated expected duration, earliest/latest position, total
     float, critical paths, unresolved prerequisites, and external blockers;
  5. `dag next` active, ready, `RUNNABLE NOW`, `BLOCKED NOW`, and upcoming
     classifications, plus the capability each runnable node unlocks; and
  6. comparison with credible alternative orderings, including future rework or
     qualification debt created by each ordering.
- Select only from `dag next`'s `RUNNABLE NOW` zero-slack or least-slack
  frontier, using the resource capacity encoded in the plan. `priority` may
  express an evidence-backed product constraint but may not encode local ease
  or force a preselected candidate. When a critical node is externally
  blocked, keep it in the network and first select an
  unblocked predecessor, blocker-reduction activity, or other node that reduces
  critical-path duration. A non-critical node may be selected only when no such
  critical work is runnable and the PERT record shows its slack, opportunity
  cost, and reason for using the capacity. Never replace a blocked critical node
  with the locally easiest task by default.
- Correctness, data-integrity, and durability findings are mandatory predecessor
  nodes only when current evidence identifies the causal capability edge they
  protect. The fact that a finding is real or a change is safe is not by itself
  evidence that it has the next priority.
- Only after PERT selects a wave may the Task Start Gate derive its
  implementation boundary and decide `PASS`, `REPLAN`, or `BLOCKED`. A coherent,
  useful, or low-risk candidate does not pass priority selection merely because
  it can be implemented safely.
- Refresh the `.pert` network and rerun all three `perttool` commands before
  every next-task proposal and after every wave closeout, and whenever the
  accepted goal, HEAD, relevant evidence,
  dependencies, estimates, or blocker state changes. Do not carry a previous
  candidate across those events without recomputation.
- Static-analysis findings, age, authorship, existing ticket order, and code
  proximity may inform risk and effort but may not determine wave priority.
  Keep correctness, data-integrity, and durability findings as mandatory owned
  constraints regardless of where they were introduced, but require the PERT
  record to identify their causal edge before they determine next-wave order.
- Keep every confirmed finding owned and assigned a disposition even when it is
  off the current critical path. Off-path does not mean unrelated, accepted, or
  exempt from recovery.
- At each wave closeout, verify the dependency was actually closed, update the
  stored task/milestone state, rebuild the `perttool` calculations from current
  evidence, and publish the new `dag next` frontier before proposing the next
  wave. Do not advance mechanically from a prior plan, handoff, nearby finding,
  or the shape of the completed implementation.
- Use history and `git blame` only as BlameCheck evidence for design intent,
  constraints, and change context. Never use authorship, age, or provenance to
  transfer responsibility, lower priority, or exclude a finding.
- When executing countermeasures from a completed RCA, keep the already accepted
  product goal as an input; do not reopen goal selection or require a
  comprehensive current-capability inventory before recovery can start.
  Inventory the RCA actions and plausible same-cause impact first, use only the
  capability context needed to measure their distance to the goal, and then
  derive waves from that evidence.
- Recovery-wave boundaries are not fixed inputs. Merge, split, remove, or add
  waves when doing so closes goal-relevant dependencies with less rework. Keep
  off-path RCA findings owned and deferred with a disposition rather than
  excluding them. After the necessary RCA countermeasures close, perform the
  comprehensive capability rebaseline and derive subsequent product work.

## Build And Test

- Bootstrap and default build:

  ```sh
  autoreconf -fi
  ./configure
  make -j2
  ```

- Default test gate:

  ```sh
  make check -j2
  ```

- Debug build:

  ```sh
  ./configure --enable-debug-build
  make -j2
  ```

- Release/performance-oriented build:

  ```sh
  ./configure --enable-lto
  make -j2
  ```

- Static/PR gates:

  ```sh
  ./scripts/format.sh
  ./scripts/lint.sh
  ./scripts/clones.sh
  ./scripts/static-checks.sh
  ```

- `./scripts/deadcode.sh` is optional for normal PR/update gates and required for release gates.
- Some FUSE tests may need `/dev/fuse`, mount permissions, or longer startup waits. Use `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check` when debugging slow mounts.
- The TSan workflow builds with clang/ThreadSanitizer and only runs the non-FUSE smoke test:

  ```sh
  CC=clang CFLAGS='-O1 -g -fsanitize=thread -fno-omit-frame-pointer' LDFLAGS='-fsanitize=thread' ./configure
  make -j2
  make -C tests hrl_smoketest
  ./tests/hrl_smoketest
  ```

## Verification Expectations

- After code edits, run the narrowest relevant build/test first, then broaden to `make check -j2` when the change touches shared filesystem behavior, image format, locking, RPC/control paths, or test utilities.
- After formatting-sensitive C changes, run `./scripts/format.sh` or `./scripts/format.sh fix` as appropriate.
- After broad PR/update work, run clone/static gates and report PASS/FAIL with the exact commands that ran.
- If a tool is missing or a sandbox/permission issue prevents verification, report that explicitly and include the command that could not run.
- These are completion and escape-detection controls. They do not replace the
  Task Start Gate, Goal And Critical Path Gate, or justify an implementation
  boundary by themselves.

## Codex Project Setup

- Project-scoped Codex config lives in `.codex/config.toml` and loads only after Codex trusts this repository.
- Custom agents live in `.codex/agents/` and should be used only when the user explicitly asks for subagents or parallel agent work.
- Available agents:
  - `implementer`: scoped implementation plus validation.
  - `reviewer`: read-only correctness, regression, locking, and test review.
  - `gatekeeper`: read-only PASS/FAIL gate decisions from evidence.
  - `orchestrator`: read-only task breakdown and delegation planning.
  - `progress-manager`: read-only milestone/docs/progress consistency checks.
  - `github-rules`: GitHub workflow and policy documentation maintenance.
  - `agent-coordinator`: Codex/GitHub agent definition maintenance.
  - `consistency-reviewer`: read-only consistency, symmetry, completeness, and counterpart-case review.
  - `plain-reviewer`: read-only non-expert clarity and operational-usability review.
  - `domain-expert-reviewer`: read-only filesystem/storage expert review.
- Parallel review trigger: when the user asks for multi-perspective or parallel review, or explicitly approves it for a broad/risky pre-commit or PR review, use the `kafs-parallel-review` skill and run `consistency-reviewer`, `plain-reviewer`, and `domain-expert-reviewer` in parallel.
- Manual or hook-style entrypoint: `./scripts/codex-parallel-review.sh --target working-tree` writes ignored reports under `report/codex-review/`. Use `--dry-run` first when wiring it into local hooks or automation.

## Coding Conventions

- Follow `.clang-format`; use `./scripts/format.sh fix` for C formatting.
- Keep C changes warning-clean under the existing `-Wall -Werror` build flags.
- Avoid introducing code clones. Extract helpers when repeated logic becomes meaningful.
- For refactoring, actively use `lsp-cli` with `clangd` when available. Prefer
  semantic checks such as `symbols`, `references`, `definition`, `hover`, and
  rename dry-runs before broad textual edits; apply edits only after reviewing
  the planned workspace changes.
- Keep `compile_commands.json` current for LSP-backed refactoring. When it is
  missing or stale, regenerate it with the repository build flow, for example
  `bear -- make -j2` after a clean or relevant rebuild.
- Prefer structured parsing and existing helper APIs over ad hoc string handling.
- If `Makefile.am`, `configure.ac`, or other Autotools inputs change, refresh generated files with `autoreconf -fi` or explain why generated outputs were intentionally left untouched.

## Architecture Guardrails

- New images default to on-disk format v5. Runtime mount continues to support existing v4 images.
- Legacy v2/v3 images require explicit offline migration before use.
- Treat v5-and-earlier, v6, and v7 as separate ownership boundaries. A
  format-specific resource may not be directly reused by another format as a
  shortcut.
- Format v7 is a breaking-change boundary, not a v6 compatibility layer. Do
  not preserve old v6 wire/API behavior in v7 unless the user explicitly asks
  for a compatibility exception.
- Format-specific entrypoints must remain explicit: production `kafs` owns
  v4/v5, the temporary `kafs-v6` placeholder rejects retired v6 runtime use,
  and `kafs-v7` owns v7. Do not
  route successful v7 admission through `kafs.c`, `kafs-v6`, or v6-owned
  admission/layout entrypoints.
- When v7 needs logic that currently lives in v5/v6-owned files, first copy it
  into v7-owned files or extract a clearly neutral helper with no v5/v6 public
  entrypoint dependency. Avoid premature "common" names until ownership is
  clear.
- Format v6 is under phased source retirement, not maintenance. Do not add v6
  compatibility, features, parity refactors, or shared abstractions whose
  purpose is to preserve v6 behavior. Follow
  `docs/sd-card-wear-v6-retirement-plan.md`: first replace independent,
  clone-heavy runtime surfaces with fail-closed placeholders; then decouple
  active neutral/v7 code from v6-owned layout and policy types before deleting
  those sources; remove the `kafs-v6` entrypoint, packaging, documentation, and
  tests only in the final phase. Temporary static-analysis exclusions must
  shrink as the corresponding v6 sources are deleted.
- For filesystem geometry changes such as size or inode count, prefer offline rebuild/migration via `kafsresize --migrate-create` over in-place metadata relocation.
- Treat in-place inode-table expansion as out of scope unless the user explicitly asks for that high-risk migration path.
- For performance optimization, prefer enabling LTO (`./configure --enable-lto`) before removing `static inline` hints wholesale.
- Do not change IPC/transport choices, control-plane paths, permission/security boundaries, or introduce long-running services/daemons without asking the user to choose the design direction first.

## Locking Policy

- Follow `.github/lock-policy.md`.
- Acquire locks only in ascending rank order:
  1. `v7_write_gate` rank 1
  2. `v7_sequence` rank 2
  3. `v7_group` rank 3
  4. `hrl_global` rank 10
  5. `inode_alloc` rank 20
  6. `inode` rank 30
  7. `hrl_bucket` rank 40
  8. `bitmap` rank 50
- Unlock in strict reverse order.
- New lock classes must declare an explicit rank and be inserted into the policy.
- Never introduce lock-order inversions, silent infinite lock waits, or code paths that hide stale-owner diagnostics.
- Do not use `KAFS_CALL` after acquiring a lock; capture `rc` and use a single unlock path.

## Review And Commit Workflow

- Follow the reviewed-scope WIP workflow in `.github/github-dev-rules.md` for
  implementation work.
- Review changes in explicit file or hunk units. Once a unit is reviewed, stage
  only that unit and create a `WIP: review <scope>` commit whose body records a
  `Reviewed-scope:` file/function/section description and relevant
  `Validation:` evidence.
- Do not mix unreviewed changes into a reviewed-scope WIP commit.
- When one logical final commit unit is complete, consolidate its WIP commits
  with squash/fixup, amend, or interactive rebase, re-review the resulting
  diff, rerun proportional validation, and replace the WIP history with a
  normal final commit.
- Do not leave `WIP:` commits in PR-ready history. Rewrite history only on an
  unshared working branch unless the user explicitly approves rewriting the
  identified shared commits.

## Git And Safety

- Respect `.github/github-dev-rules.md` for branch, commit, PR, CI, review, and release expectations.
- For GitHub issue/PR implementation work, use a purpose-named branch and, when parallel work is needed, create worktrees under `.worktree/<worktree_name>`.
- Do not run destructive commands such as `git reset --hard`, `git checkout -- <path>`, `git clean -fdx`, or broad `rm -rf` against this repository unless the user explicitly approves the exact target and impact.
- Do not make network calls unless required by the task.
- Do not exfiltrate secrets or include secret values in logs, reports, commits, or docs.
