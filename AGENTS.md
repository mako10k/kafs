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

- Derive implementation waves from the shortest dependency path to the current
  accepted end goal, not from file boundaries, finding counts, apparent cleanup
  convenience, or the previous wave's local shape.
- Before assigning priority or starting a wave, extend the Task Start Record
  with:
  1. the accepted end goal and current capability position;
  2. the capability dependency graph and unresolved prerequisites;
  3. the dependency this wave closes and the downstream capabilities it
     unlocks;
  4. comparison with credible alternative orderings; and
  5. an explicit local-optimum check showing that the wave shortens the path to
     the goal rather than only improving an isolated component.
- Static-analysis findings, age, authorship, existing ticket order, and code
  proximity may inform risk and effort but may not determine wave priority.
  Correctness, data-integrity, and durability findings are mandatory path
  constraints, regardless of where they were introduced.
- Keep every confirmed finding owned and assigned a disposition even when it is
  off the current critical path. Off-path does not mean unrelated, accepted, or
  exempt from recovery.
- At each wave closeout, verify the dependency was actually closed, refresh the
  graph from current evidence, and recompute the shortest path before selecting
  the next wave. Do not advance mechanically from a prior plan or handoff.
- Use history and `git blame` only as BlameCheck evidence for design intent,
  constraints, and change context. Never use authorship, age, or provenance to
  transfer responsibility, lower priority, or exclude a finding.

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
  v4/v5, `kafs-v6` owns frozen experimental v6, and `kafs-v7` owns v7. Do not
  route successful v7 admission through `kafs.c`, `kafs-v6`, or v6-owned
  admission/layout entrypoints.
- When v7 needs logic that currently lives in v5/v6-owned files, first copy it
  into v7-owned files or extract a clearly neutral helper with no v5/v6 public
  entrypoint dependency. Avoid premature "common" names until ownership is
  clear.
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
