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

## Coding Conventions

- Follow `.clang-format`; use `./scripts/format.sh fix` for C formatting.
- Keep C changes warning-clean under the existing `-Wall -Werror` build flags.
- Avoid introducing code clones. Extract helpers when repeated logic becomes meaningful.
- Prefer structured parsing and existing helper APIs over ad hoc string handling.
- If `Makefile.am`, `configure.ac`, or other Autotools inputs change, refresh generated files with `autoreconf -fi` or explain why generated outputs were intentionally left untouched.

## Architecture Guardrails

- New images default to on-disk format v5. Runtime mount continues to support existing v4 images.
- Legacy v2/v3 images require explicit offline migration before use.
- For filesystem geometry changes such as size or inode count, prefer offline rebuild/migration via `kafsresize --migrate-create` over in-place metadata relocation.
- Treat in-place inode-table expansion as out of scope unless the user explicitly asks for that high-risk migration path.
- For performance optimization, prefer enabling LTO (`./configure --enable-lto`) before removing `static inline` hints wholesale.
- Do not change IPC/transport choices, control-plane paths, permission/security boundaries, or introduce long-running services/daemons without asking the user to choose the design direction first.

## Locking Policy

- Follow `.github/lock-policy.md`.
- Acquire locks only in ascending rank order:
  1. `hrl_global` rank 10
  2. `inode_alloc` rank 20
  3. `inode` rank 30
  4. `hrl_bucket` rank 40
  5. `bitmap` rank 50
- Unlock in strict reverse order.
- New lock classes must declare an explicit rank and be inserted into the policy.
- Never introduce lock-order inversions, silent infinite lock waits, or code paths that hide stale-owner diagnostics.
- Do not use `KAFS_CALL` after acquiring a lock; capture `rc` and use a single unlock path.

## Git And Safety

- Respect `.github/github-dev-rules.md` for branch, commit, PR, CI, review, and release expectations.
- For GitHub issue/PR implementation work, use a purpose-named branch and, when parallel work is needed, create worktrees under `.worktree/<worktree_name>`.
- Do not run destructive commands such as `git reset --hard`, `git checkout -- <path>`, `git clean -fdx`, or broad `rm -rf` against this repository unless the user explicitly approves the exact target and impact.
- Do not make network calls unless required by the task.
- Do not exfiltrate secrets or include secret values in logs, reports, commits, or docs.
