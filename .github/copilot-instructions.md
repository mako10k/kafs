# Copilot Agent Instructions

Mandatory rules only.

- Default locale: `ja`.
- Identity: reply `GitHub Copilot` when asked your name.
- Follow repository rules in `.github/github-dev-rules.md`.
- Respect repository scripts and Makefile targets.
- Build and run tests after code edits; report PASS/FAIL with deltas.
- Avoid introducing code clones; prefer helper extraction/refactoring when logic repeats.
- For PR/update gates, run clone/static checks and report PASS/FAIL:
  - `./scripts/clones.sh`
  - `./scripts/static-checks.sh` (or equivalent component checks)

Project direction (mandatory):
- Performance optimization baseline: prefer enabling LTO (`./configure --enable-lto`) over removing `static inline` hints wholesale.
- Filesystem geometry changes (size/inode count): prefer offline rebuild/migration workflow (`kafsresize --migrate-create`) over in-place metadata relocation.
- Treat in-place inode-table expansion as out-of-scope unless user explicitly requests high-risk migration logic.
- For flaky long-running checks, use timeout-controlled script paths and report which step timed out/failed instead of leaving checks hanging.

Lock policy (mandatory):
- Follow lock rank order defined in `.github/lock-policy.md`.
- Never introduce lock-order inversions.
- Do not allow silent infinite lock waits; preserve diagnostics and fail-fast behavior on stale owner detection.

Supplemental guidance:
- `.github/copilot-guidelines.md`
- `.github/lock-policy.md`

Security and safety:
- No network calls unless required by the task.
- Do not exfiltrate secrets.

## Decision checkpoints (must ask user)

When a change affects system boundaries or control paths, the agent MUST ask before implementing.
Treat these as design decisions, not minor edits:
- IPC/transport choices (ioctl vs RPC, UDS vs socketpair, signals, etc.).
- Control-plane paths (how kafsctl talks to kafs-front/back).
- Permission or security boundary changes.
- New long-running services or daemons.

If any of the above applies:
1) Describe the options and trade-offs succinctly.
2) Ask for explicit user selection.
3) Proceed only after confirmation.

## Guardrails for destructive commands

To avoid accidental data loss, the agent MUST follow these rules:

- **Never run destructive commands** (examples: `git reset --hard`, `git checkout -- <path>`, `git clean -fdx`, `rm -rf`, mass deletes) **against the WorkspaceRoot or repository checkout** unless the user explicitly approves after being shown the exact target path and the impact.
- Default mode is **read-only** for the WorkspaceRoot: use `git status`, `git diff`, `git fsck`, log inspection, etc.
- If a destructive operation is necessary, the agent must first:
  1) show the exact command, target path(s), and what will be lost
  2) propose a non-destructive alternative (backup/patch/dry-run)
  3) obtain explicit user confirmation
