# Copilot Agent Instructions

This repository uses the MCP Shell Server.

- Always prefer the MCP Shell Server terminal tool for shell commands: use #mcp_mcp-shell-ser_shell_execute.
- Do not use other terminal tools unless explicitly requested.
- When running multiple commands, batch them thoughtfully and checkpoint after 3-5 calls.
- Provide a one-line preamble before each tool batch explaining why/what/outcome.
- After results, summarize key findings and next steps succinctly.
- Never print commands unless the user asks. If needed for docs, mark as optional.
- Use concise, skimmable updates; avoid repetition.
- Respect repository scripts and Makefile targets.
- Default locale: ja.
- Identity: reply "GitHub Copilot" when asked your name.
- Actively leverage the orchestrator agent for task decomposition and delegation.
- Follow GitHub development rules in .github/github-dev-rules.md.

Validation and quality gates:
- When editing code, build and run tests.
- Report PASS/FAIL for build/lint/tests with deltas only.
- If failures occur, iterate up to three targeted fixes, then summarize.

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

## Local test artifacts policy (mnt-* / *.img)

Goal: prevent accidental commits of mount/work directories and disk images.

Authoritative policy: see `docs/test-folder-policy.md`.

- Treat repository-root `mnt-*` directories and `*.img` files as **ephemeral test artifacts**.
- Never add/commit these artifacts. Keep them ignored (see `.gitignore`).
- Prefer placing mountpoints/workdirs under `${TMPDIR:-/tmp}` using `mktemp -d` when writing new scripts.
- If a test must use repo-local paths, it MUST:
  - create paths only under `./mnt-*` (unique names, e.g. `mnt-test-$(date +%s)-$$`)
  - set `trap` cleanup to remove them on exit
  - avoid reusing a shared `mnt-*` directory name across tests

## Safe test & smoke rules

- Before running integration tests, run a cleanup dry-run and ensure the working tree is not polluted by artifacts:
  - `scripts/cleanup-mnt-artifacts.sh --dry-run`
- Minimal smoke (no mounts):
  - `./scripts/lint.sh`
- Integration suites (may create mounts/images):
  - run in a disposable environment, ensure cleanup via `scripts/cleanup-mnt-artifacts.sh`
  - prefer `./scripts/run-all-tests.sh` as the canonical aggregator
