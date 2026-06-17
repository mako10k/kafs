# Codex Review Orchestration

This repository supports a three-perspective review flow for changes that need more than a single correctness pass:

- `consistency-reviewer`: consistency, symmetry, completeness, and missing counterpart cases.
- `plain-reviewer`: frank non-expert feedback on clarity, operational usability, and reader expectations.
- `domain-expert-reviewer`: filesystem, FUSE, C, Autotools, durability, migration, locking, and SD-card wear review.

## Trigger Conditions

Run the parallel review flow when a user explicitly asks for multi-agent or parallel review, or explicitly approves it for a broad change that affects multiple surfaces such as `src/`, `tests/`, `docs/`, `man/`, and `completions/`.

It is also appropriate, with explicit user approval, before commit or PR updates for changes touching on-disk format, migration, fsck, resize, locking, control paths, FUSE-visible behavior, or SD-card wear policy.

Do not require it for tiny typo-only edits unless requested.

## Interactive Codex Flow

When the multi-agent tool is available, the main agent should:

1. Load the `kafs-parallel-review` skill.
2. Read `.codex/skills/kafs-parallel-review/references/review-prompts.md`.
3. Spawn `consistency-reviewer`, `plain-reviewer`, and `domain-expert-reviewer` in parallel against the same target.
4. Wait for all results and synthesize findings by severity.

The final synthesis should keep non-expert impressions separate from confirmed technical defects and should name any perspective that found no issues.

## Manual Or Hook Entrypoint

Use the script below when a shell-based hook or manual review run is preferred:

```sh
./scripts/codex-parallel-review.sh --target working-tree --dry-run
./scripts/codex-parallel-review.sh --target working-tree
./scripts/codex-parallel-review.sh --target HEAD
./scripts/codex-parallel-review.sh --base main
```

The script runs three `codex exec review` processes in parallel with read-only configuration overrides and stores outputs under `report/codex-review/`, which is ignored by Git.

This repository does not commit an automatic Codex hook schema here because hook configuration is Codex-version dependent. Instead, `AGENTS.md` defines the trigger conditions and `scripts/codex-parallel-review.sh` provides the stable hook target.
