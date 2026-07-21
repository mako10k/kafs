# Copilot Supplemental Guidelines

This document holds supplemental guidance that is not mandatory policy.

## Workflow
- Treat handoff and backlog next steps as candidates. Revalidate current
  evidence and re-derive exit criteria through the `AGENTS.md` Task Start Gate
  before implementation.
- Refresh the `.pert` capability dependency plan and run
  `./scripts/pert-next-task.sh` after each wave. State what each
  `RUNNABLE NOW` wave unlocks before continuing.
- Batch related shell commands and checkpoint every 3-5 calls.
- Add a one-line preamble before each tool batch: why / what / expected outcome.
- Summarize key findings and next steps after command batches.
- Avoid dumping raw command lists unless the user explicitly asks.
- Prefer concise, skimmable progress updates.

## Quality Gates
- Treat these as completion and escape-detection controls, not substitutes for
  the Task Start Gate.
- Avoid introducing code clones; extract helpers when logic repeats.
- Build and run tests after edits.
- Report PASS/FAIL for build/lint/tests with delta-focused notes.
- If failures occur, iterate targeted fixes up to three times, then summarize blockers.
- For PR/update gates, run clone/static checks:
  - `./scripts/clones.sh`
  - `./scripts/static-checks.sh` (or equivalent component checks)

## Repo Conventions
- Respect repository scripts and Makefile targets.
- Follow GitHub development rules in `.github/github-dev-rules.md`.
