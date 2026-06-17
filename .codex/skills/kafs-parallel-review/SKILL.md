---
name: kafs-parallel-review
description: Run complementary KAFS reviews in parallel using the consistency-reviewer, plain-reviewer, and domain-expert-reviewer agents. Use this when the user asks for multi-perspective or parallel review, or explicitly approves it for risky filesystem, migration, locking, or SD-card wear work.
---

# KAFS Parallel Review

## Overview

This skill coordinates three read-only review perspectives for KAFS and merges their outputs into one evidence-based response. It is intended for review orchestration, not implementation.

## Workflow

1. Identify the review target: working tree, staged diff, commit, branch base, PR, or a specific design document.
2. Read `references/review-prompts.md` before launching agents.
3. Spawn these agents in parallel with the same target and repository context:
   - `consistency-reviewer`
   - `plain-reviewer`
   - `domain-expert-reviewer`
4. Ask each agent to stay read-only, lead with findings, cite file/line references where available, and separate direct evidence from inference.
5. Wait for all three results, then synthesize:
   - Deduplicate overlapping findings.
   - Preserve severity and perspective when they differ.
   - Keep non-expert impressions separate from confirmed defects.
   - Report "no findings" only for a perspective that explicitly found none.

## Trigger Conditions

Use this skill when:

- The user asks for "parallel review", "multiple reviewers", "non-expert review", "expert review", or consistency/symmetry/completeness review.
- The user explicitly approves the review for a change that touches multiple repository surfaces, such as `src/`, `tests/`, `docs/`, `man/`, and `completions/`.
- The user explicitly approves the review for on-disk format, migration, fsck, resize, locking, control paths, or SD-card wear behavior.
- The user explicitly approves a structured review before committing or before a PR/update gate.

Do not use this skill for tiny typo-only changes unless the user explicitly asks.

## Output Shape

Return a concise synthesis in this order:

1. Findings ordered by severity, each tagged with the originating perspective.
2. Open questions or assumptions.
3. Test or evidence gaps.
4. Brief note naming which agents ran and whether any agent found no issues.

For manual or hook-style execution outside the interactive multi-agent tool, use `scripts/codex-parallel-review.sh`.

## Resources

- `references/review-prompts.md`: exact per-agent prompt templates and synthesis checklist.
