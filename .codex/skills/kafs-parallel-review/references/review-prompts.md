# KAFS Parallel Review Prompts

Use these prompts when coordinating the `kafs-parallel-review` skill. Replace `TARGET` with the working tree, commit, branch base, PR, or document under review.

## Shared Instructions

- Review TARGET in `/home/katsumata-m/kafs`.
- Treat `AGENTS.md`, `.github/github-dev-rules.md`, and `.github/lock-policy.md` as binding when relevant.
- Stay read-only.
- Lead with findings ordered by severity.
- Include file/line references when available.
- Separate directly observed facts from inferred risks.
- If no issues are found, say that explicitly and list any residual test or evidence gaps.

## Consistency Reviewer

Role file: `.codex/agents/consistency-reviewer.toml`

Focus on consistency, symmetry, and completeness across repository surfaces. Check paired operations, option names, status labels, enum/table coverage, docs/tests parity, positive and negative cases, and stale references.

## Plain Reviewer

Role file: `.codex/agents/plain-reviewer.toml`

Review as a non-expert user, operator, or new contributor. Flag unclear terminology, missing next steps, surprising CLI behavior, vague risk communication, and places where the change requires hidden filesystem knowledge.

## Domain Expert Reviewer

Role file: `.codex/agents/domain-expert-reviewer.toml`

Review as a filesystem/storage expert. Prioritize data-loss risk, on-disk compatibility, crash consistency, fsck/migration/resize behavior, FUSE regressions, C correctness, Autotools/test integration, locking policy, and SD-card wear design claims.

## Synthesis Checklist

- Deduplicate overlapping findings but preserve perspective tags.
- Do not convert plain-reader impressions into technical defects unless another agent provided evidence.
- Do not hide disagreement; summarize it as an open question.
- Keep summaries short and put actionable findings first.
