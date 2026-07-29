# KAFS Documentation Index

This index is the recommended starting point for KAFS operators and developers.
It prioritizes product usage, operational workflows, and tool references before
historical investigations and deep design notes.

## Start Here

- [../README.md](../README.md): top-level project overview, quick start, and tool summary
- [tools-suite.md](tools-suite.md): current tool surface, gaps, and long-range direction
- [static-checks.md](static-checks.md): repo validation and CI/static-check expectations

## Operator Workflows

- [crash-diagnostics.md](crash-diagnostics.md): crash logs, core dumps, and failure capture
- [kafsctl-path-ops-requirements.md](kafsctl-path-ops-requirements.md): mounted-tree control and path-based operation expectations
- [kafsimage-format.md](kafsimage-format.md): export modes and output semantics for kafsimage
- [kafsresize-cutover-playbook.md](kafsresize-cutover-playbook.md): staged destination-image cutover workflow for `kafsresize --migrate-create`
- [operator-diagnostics-plan.md](operator-diagnostics-plan.md): staged design for the next operator-facing diagnostics slice

## Design And Planning

- [codex-review-orchestration.md](codex-review-orchestration.md)
- [pert-task-selection.md](pert-task-selection.md): mandatory `perttool` gate for capability-level task ordering
- [../plans/current.pert](../plans/current.pert): canonical residual network for current next-task selection
- [hotplug-plan.md](hotplug-plan.md)
- [hotplug-requirements.md](hotplug-requirements.md)
- [hotplug-design.md](hotplug-design.md)
- [hotplug-milestones.md](hotplug-milestones.md)
- [hotplug-tickets.md](hotplug-tickets.md)
- [hotplug-pipe-plan.md](hotplug-pipe-plan.md)
- [hotplug-pipe-requirements.md](hotplug-pipe-requirements.md)
- [hotplug-pipe-design.md](hotplug-pipe-design.md)
- [hotplug-pipe-milestones.md](hotplug-pipe-milestones.md)
- [hotplug-pipe-tickets.md](hotplug-pipe-tickets.md)
- [dedup-design.md](dedup-design.md)
- [duplicate-policy.md](duplicate-policy.md)
- [concurrency-plan.md](concurrency-plan.md)
- [journal-plan.md](journal-plan.md)
- [sd-card-wear-plan.md](sd-card-wear-plan.md)
- [sd-card-wear-tickets.md](sd-card-wear-tickets.md)
- [sd-card-wear-phase2-validation-20260617.md](sd-card-wear-phase2-validation-20260617.md)
- [sd-card-wear-phase5-validation-20260626.md](sd-card-wear-phase5-validation-20260626.md)
- [sd-card-wear-format-v6-descriptor.md](sd-card-wear-format-v6-descriptor.md)
- [sd-card-wear-v6-runtime-mount-checkpoint.md](sd-card-wear-v6-runtime-mount-checkpoint.md)
- [sd-card-wear-v6-write-mount-dependency-audit.md](sd-card-wear-v6-write-mount-dependency-audit.md)
- [sd-card-wear-v6-delayed-background-policy.md](sd-card-wear-v6-delayed-background-policy.md)
- [sd-card-wear-v6-post-write-fsck-repair-policy.md](sd-card-wear-v6-post-write-fsck-repair-policy.md)
- [sd-card-wear-v6-lock-stress-gate.md](sd-card-wear-v6-lock-stress-gate.md)
- [sd-card-wear-v6-explicit-write-cutover-boundary.md](sd-card-wear-v6-explicit-write-cutover-boundary.md)
- [sd-card-wear-v6-runtime-binary-split-decision.md](sd-card-wear-v6-runtime-binary-split-decision.md)
- [sd-card-wear-v6-cutover-preparation.md](sd-card-wear-v6-cutover-preparation.md)
- [sd-card-wear-v6-shared-artifact-boundary-plan.md](sd-card-wear-v6-shared-artifact-boundary-plan.md)
- [sd-card-wear-v6-runtime-entrypoint-plan.md](sd-card-wear-v6-runtime-entrypoint-plan.md)
- [sd-card-wear-v6-runtime-handoff-20260626.md](sd-card-wear-v6-runtime-handoff-20260626.md)
- [sd-card-wear-v7-capability-rebaseline-20260721.md](sd-card-wear-v7-capability-rebaseline-20260721.md): current v7 capability, R2 closeout, and selected M7 qualification task
- [sd-card-wear-v7-controlled-write-qualification.md](sd-card-wear-v7-controlled-write-qualification.md): T48 qualification path, non-destructive evidence contract, and real-media boundary
- [sd-card-wear-v7-real-media-qualification-approval.md](sd-card-wear-v7-real-media-qualification-approval.md): T48-B1 real-media matrix, destructive-impact, digest-bound approval, and independent-review contract
- [sd-card-wear-v7-vhdx-host-recovery.md](sd-card-wear-v7-vhdx-host-recovery.md): safe Windows-host VHDX-backed terminate/restart prequalification and evidence boundary
- [sd-card-wear-v7-vhdx-handoff-20260721.md](sd-card-wear-v7-vhdx-handoff-20260721.md): WIP closeout, tomorrow's Task Start Gate, and native Windows restart point
- [sd-card-wear-v7-runtime-handoff-20260716.md](sd-card-wear-v7-runtime-handoff-20260716.md): detailed v7 runtime implementation handoff
- [write-performance-ideas.md](write-performance-ideas.md)
- [bottleneck-measurement-plan.md](bottleneck-measurement-plan.md)
- [perf-evaluation-20260228.md](perf-evaluation-20260228.md)
- [perf-discrepancy-analysis-20260228.md](perf-discrepancy-analysis-20260228.md)
- [format-v3-123-spec.md](format-v3-123-spec.md)
- [format-v3-123-tickets.md](format-v3-123-tickets.md)
- [migration-v2-to-v3.md](migration-v2-to-v3.md)
- [phase1-validation-20260228.md](phase1-validation-20260228.md)
- [phase2-validation-20260228.md](phase2-validation-20260228.md)
- [phase3-validation-20260228.md](phase3-validation-20260228.md)

## Release And Project History

- [release-announcement-v0.2.1.md](release-announcement-v0.2.1.md)
- [release-announcement-v0.3.0.md](release-announcement-v0.3.0.md)
- [release-announcement-v0.3.1.md](release-announcement-v0.3.1.md)
- [release-note-v6-explicit-write-opt-in-boundary.md](release-note-v6-explicit-write-opt-in-boundary.md)
- [discussion-post-v0.2.1-final.md](discussion-post-v0.2.1-final.md)

## Historical Investigations And Reproductions

These documents remain useful for archaeology and incident follow-up, but they
are no longer the recommended first-read path for normal product usage.

- [README_STRACE_ANALYSIS.md](README_STRACE_ANALYSIS.md)
- [STRACE_MINIMAL_REPRODUCTION_FINAL.md](STRACE_MINIMAL_REPRODUCTION_FINAL.md)
- [STRACE_EIO_ANALYSIS.md](STRACE_EIO_ANALYSIS.md)
- [STRACE_ANALYSIS_INDEX.md](STRACE_ANALYSIS_INDEX.md)

## Miscellaneous References

- [tail-packing-format-sketch.md](tail-packing-format-sketch.md)
