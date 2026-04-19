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