# KAFS format v6 source retirement plan

Date: 2026-07-20
Status: accepted

## Decision

Format v6 source is being removed in stages. It is not a compatibility or
maintenance target. New behavior belongs to v7, and v6 code must not be
refactored merely to keep it aligned with v7.

The retirement order follows the shortest path to the v7 goal:

1. Replace independent, clone-heavy v6 runtime surfaces with a fail-closed
   `kafs-v6` placeholder. Keep the command name temporarily so existing callers
   receive an explicit retirement diagnostic instead of a missing command.
2. Move active offline diagnostics and neutral code away from v6-owned layout
   and policy types. Delete each v6 source after its active consumers have an
   owned v7 or genuinely neutral replacement.
3. Remove remaining v6 image creation, migration, offline diagnostics, fixtures,
   documentation, and tests once no active v7 or neutral code depends on them.
4. Remove the `kafs-v6` entrypoint, build/install rules, manual page, completion,
   and final retirement-only tests last.

## Start and exit gates

Before each retirement slice, re-read current source references and build/test
wiring. A handoff or this plan is only a starting hypothesis; it is not evidence
that a file is unused.

A slice may start only when:

- its active consumers and ownership boundary are enumerated from the current
  tree;
- the replacement or fail-closed behavior is explicit;
- deleting the selected sources does not route v7 through v6 compatibility;
- the slice reduces v6 surface or removes a prerequisite on the critical path.

A slice is complete only when the selected sources are no longer built or
referenced, user-visible guidance matches the new behavior, focused tests pass,
and repository static-analysis results do not regress under the same analysis
population. If ownership movement changes that population, record the old and
new populations separately; do not add unrelated refactors merely to recover a
threshold. Clone/static exclusions are temporary inventory controls, not
justification to retain excluded code.

## Current phase

Phase 1 retired v6 runtime mounting. `kafs-v6` remains only as a fail-closed
placeholder.

Phase 2 moved descriptor structures and validation into
`kafs_descriptor_layout.h`. Active common/v7 code no longer includes the v6
layout header; `kafs_v6_layout.h` is a temporary offline name adapter. The next
prerequisite is to stop v6 image creation and move the remaining offline v6
diagnostic callers to explicit neutral names so the adapter can be deleted.

The final entrypoint-removal phase must not be pulled forward merely to make the
tree smaller: until the other v6 surfaces are gone, the placeholder provides a
deterministic explanation to existing callers.
