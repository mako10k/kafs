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

The `V6_OFFLINE_RETIREMENT` wave is complete under
[`plans/cli-v6-retirement.pert`](../plans/cli-v6-retirement.pert). Its ownership
record is
[`sd-card-wear-v6-retirement-inventory-20260722.md`](sd-card-wear-v6-retirement-inventory-20260722.md).
It removed the neutral descriptor implementation, v6 fixture creation and
descriptor tests, shared descriptor mappings and journal routing, and v6
offline fsck/dump behavior. `fsck.kafs` and `kafsdump` now reject a minimal v6
format marker with recreate-as-v7 guidance. The production v4/v5 path and the
v7-owned raw layout remain independent. The v5 metadata heatmap script remains;
only its v6 JSON ingestion mode was retired.

The closeout gate covered `autoreconf -fi`, configure/build, three focused
tests, all 46 current regression tests, formatting, lint, ownership, CLI,
clone, and static checks. The strict clone population remains 80 source files
after including the temporary placeholder and deleting the descriptor header;
findings fell from 45 to 37, duplicated lines from 466 to 382, and duplicated
tokens from 3393 to 2812. Complexity NLOC fell from 46011 to 42327 and warnings
from 116 to 104.

The refreshed `dag next` frontier is recorded in the companion selection record;
it does not itself authorize a successor wave.

`kafs-v6` now remains only as the intentionally ordered fail-closed placeholder.
Its binary, build/install rule, manual, and completion surface belong together
to `V6_FINAL_ENTRYPOINT_RETIREMENT`; explicit legacy-format rejection in active
v4/v5/v7 tools is a negative boundary, not a v6 compatibility implementation.

No repository evidence established an external recovery obligation beyond the
accepted recreate-as-v7 policy. External image holdings remain unverified; new
contrary evidence requires `REPLAN` before deleting a recovery artifact, but it
does not justify restoring the retired implementation.
