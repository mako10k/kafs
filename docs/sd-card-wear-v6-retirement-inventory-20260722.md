# KAFS format v6 residual inventory 2026-07-22

Status: completed `V6_RESIDUAL_CONTRACT` ownership record

Baseline: planning commit `0d4b5d80d7f50e330a3bc75d043a9c3479aea8db`;
dispositions were verified against the completed `V6_RESIDUAL_CONTRACT` wave.

This inventory owns every file returned before adding this inventory itself by:

```sh
rg -l -i '(^|[^a-z0-9])v6([^a-z0-9]|$)|KAFS_FORMAT_VERSION_V6|kafs-v6' \
  --glob '!autom4te.cache/**' --glob '!report/**' --glob '!.git/**' \
  --glob '!**/*.o' --glob '!**/*.a' --glob '!**/*.so' .
```

The query returned 80 files. A textual match is not automatically a defect:
negative admission rules, staged-removal evidence, and clearly labeled history
are distinct from supported behavior. Each path below has one primary owner;
mixed files are re-enumerated by their owning wave before modification.

## Current status

- `CONFIRMED`: `kafs-v6` rejects operational requests without opening an image.
- `CONFIRMED`: production `mkfs.kafs` and `kafsresize` reject format-v6 creation.
- `CONFIRMED`: the two repository v6 controlled-write operator scripts required
  a runtime entrypoint that now always rejects their operation.
- `CONFIRMED`: v6-specific offline dump/fsck and fixture-building code remains.
- `CONFIRMED`: active shared runtime/context source still contains v6-owned
  policy and state.
- `INFERRED`: v6 controlled-write code in shipped runtime binaries is
  unreachable because the enabling field has no assignment in tracked C source.
- `UNKNOWN`: whether an external experimental v6 image creates a recovery
  obligation beyond the accepted recreate-as-v7 policy. Such evidence blocks
  the later offline-removal wave and requires `REPLAN`.

## Removed by `V6_RESIDUAL_CONTRACT`

These repository-only operator surfaces were not installed by `Makefile.am`,
had no test consumer, and could not succeed through the current placeholder:

- `scripts/v6-controlled-write-smoke.sh`
- `scripts/v6-controlled-write-acceptance-gate.sh`

Historical documents may retain their names as past evidence, but every such
document is labeled non-operational and the current documentation index routes
operators to the retirement plan instead.

## Current governance and truthful guidance

Owner: `V6_RESIDUAL_CONTRACT`, then integrated qualification. Keep these files;
update their current-state wording as each wave closes.

- `AGENTS.md`
- `CHANGELOG.md`
- `docs/INDEX.md`
- `docs/kafs-cli-v6-retirement-selection-20260722.md`
- `docs/kafsresize-cutover-playbook.md`
- `docs/sd-card-wear-plan.md`
- `docs/sd-card-wear-tickets.md`
- `docs/sd-card-wear-v6-retirement-inventory-20260722.md`
- `docs/sd-card-wear-v6-retirement-plan.md`
- `man/kafsresize.8`
- `plans/cli-v6-retirement.pert`

Disposition: retain. Current guidance must not contain an executable v6
creation, mount, controlled-write, smoke, or cutover workflow. Historical
ticket text is allowed only behind the explicit retirement notice.

## Historical v6 records

Owner: repository history. These files now carry a visible historical or
superseded notice. They may retain exact past commands for archaeology, but are
not supported behavior and are no longer indexed as current design guidance.

- `docs/release-note-v6-explicit-write-opt-in-boundary.md`
- `docs/sd-card-wear-format-v6-descriptor.md`
- `docs/sd-card-wear-phase2-validation-20260617.md`
- `docs/sd-card-wear-phase5-validation-20260626.md`
- `docs/sd-card-wear-v6-cutover-preparation.md`
- `docs/sd-card-wear-v6-delayed-background-policy.md`
- `docs/sd-card-wear-v6-explicit-write-cutover-boundary.md`
- `docs/sd-card-wear-v6-fuse-write-surface-audit.md`
- `docs/sd-card-wear-v6-lock-stress-gate.md`
- `docs/sd-card-wear-v6-post-write-fsck-repair-policy.md`
- `docs/sd-card-wear-v6-production-diagnostic-scaffolding-inventory.md`
- `docs/sd-card-wear-v6-runtime-binary-split-decision.md`
- `docs/sd-card-wear-v6-runtime-entrypoint-plan.md`
- `docs/sd-card-wear-v6-runtime-handoff-20260626.md`
- `docs/sd-card-wear-v6-runtime-handoff-20260703.md`
- `docs/sd-card-wear-v6-runtime-mount-checkpoint.md`
- `docs/sd-card-wear-v6-shared-artifact-boundary-plan.md`
- `docs/sd-card-wear-v6-write-mount-dependency-audit.md`

Disposition: retain as history unless integrated qualification finds a document
that duplicates no unique decision or evidence. Never use history retention as
a reason to preserve build or runtime dependencies.

## Current v7 negative-boundary evidence

Owner: the v7 capability that names the boundary. These files mention v6 to
prevent compatibility leakage or to explain the pivot; they are not v6 support.

- `docs/incidents/2026-07-19-v7-recovery-investigation-plan.md`
- `docs/sd-card-wear-format-v7-inception-deck.md`
- `docs/sd-card-wear-format-v7-pivot.md`
- `docs/sd-card-wear-format-v7-raw-layout.md`
- `docs/sd-card-wear-v7-capability-rebaseline-20260721.md`
- `docs/sd-card-wear-v7-controlled-write-qualification.md`
- `docs/sd-card-wear-v7-indirect-pert-20260721.md`
- `docs/sd-card-wear-v7-migration-evidence.md`
- `docs/sd-card-wear-v7-runtime-handoff-20260716.md`
- `docs/sd-card-wear-v7-vhdx-handoff-20260721.md`
- `man/kafs-v7.8`

Disposition: retain accurate negative boundaries. Remove only stale references
to deleted symbols or workflows; do not weaken v7 rejection of legacy tokens.

## Active common and v7 source decoupling

Next owner: `V6_ACTIVE_COMMON_DECOUPLING`.

- `scripts/check-v7-runtime-policy-ownership.sh`
- `src/kafs.h`
- `src/kafs_block.h`
- `src/kafs_context.h`
- `src/kafs_hrl.c`
- `src/kafs_inode.h`
- `src/kafs_mount_options_common.c`
- `src/kafs_shared_fuse_runner.h`
- `src/kafs_shared_fuse_runtime.c`
- `src/kafs_v6_fuse_init_policy.h`
- `src/kafs_v6_fuse_policy.h`
- `src/kafs_v7.c`
- `src/kafs_v7_entrypoint_adapter.h`
- `src/kafs_v7_mount_options.c`
- `src/kafs_v7_runtime.c`
- `src/kafs_v7_runtime_view.c`
- `tests/tests_v7_entrypoint_smoketest.c`

Disposition: remove v6 runtime-policy behavior and v6-owned state from active
common/v7 code, or rename a proven format-neutral concept under neutral
ownership. Keep explicit v7 legacy-token rejection where it remains a useful
fail-closed input boundary. Extend the ownership check to include shared code.

## Offline diagnostics and deterministic fixtures

Next owner: `V6_OFFLINE_RETIREMENT`, after active-common decoupling and a fresh
consumer inventory.

- `docs/duplicate-policy.md`
- `docs/static-checks.md`
- `docs/tools-suite.md`
- `man/kafsdump.8`
- `scripts/check-v7-layout-ownership.sh`
- `scripts/clones.sh`
- `scripts/metadata-heatmap-report.sh`
- `src/fsck_kafs.c`
- `src/kafs_descriptor_layout.h`
- `src/kafsdump.c`
- `src/mkfs_kafs.c`
- `tests/test_utils.c`
- `tests/test_utils.h`
- `tests/tests_journal_boundary.c`
- `tests/tests_kafsresize.c`
- `tests/tests_v6_descriptor_smoketest.c`
- `tests/tests_v6_descriptor_validation.c`

Disposition: remove v6-specific behavior, fixtures, exclusions, and test cases
without deleting v4/v5/v7 behavior owned by the same file. Production
`mkfs.kafs` must continue to reject v6 throughout the transition. If external
recovery evidence requires bounded read-only inspection, record and replan that
obligation before removing the last diagnostic.

## Final placeholder and package surface

Next owner: `V6_FINAL_ENTRYPOINT_RETIREMENT`, only after the prior two source
waves close.

- `Makefile.am`
- `completions/kafs`
- `man/kafs-v6.8`
- `scripts/test-cli-surface.sh`
- `src/Makefile.am`
- `src/kafs_v6.c`
- `tests/Makefile.am`

Disposition: retain the fail-closed diagnostic now. In the final wave, remove
the binary, build/install rules, manual, completion, test environment wiring,
and final placeholder-only assertions together. Shared build files must retain
all unrelated targets.

## Wave-close checks

At every wave closeout:

1. rerun the inventory query and explain additions, removals, and ownership
   moves rather than comparing match count alone;
2. verify current operator guidance has no successful v6 workflow;
3. verify production creation and runtime admission remain fail closed until
   their surfaces are removed;
4. verify v7 has not acquired a v6 compatibility dependency; and
5. update `plans/cli-v6-retirement.pert` and rerun the standard three-command
   `perttool` gate.
