# HANDOFF

Date: 2026-02-09
Branch: feat/complete-kafs-back-delegation

## Summary
- Migrated hotplug IPC toward socketpair-only and removed UDS-based test coverage.
- Renamed hotplug-pipe docs to hotplug-ipc and updated references.

## Changes
- Removed UDS-based e2e hotplug test and its build wiring.
  - tests/Makefile.am
  - tests/tests_e2e_hotplug.c (deleted)
- Socketpair-only hotplug changes (partial):
  - src/kafs.c (spawn/handshake/restart via socketpair, UDS path removal)
  - src/kafs_back.c (requires KAFS_HOTPLUG_BACK_FD/--fd, no UDS)
  - src/kafs_context.h (removed UDS path/connect fields)
- Documentation rename:
  - docs/hotplug-ipc-*.md added
  - docs/hotplug-pipe-*.md removed
  - references updated in docs/hotplug-*.md

## Tests
- (fill after run) make -j
- (fill after run) ./scripts/lint.sh
- (fill after run) make check

## Notes / Next Steps
- kafs_front.c still references KAFS_HOTPLUG_UDS; decide whether to delete kafs_front or align it with socketpair-only.
- Regenerate autotools outputs if needed (tests/Makefile.in) after Makefile.am changes.
- Ensure hotplug restart-back flow works with socketpair path end-to-end.
