SUBDIRS = src tests

man_MANS = \
	man/kafs.1 \
	man/kafs-v6.8 \
	man/kafs-v7.8 \
	man/mount.kafs.8 \
	man/kafsctl.1 \
	man/mkfs.kafs.8 \
	man/fsck.kafs.8 \
	man/kafsdump.8 \
	man/kafsimage.8 \
	man/kafsresize.8 \
	man/kafs-info.8 \
	man/kafs-front.8 \
	man/kafs-back.8

bashcompletiondir = $(datadir)/bash-completion/completions
bashcompletion_DATA = completions/kafs completions/mount.kafs

EXTRA_DIST = $(man_MANS) $(bashcompletion_DATA) \
	docs/sd-card-wear-v7-real-media-qualification-approval.md \
	docs/sd-card-wear-v7-real-media-qualification-matrix.json \
	docs/sd-card-wear-v7-vhdx-host-recovery.md \
	scripts/v7-controlled-write-qualification-dry-run.sh \
	scripts/v7-controlled-write-qualification-gate.sh \
	scripts/v7-real-media-qualification-approval-gate.sh \
	scripts/v7-vhdx-host-recovery.ps1 \
	scripts/v7-vhdx-host-recovery.sh

# Convenience targets for static analysis (call into scripts/)
.PHONY: format lint clones deadcode complexity static-checks

format:
	@./scripts/format.sh fix

lint:
	@./scripts/lint.sh

clones:
	@./scripts/clones.sh

deadcode:
	@./scripts/deadcode.sh

complexity:
	@./scripts/complexity.sh

static-checks: format lint clones deadcode complexity
