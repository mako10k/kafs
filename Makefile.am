SUBDIRS = src tests

man_MANS = \
	man/kafs.1 \
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
bashcompletion_DATA = completions/kafs

EXTRA_DIST = $(man_MANS) $(bashcompletion_DATA)

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
