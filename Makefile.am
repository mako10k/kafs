SUBDIRS = src tests

man_MANS = man/kafs.1 man/mkfs.kafs.8 man/fsck.kafs.8

EXTRA_DIST = $(man_MANS)

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
