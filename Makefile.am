SUBDIRS = src

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
