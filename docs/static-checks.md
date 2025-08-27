# Static Checks

- Formatting: clang-format via scripts/format.sh
- Lint: compiler warnings-as-errors via scripts/lint.sh
- Clones: jscpd via scripts/clones.sh (formats: c,c-header)
- Dead code: cppcheck via scripts/deadcode.sh
- Complexity: lizard via scripts/complexity.sh

Run all: scripts/static-checks.sh
