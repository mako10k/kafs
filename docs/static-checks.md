# Static Checks

- Formatting: clang-format via scripts/format.sh
- Lint: compiler warnings-as-errors via scripts/lint.sh
- Clones: jscpd via scripts/clones.sh with a strict `src/**/*.{c,h}` gate,
  including the temporary fail-closed `kafs-v6` placeholder, plus a separate
  non-gating `tests/**/*.{c,h}` report (formats: c,c-header)
- Dead code and semantic diagnostics: cppcheck via scripts/deadcode.sh
- Complexity: lizard via scripts/complexity.sh

Run the normal aggregate gate (format, lint, clones, and complexity):
`scripts/static-checks.sh`. Run cppcheck separately with
`scripts/deadcode.sh`; it is not part of the aggregate script.
