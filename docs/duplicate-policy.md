# Duplicate Code Policy

- We run jscpd in two separate passes with formats `c,c-header`.
- Test utilities are extracted into `test_utils.c/h` to reduce duplication across tests.
- Generated artifacts stay out of scope, including `.deps/**` and generated `Makefile*` files.
- The strict gate scans `src/**/*.{c,h}` with `--min-lines=8` and `--threshold=1`.
- A separate informational report scans `tests/**/*.{c,h}` with the same minimum match size and a non-gating threshold.
- Because the scans are separated, duplication between `src/` and `tests/` is intentionally ignored.
- If the remaining clones only appear in generated or non-critical boilerplate, add targeted `--ignore` patterns with justification in this file.
