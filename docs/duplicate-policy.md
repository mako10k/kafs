# Duplicate Code Policy

- We run jscpd with formats `c,c-header` scanning `src/**/*.{c,h}`.
- Test utilities are extracted into `test_utils.c/h` to reduce duplication across tests.
- Acceptable duplication threshold is `--min-lines=20` and `--threshold=2` in scripts/clones.sh.
- If the remaining clones only appear in generated or non-critical boilerplate, consider adding targeted `--ignore` patterns with justification in this file.
