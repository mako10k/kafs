#!/usr/bin/env bash
set -euo pipefail
# C/C++ formatting via clang-format; supports check mode and fix mode
MODE=${1:-check}
shopt -s globstar nullglob
FILES=(src/**/*.[ch])
if ! command -v clang-format >/dev/null 2>&1; then
  echo "clang-format not found. Install it to run formatting." >&2
  exit 0
fi
if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "No C files to format."; exit 0
fi
if [[ "$MODE" == "fix" ]]; then
  clang-format -i "${FILES[@]}"
  echo "Formatted ${#FILES[@]} files."
else
  # Check mode: show diff and fail if changes would occur
  FAIL=0
  for f in "${FILES[@]}"; do
    if ! diff -u "$f" <(clang-format "$f") >/dev/null; then
      echo "Would reformat: $f"
      FAIL=1
    fi
  done
  if [[ $FAIL -ne 0 ]]; then
    echo "Formatting issues found." >&2
    exit 1
  fi
  echo "Formatting OK."
fi
