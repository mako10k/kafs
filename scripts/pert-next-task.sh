#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PLAN="${1:-$REPO_ROOT/plans/current.pert}"

if [[ $# -gt 1 ]]; then
  echo "usage: $0 [plan.pert]" >&2
  exit 2
fi

if ! command -v perttool >/dev/null 2>&1; then
  echo "error: perttool is required for KAFS next-task selection" >&2
  exit 127
fi

if [[ ! -r "$PLAN" ]]; then
  echo "error: PERT plan is not readable: $PLAN" >&2
  exit 2
fi

echo "PLAN $PLAN"
perttool --version
perttool document check "$PLAN"
perttool dag analyze "$PLAN" --schedule both --format text
perttool dag next "$PLAN" --format text
