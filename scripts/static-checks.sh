#!/usr/bin/env bash
set -euo pipefail
# Aggregated static analysis runner
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"
./scripts/format.sh || true
./scripts/lint.sh || true
./scripts/clones.sh || true
./scripts/deadcode.sh || true
./scripts/complexity.sh || true
echo "Static checks completed. Reports under ./report"
