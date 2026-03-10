#!/usr/bin/env bash
set -euo pipefail
# Aggregated static analysis runner
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"
STEP_TIMEOUT_SECONDS=${STATIC_CHECK_STEP_TIMEOUT_SECONDS:-300}
FAILED_STEPS=0

run_step(){
	local name="$1"
	shift
	local rc=0
	echo "[static-checks] start: ${name}"
	if command -v timeout >/dev/null 2>&1; then
		if timeout --preserve-status "${STEP_TIMEOUT_SECONDS}s" "$@"; then
			echo "[static-checks] done: ${name}"
			return 0
		else
			rc=$?
		fi
	else
		if "$@"; then
			echo "[static-checks] done: ${name}"
			return 0
		else
			rc=$?
		fi
	fi

	if [[ $rc -eq 124 || $rc -eq 137 ]]; then
		echo "[static-checks] timeout: ${name} (${STEP_TIMEOUT_SECONDS}s)" >&2
	else
		echo "[static-checks] fail: ${name} (rc=${rc})" >&2
	fi
	FAILED_STEPS=$((FAILED_STEPS + 1))
	return 0
}

run_step format ./scripts/format.sh
run_step lint ./scripts/lint.sh
run_step clones ./scripts/clones.sh
run_step deadcode ./scripts/deadcode.sh
run_step complexity ./scripts/complexity.sh

if [[ $FAILED_STEPS -gt 0 ]]; then
	echo "[static-checks] completed with ${FAILED_STEPS} non-passing step(s)." >&2
fi
echo "Static checks completed. Reports under ./report"
