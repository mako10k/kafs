#!/usr/bin/env bash
set -euo pipefail

if ! command -v reuse >/dev/null 2>&1; then
	echo "license-check: reuse is required (expected version 6.2.0)" >&2
	echo "install with: python3 -m pip install reuse==6.2.0" >&2
	exit 127
fi

reuse lint
