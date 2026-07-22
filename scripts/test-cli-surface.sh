#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

declare -a help_bins=(
  "./src/kafs"
  "./src/kafs-v6"
  "./src/mkfs.kafs"
  "./src/fsck.kafs"
  "./src/kafsctl"
  "./src/kafsdump"
  "./src/kafsimage"
  "./src/kafsresize"
  "./src/kafs-info"
)

for bin in "${help_bins[@]}"; do
  if ! "$bin" --help >/dev/null 2>&1; then
    echo "FAIL: $bin --help returned non-zero" >&2
    exit 1
  fi
done

kafsresize_help=$(./src/kafsresize --help 2>&1)
if grep -Fq -- "v6 migration" <<<"$kafsresize_help"; then
  echo "FAIL: kafsresize help advertises retired v6 migration" >&2
  exit 1
fi

declare -a retired_v6_operator_scripts=(
  "scripts/v6-controlled-write-smoke.sh"
  "scripts/v6-controlled-write-acceptance-gate.sh"
)

for path in "${retired_v6_operator_scripts[@]}"; do
  if [[ -e "$path" ]]; then
    echo "FAIL: retired v6 operator script is present: $path" >&2
    exit 1
  fi
done

declare -a current_migration_guidance=(
  "README.md"
  "CHANGELOG.md"
  "docs/INDEX.md"
  "docs/kafsresize-cutover-playbook.md"
  "man/kafsresize.8"
)

retired_v6_pattern='--format-version 6|v6-controlled-write-(smoke|acceptance)|kafs-v6.*(--inspection-mount|--controlled-write-mount)'
if grep -En -- "$retired_v6_pattern" "${current_migration_guidance[@]}"; then
  echo "FAIL: current migration guidance contains a retired v6 workflow" >&2
  exit 1
fi

declare -a subcommand_help_cmds=(
  "./src/kafsctl help fsstat"
  "./src/kafsctl fsstat --help"
  "./src/kafsctl hotplug --help"
  "./src/kafsctl hotplug status --help"
  "./src/kafsctl hotplug env set --help"
  "./src/kafsctl help stat"
  "./src/kafsctl stat --help"
  "./src/kafsctl help chmod"
  "./src/kafsctl chmod --help"
  "./src/kafsctl cp --help"
  "./src/kafsctl mv --help"
  "./src/kafsctl ln --help"
  "./src/kafsctl symlink --help"
  "./src/kafsctl rsync --help"
)

for cmd in "${subcommand_help_cmds[@]}"; do
  if ! eval "$cmd" >/dev/null 2>&1; then
    echo "FAIL: subcommand help returned non-zero: $cmd" >&2
    exit 1
  fi
done

completion_file="completions/kafs"

declare -a required_completion_tokens=(
  "--hrl-entry-ratio"
  "--check-inode-block-counts"
  "--repair-inode-block-counts"
  "--migrate-create"
  "--dst-image"
  "--src-mount"
  "--dst-mount"
  "--verbose"
)

for token in "${required_completion_tokens[@]}"; do
  if ! grep -Fq -- "$token" "$completion_file"; then
    echo "FAIL: completion missing token $token" >&2
    exit 1
  fi
done

echo "PASS: CLI help and completion surface checks"
