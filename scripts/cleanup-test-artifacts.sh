#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

usage() {
  cat <<'USAGE'
Usage: scripts/cleanup-test-artifacts.sh [--dry-run] [--yes]

Cleans up common untracked test artifacts under the repository checkout.
Safety:
- Default is --dry-run (prints what would be deleted)
- Will NOT delete files/dirs that are tracked by git
- Use --yes to actually delete

Targets (if present and untracked):
- ./report/ ./logs/
- ./*.log ./*minisrv.log ./strace*.log
- ./_mnt_* ./_kafs*.log
- ./src/*.img ./src/mnt-* ./src/minisrv.log
- legacy root scripts ignored by .gitignore (e.g. test repro helpers)
- plus mount/image artifacts via scripts/cleanup-mnt-artifacts.sh
USAGE
}

dry_run=1

case "${1:-}" in
  --help|-h)
    usage
    exit 0
    ;;
  --yes)
    dry_run=0
    shift
    ;;
  --dry-run|"")
    dry_run=1
    [[ "${1:-}" == "--dry-run" ]] && shift || true
    ;;
  *)
    echo "Unknown option: $1" >&2
    usage >&2
    exit 2
    ;;
esac

if [[ $# -ne 0 ]]; then
  echo "Unexpected extra arguments: $*" >&2
  usage >&2
  exit 2
fi

is_tracked_file() {
  git ls-files --error-unmatch -- "$1" >/dev/null 2>&1
}

is_tracked_dir_tree() {
  local dir="$1"
  [[ -d "$dir" ]] || return 1
  git ls-files -- "$dir" | head -n 1 | grep -q .
}

collect_candidates() {
  # directories
  if [[ -d ./report ]]; then printf '%s\0' ./report; fi
  if [[ -d ./logs ]]; then printf '%s\0' ./logs; fi

  # root-level logs
  find . -maxdepth 1 -type f \( -name '*.log' -o -name 'minisrv.log' -o -name 'strace*.log' \) -print0 2>/dev/null || true

  # known prefixes
  find . -maxdepth 1 -mindepth 1 \( -name '_mnt_*' -o -name '_kafs*.log' \) -print0 2>/dev/null || true

  # src-local test artifacts (historically produced by C tests)
  find ./src -maxdepth 1 -type f \( -name '*.img' -o -name 'minisrv.log' \) -print0 2>/dev/null || true
  find ./src -maxdepth 1 -mindepth 1 -type d -name 'mnt-*' -print0 2>/dev/null || true

  # legacy root scripts (typically untracked and ignored)
  for f in \
    minimal-fsck-test.sh \
    reproduce_workload.sh \
    run-eio-test.sh \
    trigger-eio.sh \
    test-semantics-debug.sh \
    correct-strace.sh \
    detailed-strace.sh \
    final-eio-strace.sh
  do
    [[ -e "./$f" ]] && printf '%s\0' "./$f" || true
  done
}

mapfile -d '' raw < <(collect_candidates | sort -z)

# also include mnt/img cleanup targets (but only show here; deletion is delegated)

echo "[cleanup-test] repo: $ROOT_DIR"

echo "[cleanup-test] scanning candidates: ${#raw[@]}"

safe=()
skipped=()

for p in "${raw[@]}"; do
  if [[ -d "$p" ]]; then
    if is_tracked_dir_tree "$p"; then
      skipped+=("$p")
    else
      safe+=("$p")
    fi
  else
    if is_tracked_file "$p"; then
      skipped+=("$p")
    else
      safe+=("$p")
    fi
  fi
done

echo "[cleanup-test] delete targets (untracked): ${#safe[@]}"
for p in "${safe[@]}"; do
  echo "  $p"
done

echo "[cleanup-test] skipped (tracked by git): ${#skipped[@]}"
for p in "${skipped[@]}"; do
  echo "  $p"
done

echo "[cleanup-test] mount/image targets handled by scripts/cleanup-mnt-artifacts.sh"

if [[ $dry_run -eq 1 ]]; then
  echo "[cleanup-test] dry-run: nothing deleted (pass --yes to delete)"
  exit 0
fi

# Clean mount/image artifacts first
"$ROOT_DIR/scripts/cleanup-mnt-artifacts.sh" --yes >/dev/null

if [[ ${#safe[@]} -gt 0 ]]; then
  rm -rf -- "${safe[@]}"
fi

echo "[cleanup-test] deleted."
