#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

usage() {
  cat <<'USAGE'
Usage: scripts/cleanup-mnt-artifacts.sh [--dry-run] [--yes]

Deletes local test artifacts created under the repository root:
- ./mnt-*/ directories
- ./mnt_setup_temp/ directory
- ./*.img files

Safety:
- Default is --dry-run (prints what would be deleted)
- Use --yes to actually delete
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

mapfile -d '' mnt_dirs < <(
  find . -maxdepth 1 -mindepth 1 -type d \( -name 'mnt-*' -o -name 'mnt_setup_temp' \) -print0 | sort -z
)

mapfile -d '' img_files < <(
  find . -maxdepth 1 -type f -name '*.img' -print0 | sort -z
)

echo "[cleanup-mnt] repo: $ROOT_DIR"

echo "[cleanup-mnt] targets (directories): ${#mnt_dirs[@]}"
for p in "${mnt_dirs[@]}"; do
  echo "  $p"
done

echo "[cleanup-mnt] targets (image files): ${#img_files[@]}"
for p in "${img_files[@]}"; do
  echo "  $p"
done

if [[ $dry_run -eq 1 ]]; then
  echo "[cleanup-mnt] dry-run: nothing deleted (pass --yes to delete)"
  exit 0
fi

if [[ ${#mnt_dirs[@]} -gt 0 ]]; then
  rm -rf -- "${mnt_dirs[@]}"
fi
if [[ ${#img_files[@]} -gt 0 ]]; then
  rm -f -- "${img_files[@]}"
fi

echo "[cleanup-mnt] deleted."
