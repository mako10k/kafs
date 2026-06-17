#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage: scripts/codex-parallel-review.sh [OPTIONS]

Run three complementary Codex reviews in parallel:
  - consistency-reviewer
  - plain-reviewer
  - domain-expert-reviewer

Options:
  --target TARGET     working-tree, uncommitted, or a commit-ish value (default: working-tree)
  --base BRANCH       review changes against BRANCH instead of TARGET
  --out-dir DIR       output directory; relative paths are repo-root relative
                      (default: report/codex-review/<utc timestamp>-<pid>)
  --dry-run           print planned prompts without invoking Codex
  -h, --help          show this help
USAGE
}

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target="working-tree"
base=""
out_dir=""
dry_run=0

while (($#)); do
    case "$1" in
        --target)
            if (($# < 2)); then
                echo "missing value for --target" >&2
                exit 2
            fi
            target="$2"
            shift 2
            ;;
        --base)
            if (($# < 2)); then
                echo "missing value for --base" >&2
                exit 2
            fi
            base="$2"
            shift 2
            ;;
        --out-dir)
            if (($# < 2)); then
                echo "missing value for --out-dir" >&2
                exit 2
            fi
            out_dir="$2"
            shift 2
            ;;
        --dry-run)
            dry_run=1
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z "$out_dir" ]]; then
    out_dir="$root_dir/report/codex-review/$(date -u +%Y%m%dT%H%M%SZ)-$$"
elif [[ "$out_dir" != /* ]]; then
    out_dir="$root_dir/$out_dir"
fi

review_scope_args=()
review_scope_label=""
if [[ -n "$base" ]]; then
    review_scope_args+=(--base "$base")
    review_scope_label="changes against base: $base"
elif [[ "$target" == "working-tree" || "$target" == "uncommitted" ]]; then
    review_scope_args+=(--uncommitted)
    review_scope_label="uncommitted working tree"
else
    review_scope_args+=(--commit "$target")
    review_scope_label="commit: $target"
fi

make_prompt() {
    local role="$1"
    cat <<PROMPT
Run the KAFS ${role} review for scope: ${review_scope_label}

Use AGENTS.md and .codex/agents/${role}.toml as binding role context.
Also read .codex/skills/kafs-parallel-review/references/review-prompts.md and apply the ${role} instructions.

Stay read-only. Lead with findings ordered by severity. Include file/line references when available.
Separate directly observed facts from inferred risks. If no issues are found, say so explicitly and list residual evidence gaps.
PROMPT
}

roles=(
    consistency-reviewer
    plain-reviewer
    domain-expert-reviewer
)

if ((dry_run)); then
    echo "Would write review output to: $out_dir"
    echo "Review scope: $review_scope_label"
    echo "Review scope args: ${review_scope_args[*]}"
    for role in "${roles[@]}"; do
        echo
        echo "### $role"
        make_prompt "$role"
    done
    exit 0
fi

if ! command -v codex >/dev/null 2>&1; then
    echo "codex command not found" >&2
    exit 127
fi

mkdir -p "$out_dir"

pids=()
for role in "${roles[@]}"; do
    (
        cd "$root_dir"
        codex exec review \
            -c 'sandbox_mode="read-only"' \
            -c 'approval_policy="never"' \
            --ephemeral \
            "${review_scope_args[@]}" \
            -o "$out_dir/${role}.md" \
            "$(make_prompt "$role")" \
            >"$out_dir/${role}.stdout" \
            2>"$out_dir/${role}.stderr"
    ) &
    pids+=("$!")
done

status=0
for i in "${!roles[@]}"; do
    if wait "${pids[$i]}"; then
        echo "PASS ${roles[$i]} -> $out_dir/${roles[$i]}.md"
    else
        rc=$?
        echo "FAIL ${roles[$i]} rc=$rc -> $out_dir/${roles[$i]}.stderr" >&2
        status=1
    fi
done

if ((status == 0)); then
    echo "Review outputs written under: $out_dir"
fi

exit "$status"
