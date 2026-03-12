#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/watch-bg-dedup.sh <mountpoint> [interval_sec]

Description:
  Continuously prints a compact background-dedup observability dashboard from:
    kafsctl fsstat <mountpoint> --json

Examples:
  scripts/watch-bg-dedup.sh /home2
  scripts/watch-bg-dedup.sh /home2 0.5
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage >&2
  exit 2
fi

MNT="$1"
INTERVAL="${2:-1}"

if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq is required for scripts/watch-bg-dedup.sh" >&2
  echo "hint: sudo apt-get install -y jq" >&2
  exit 2
fi

if ! command -v kafsctl >/dev/null 2>&1; then
  if [[ -x "$(cd "$(dirname "$0")/.." && pwd)/src/kafsctl" ]]; then
    KAFSCTL="$(cd "$(dirname "$0")/.." && pwd)/src/kafsctl"
  else
    echo "error: kafsctl command not found" >&2
    exit 2
  fi
else
  KAFSCTL="kafsctl"
fi

last_steps=0
last_scanned=0
last_repl=0
last_hits=0
last_evicts=0
last_cooldowns=0
have_prev=0

while true; do
  json="$($KAFSCTL fsstat "$MNT" --json 2>/dev/null || true)"
  if [[ -z "$json" ]]; then
    echo "[$(date +%H:%M:%S)] fsstat failed for mountpoint: $MNT"
    sleep "$INTERVAL"
    continue
  fi

  # shellcheck disable=SC2016
  readarray -t vals < <(printf '%s\n' "$json" | jq -r '
    [
      (.version // 0),
      (.bg_dedup_steps // 0),
      (.bg_dedup_scanned_blocks // 0),
      (.bg_dedup_replacements // 0),
      (.bg_dedup_direct_candidates // 0),
      (.bg_dedup_direct_hits // 0),
      (.bg_dedup_index_evicts // 0),
      (.bg_dedup_cooldowns // 0),
      (.bg_dedup_mode // 0),
      (.bg_dedup_mode_str // "unknown"),
      (.bg_dedup_telemetry_valid // 0),
      (.bg_dedup_last_scanned_blocks // 0),
      (.bg_dedup_last_direct_candidates // 0),
      (.bg_dedup_last_replacements // 0),
      (.bg_dedup_idle_skip_streak // 0),
      (.bg_dedup_cold_start_due_ms // 0),
      (.bg_dedup_retry_rate // 0),
      (.hrl_entries_used // 0),
      (.hrl_entries_total // 0),
      (.hrl_put_fallback_legacy // 0),
      (.pending_queue_depth // 0),
      (.pending_queue_capacity // 0),
      (.pending_queue_head // 0),
      (.pending_queue_tail // 0),
      (.pending_worker_running // 0),
      (.pending_worker_stop_flag // 0),
      (.pending_worker_start_calls // 0),
      (.pending_worker_start_failures // 0),
      (.pending_worker_start_last_error // 0),
      (.pending_worker_lwp_tid // 0),
      (.pending_worker_main_entries // 0),
      (.pending_worker_main_exits // 0),
      (.pending_resolved // 0),
      (.pending_old_block_freed // 0)
    ] | .[]')

  ver="${vals[0]}"
  steps="${vals[1]}"
  scanned="${vals[2]}"
  repl="${vals[3]}"
  direct_cand="${vals[4]}"
  direct_hits="${vals[5]}"
  index_evicts="${vals[6]}"
  cooldowns="${vals[7]}"
  bg_mode="${vals[8]}"
  bg_mode_str="${vals[9]}"
  bg_telemetry_valid="${vals[10]}"
  bg_last_scanned="${vals[11]}"
  bg_last_candidates="${vals[12]}"
  bg_last_replacements="${vals[13]}"
  bg_idle_skip_streak="${vals[14]}"
  bg_cold_due_ms="${vals[15]}"
  retry_rate="${vals[16]}"
  hrl_used="${vals[17]}"
  hrl_total="${vals[18]}"
  fallback_legacy="${vals[19]}"
  pending_depth="${vals[20]}"
  pending_cap="${vals[21]}"
  pending_head="${vals[22]}"
  pending_tail="${vals[23]}"
  pending_running="${vals[24]}"
  pending_stop="${vals[25]}"
  pending_start_calls="${vals[26]}"
  pending_start_failures="${vals[27]}"
  pending_last_error="${vals[28]}"
  pending_lwp_tid="${vals[29]}"
  pending_main_entries="${vals[30]}"
  pending_main_exits="${vals[31]}"
  pending_resolved="${vals[32]}"
  pending_old_block_freed="${vals[33]}"

  d_steps=0
  d_scanned=0
  d_repl=0
  d_hits=0
  d_evicts=0
  d_cooldowns=0
  if [[ $have_prev -eq 1 ]]; then
    d_steps=$((steps - last_steps))
    d_scanned=$((scanned - last_scanned))
    d_repl=$((repl - last_repl))
    d_hits=$((direct_hits - last_hits))
    d_evicts=$((index_evicts - last_evicts))
    d_cooldowns=$((cooldowns - last_cooldowns))
  fi

  direct_hit_rate="0.000"
  if [[ "$direct_cand" -gt 0 ]]; then
    direct_hit_rate=$(awk -v a="$direct_hits" -v b="$direct_cand" 'BEGIN{printf "%.3f", a/b}')
  fi

  hrl_fill="0.00"
  if [[ "$hrl_total" -gt 0 ]]; then
    hrl_fill=$(awk -v a="$hrl_used" -v b="$hrl_total" 'BEGIN{printf "%.2f", (a*100.0)/b}')
  fi

  clear
  echo "KAFS BG DEDUP DASHBOARD  $(date '+%Y-%m-%d %H:%M:%S')"
  echo "mount=$MNT  fsstat_version=$ver"
  echo
  echo "[cumulative]"
  echo "  steps=$steps  scanned_blocks=$scanned  replacements=$repl"
  echo "  direct_candidates=$direct_cand  direct_hits=$direct_hits  direct_hit_rate=$direct_hit_rate"
  echo "  index_evicts=$index_evicts  cooldowns=$cooldowns  retry_rate=$retry_rate"
  echo "  scheduler: mode=$bg_mode ($bg_mode_str) telemetry_valid=$bg_telemetry_valid cold_due_ms=$bg_cold_due_ms"
  echo "             last_scan: scanned=$bg_last_scanned direct_candidates=$bg_last_candidates replacements=$bg_last_replacements idle_skip_streak=$bg_idle_skip_streak"
  echo "  hrl_used=$hrl_used/$hrl_total (${hrl_fill}%)  fallback_legacy=$fallback_legacy"
  echo "  pending_queue=$pending_depth/$pending_cap (head=$pending_head tail=$pending_tail)"
  echo "  pending_worker: running=$pending_running stop=$pending_stop start_calls=$pending_start_calls start_failures=$pending_start_failures last_error=$pending_last_error lwp_tid=$pending_lwp_tid"
  echo "  pending_worker_main: entries=$pending_main_entries exits=$pending_main_exits"
  echo "  pending_progress: resolved=$pending_resolved old_block_freed=$pending_old_block_freed"
  echo
  echo "[delta since last tick]"
  echo "  d_steps=$d_steps  d_scanned=$d_scanned  d_replacements=$d_repl"
  echo "  d_direct_hits=$d_hits  d_index_evicts=$d_evicts  d_cooldowns=$d_cooldowns"
  echo
  if [[ $have_prev -eq 0 ]]; then
    echo "warming up: waiting for next sample..."
  fi

  last_steps=$steps
  last_scanned=$scanned
  last_repl=$repl
  last_hits=$direct_hits
  last_evicts=$index_evicts
  last_cooldowns=$cooldowns
  have_prev=1

  sleep "$INTERVAL"
done
