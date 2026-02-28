# Phase3 検証結果（2026-02-28）

対象: P3-T6（build/test PASS + Phase3 性能ゲート判定）

## 実行条件

- workload: `scripts/workload-npm-offline-local.sh`
- 実行回数: 5 run
- 計測出力（percentile対応後）: `report/perf/p3t6-validation-pct-20260228-112537`
- 比較基準（v2基準として運用中）: `report/perf/quick-series-fixed-20260228-021528`

## build/test

- `make -j"$(nproc)"`: PASS
- `scripts/run-all-tests.sh`: PASS

## 5-run 中央値

- elapsed: 24.820 s
- user: 8.62 s
- sys: 4.91 s

## 基準比較（w3 elapsed）

- baseline elapsed: 22.260 s
- 現在 elapsed: 24.820 s
- 差分: +2.560 s
- 改善率: -11.50%（悪化）

## write 系補助指標（FSSTAT 5-run 中央値）

- `pwrite_iblk_write_ms`: 57.703 ms
- `iblk_write_hrl_put_ms`: 22.877 ms
- `iblk_write_dec_ref_ms`: 25.265 ms
- `blk_alloc_set_usage_ms`: 0.909 ms

## write latency percentile（新規）

- `pwrite_iblk_write_p50_ms`: 0.069 ms
- `pwrite_iblk_write_p95_ms`: 0.100 ms
- `pwrite_iblk_write_p99_ms`: 0.136 ms

## P3-T6 判定

- build/test: PASS
- `w3` elapsed 15%以上短縮（v2基準）: FAIL
- P99 write latency 25%以上短縮: 判定保留（v2側の p99 baseline 未採取）

総合: **P3-T6 は未達**（elapsed ゲート未達）

## 補足

- 今回の更新で `kafsctl fsstat --json` から `pwrite_iblk_write_p50/p95/p99_{ns,ms}` が取得可能になった。
- 次アクションは、同計測系で v2 baseline の p99 を採取し、P99 ゲートを再判定する。
