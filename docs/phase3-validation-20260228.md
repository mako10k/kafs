# Phase3 検証結果（2026-02-28）

対象: P3-T6（build/test PASS + Phase3 性能ゲート判定）

## 実行条件

- workload: `scripts/workload-npm-offline-local.sh`
- 実行回数: 5 run
- 計測出力: `report/perf/p3t6-validation-20260228-111601`
- 比較基準（v2基準として運用中）: `report/perf/quick-series-fixed-20260228-021528`

## build/test

- `make -j"$(nproc)"`: PASS
- `scripts/run-all-tests.sh`: PASS

## 5-run 中央値

- elapsed: 26.370 s
- user: 8.19 s
- sys: 4.74 s

## 基準比較（w3 elapsed）

- baseline elapsed: 22.260 s
- 現在 elapsed: 26.370 s
- 差分: +4.110 s
- 改善率: -18.46%（悪化）

## write 系補助指標（FSSTAT 5-run 中央値）

- `pwrite_iblk_write_ms`: 52.931 ms
- `iblk_write_hrl_put_ms`: 19.925 ms
- `iblk_write_dec_ref_ms`: 22.748 ms
- `blk_alloc_set_usage_ms`: 0.775 ms

参考（Phase2 比較）:

- `pwrite_iblk_write_ms`: 33.572 -> 52.931（-57.66%）
- `iblk_write_dec_ref_ms`: 15.662 -> 22.748（-45.24%）
- `blk_alloc_set_usage_ms`: 0.373 -> 0.775（-107.77%）

## P3-T6 判定

- build/test: PASS
- `w3` elapsed 15%以上短縮（v2基準）: FAIL
- P99 write latency 25%以上短縮: 判定保留（現行計測系に p99 フィールド未実装）

総合: **P3-T6 は未達**（性能ゲート未達 + p99 未計測）

## 補足

- 現行 `FSSTAT_JSON` は合計時間ベースの指標が中心で、percentile（p50/p95/p99）を直接取得できない。
- 次アクションは、`kafsctl fsstat` へ write レイテンシ分布（少なくとも p99）を追加して再測定する。
