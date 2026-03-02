# Phase2 検証結果（2026-02-28）

対象: P2-T6（build/test PASS + `blk_alloc_set_usage_ms` 50%以上削減判定）

## 実行条件

- workload: `scripts/workload-npm-offline-local.sh`
- 実行回数: 5 run
- 計測出力: `report/perf/p2t6-validation-20260228-102845`
- Phase1 比較元: `report/perf/phase1-validation-20260228-081652`

## 5-run 中央値

- elapsed: 25.000 s
- user: 8.14 s
- sys: 4.47 s
- blk_alloc_set_usage_ms: 0.373 ms

## Phase1 比較（P2 ゲート）

- Phase1 `blk_alloc_set_usage_ms` 中央値: 6.262 ms
- Phase2 `blk_alloc_set_usage_ms` 中央値: 0.373 ms
- 差分: -5.889 ms
- 改善率: +94.04%

## 判定

- build/test: PASS
- `blk_alloc_set_usage_ms` 50%以上削減: PASS（+94.04%）
- P2-T6: PASS

## 補足

- elapsed は環境ノイズ（同日測定で二峰性）を観測するため、Phase2 ゲート判定は仕様通り `blk_alloc_set_usage_ms` を主指標とした。
