# Phase1 検証結果（2026-02-28）

対象: P1-T6（build/test PASS + w3 中央値改善判定）

## 実行条件

- workload: scripts/workload-npm-offline-local.sh
- 実行回数: 5 run
- 計測出力: report/perf/phase1-validation-20260228-081652

## 5-run 中央値

- elapsed: 24.640 s
- user: 8.74 s
- sys: 4.67 s

## ベースライン比較

- 対 baseline A（22.260 s, quick-series-fixed-20260228-021528）
  - 改善率: -10.69%（悪化）
  - 判定: FAIL（目標 +5% 以上を未達）

- 対 baseline B（26.880 s, quick-series-20260228-020909）
  - 改善率: +8.33%
  - 判定: PASS（目標 +5% 以上を達成）

## 結論

- どのベースラインを Phase1 ゲートに採用するかで判定が分岐する。
- 直近の fixed 系ベースライン（22.260 s）を採用する場合、現状は P1-T6 未達。
- 旧 baseline（26.880 s）を採用する場合は達成。
