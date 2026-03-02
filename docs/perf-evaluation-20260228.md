# KAFS 性能評価フェーズ（2026-02-28）

## 対象

- ベンチマーク系列: `report/perf/quick-series-fixed-20260228-021528`
- 実行回数: 5 run（全ケース success 5/5）
- ワークロード:
  - `w1_dir_ops`
  - `w2_git_ops`
  - `w3_npm_offline`（ローカル tarball + `file:` 依存、ネット不要）

## 5-run 中央値

| case | elapsed(s) | user(s) | sys(s) | success |
|---|---:|---:|---:|---:|
| w1_dir_ops | 2.860 | 0.03 | 0.11 | 5/5 |
| w2_git_ops | 5.210 | 0.05 | 0.26 | 5/5 |
| w3_npm_offline | 22.260 | 7.81 | 4.34 | 5/5 |

参照:

- `report/perf/quick-series-fixed-20260228-021528/SUMMARY_MEDIAN.md`
- `report/perf/quick-series-fixed-20260228-021528/metrics.tsv`

## 派生指標（中央値ベース）

- `w3 / w2` elapsed: **4.27x**
- `w3 / w1` elapsed: **7.78x**
- `w3 / w2` voluntary context switch: **10.81x**
- `w3 / w1` voluntary context switch: **24.37x**
- `w3` の `sys/(user+sys)`: **0.357**

## 評価（優先順位）

1. **ロック競合 + ディレクトリ処理を最優先**
   - 混在I/O (`w3`) で context switch が顕著に増加。
   - ディレクトリ操作を多く含むケースほど時間が伸びる傾向。
2. **ジャーナル同期の最適化を次点**
   - `sys` 時間の寄与が無視できず、同期・メタ更新の待ち削減余地がある。
3. **遅延 dedup（2段階書込み）は第3優先**
   - 効果は大きい可能性があるが、整合性・再マップ設計コストが高い。

## 次スプリントでの実施項目（評価フェーズ継続）

- `perf stat/record` による lock・futex・I/O 待ちの分解
- `strace -f -ttT` で `fsync/fdatasync/pwrite/futex` 待ちの可視化
- 軽量インストルメント追加（lock wait / dirent scan / journal flush counters）

## 判定

- 現時点の実測から、最初の改善ターゲットは **C（ロック範囲改善） + D（ディレクトリ特化）**。
