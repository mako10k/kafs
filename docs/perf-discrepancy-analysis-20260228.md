# KAFS 予想と結果の差異分析（2026-02-28）

## 背景

- 実装対象（Impl1）:
  - `kafs_dirent_search`
  - `kafs_op_readdir`
- 変更内容:
  - 都度 `kafs_dirent_read` で小刻みに辿る方式から、ディレクトリ全体スナップショット (`kafs_dir_snapshot`) を 1 回取得して走査する方式へ変更。

想定は「ディレクトリ探索の I/O 回数削減により、混在 I/O 系ワークロードが改善する」だった。

## 実測

比較元:

- baseline: `report/perf/quick-series-fixed-20260228-021528/metrics.tsv`
- impl1: `report/perf/quick-series-impl1-20260228-023517/metrics.tsv`
- 比較表: `report/perf/quick-series-impl1-20260228-023517/SUMMARY_COMPARE.md`

中央値差分（elapsed）:

- `w1_dir_ops`: -0.35%
- `w2_git_ops`: -0.77%
- `w3_npm_offline`: +2.29%

統計的ばらつき（bootstrap, median差の95%CI）:

- `w1_dir_ops`: [-0.70%, +19.93%]
- `w2_git_ops`: [-24.42%, +18.81%]
- `w3_npm_offline`: [-11.74%, +15.99%]

=> いずれも CI が広く、今回の差分は「有意に改善した」とは言いにくい。

## 差異の解釈（なぜ効かなかったか）

### 1) 変更範囲がホットスポット全体に届いていない

- 今回の変更は `dirent search` / `readdir` に限定。
- しかし現ワークロードは write・rename・unlink・fsync・journal・hash/dedup が支配的で、単純な名前探索改善だけでは全体時間に効きづらい。

### 2) 既存実装ですでに snapshot 化済みの経路がある

- `kafs_dirent_add_nolink` / `kafs_dirent_remove_nolink` は元々 `kafs_dir_snapshot` ベース。
- create/unlink/rename の主要更新経路の一部は既に同種戦略で、追加余地は限定的。

### 3) workload 特性が「lookup 巨大1箇所」より「小さなディレクトリ多数」寄り

- `w3_npm_offline` は `npm install` 由来で階層が深く、比較的小さいディレクトリを大量に扱う傾向。
- この場合、1 directory あたりの探索短縮より、ロック競合・メタ更新・同期待ちの影響が相対的に大きい。

### 4) 新方式の固定コスト（malloc/snapshot copy）が相殺要因

- 毎回スナップショット確保・コピーが発生。
- 対象ディレクトリが小さいと、旧方式との差が小さく、固定コストが優位性を相殺しうる。

## メトリクス上の示唆

- `w3` で `user` は微減（-2.69%）だが、`sys` はほぼ不変、`vcs` もほぼ不変。
- これは「CPU計算の軽微改善はあり得るが、カーネル側待ち/同期/競合ボトルネックは残存」のシグナル。

## 結論（今回の実装評価）

- Impl1 は正しい方向の局所最適化だが、現行ベンチでは効果が小さく、期待した改善量には届いていない。
- 予想との差異は「仮説の当たり外れ」よりも「支配ボトルネックが別層にある」ことが主因。

## 次アクション（差異を潰すための順序）

1. **計測の分解精度を上げる**
   - lock wait / dirent scan / journal flush カウンタを追加して、総時間占有率を可視化。
2. **効果優先で次実装**
   - C: ロック範囲改善（ホットロック分割、クリティカルセクション短縮）
   - D: ディレクトリ特化（索引化・キャッシュ）
3. **評価方法の厳密化**
   - 10-run 以上 + warm/cold 分離 + 同時負荷固定で再比較。
