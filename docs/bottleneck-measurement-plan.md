# KAFS ボトルネック計測計画（ディレクトリ操作重視）

最終更新: 2026-02-28

## 目的

- 書込み性能低下の主因を「ロック」「ディレクトリ処理」「ジャーナル」「重複除去」に分解して定量化する
- 施策前後比較ができる再現可能なベースラインを作る

## クイックスタート

```bash
# まずは短時間版（W1/W2）
scripts/measure-bottleneck.sh --quick

# full（W1/W2/W3）
scripts/measure-bottleneck.sh

# strace 付き（重い）
KAFS_MEASURE_STRACE=1 scripts/measure-bottleneck.sh --quick
```

出力先:

- `report/perf/<timestamp>/`

## 前提の観測

- 4 スレッドでも CPU 使用率が 100% を大きく超えにくい
- CPU はほぼ使い切っている
- ディレクトリ操作を伴う workload で顕著に遅い

## 計測対象 KPI

### 1) スループット/レイテンシ

- op/s（create, mkdir, rename, unlink, lookup）
- p50/p95/p99 latency（各 op）
- 1 workload あたり総時間

### 2) CPU/並列度

- user/sys 比率
- スレッド別 CPU 使用率
- run queue 長

### 3) ロック競合

- lock 待ち時間の合計/平均/最大
- ロックごとの待ち比率（HRL bucket, bitmap, inode, journal）

### 4) ジャーナル

- BEGIN/COMMIT 回数
- fsync/fdatasync 回数
- group commit でのバッチサイズ分布

### 5) ディレクトリ処理

- dirent 探索回数と 1 回あたりコスト
- 同一ディレクトリへの集中度（ホットディレクトリ）

## ワークロード設計

### W1: ディレクトリ作成集中

- 多スレッドで同一親ディレクトリに create/mkdir を集中
- 目的: lock と dirent 探索競合を顕在化

### W2: Git 風ワークロード

- 小ファイル大量 create + rename + unlink
- 目的: 実利用に近い形で遅延要因を抽出

### W3: 比較用（ディレクトリ軽量）

- 既存ファイルへの連続 write 主体
- 目的: ディレクトリ依存の差分確認

## 計測ステップ

### Phase 0: ベースライン固定

- [ ] マシン条件（CPU governor, バックグラウンド負荷）を固定
- [ ] mount オプションと環境変数を記録
- [ ] 各 workload を 5 回以上実行し、中央値を採用

### Phase 1: 外部観測

- [ ] `perf stat` で CPU/cs/migrations/faults を取得
- [ ] `perf record` + flamegraph 相当でホットパスを可視化
- [ ] `strace -f -ttT` で syscall 待ち（fsync, futex, pwrite）を確認

### Phase 2: 内部計測（軽量インストルメント）

- [ ] ロック取得待ち時間カウンタを追加（集計のみ、常時 off 可能）
- [ ] dirent 探索/更新時間カウンタを追加
- [ ] ジャーナル commit/fdatasync カウンタを追加

### Phase 3: 仮説検証

- [ ] スレッド数 1/2/4/8 でスケーリング曲線を取得
- [ ] lock 待ち時間の増加率を比較
- [ ] dirent 処理が占める割合を算出

## 成果物

- [ ] ベースラインレポート（KPI 表 + グラフ）
- [ ] ホットスポット上位 5 件と根拠
- [ ] 次スプリントで着手する改善チケット（小さい順）

## 判定基準（改善着手のゲート）

- ロック待ちが総時間の 20% 超: ロック改善を優先
- dirent 処理が総時間の 30% 超: ディレクトリ索引化を優先
- fsync 系待ちが総時間の 20% 超: ジャーナル同期戦略を優先
- ハッシュ計算+HRL が総時間の 25% 超: 遅延 dedup を検討

## リスクと注意点

- 計測オーバーヘッドで結果が歪むため、内部計測は最小限にする
- キャッシュ影響を避けるため、ウォーム/コールド条件を分けて記録する
- 同一 workload で比較しないと結論がぶれる

## 直近 1 週間の実行案

1. Day 1-2: Phase 0 + Phase 1（外部観測）
2. Day 3-4: Phase 2（内部計測追加）
3. Day 5: Phase 3（仮説検証）
4. Day 6-7: レポート化と改善優先順位の決定
