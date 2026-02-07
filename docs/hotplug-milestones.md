# KAFS ホットプラグ 実装マイルストーン

最終更新: 2026-02-07

## 目的

- 前段/後段分離を段階的に実装し、再接続まで到達する。
- 機能と安全性のゲートを設定し、段階ごとに検証する。

## マイルストーン

M0: ドキュメント確定
- 大方針/要件/設計/マイルストーンを確定。
- 非目標と制約を明文化。
 受け入れ基準
 - hotplug-plan/requirements/design/milestones が揃っている。
 - 非目標に画像差し替えが明記されている。

M1: 骨格分離
- 前段/後段プロセス分割。
- UDS 接続と HELLO/READY の疎通。
- 前段単独起動と終了が可能。
 受け入れ基準
 - 前段が単独で起動/終了できる。
 - 後段が起動すると HELLO/READY が完了する。

M2: 主要 I/O 経路
- getattr/read/write/truncate の RPC 化。
- data_mode (INLINE/PLAN_ONLY/SHM) の選択肢を追加。
- 断線時の待機/タイムアウト/キュー上限の挙動を実装。
 受け入れ基準
 - getattr/read/write/truncate が RPC 経路で動作する。
 - data_mode を切り替えても read/write が成功する。
 - 断線時に待機/タイムアウト/即時エラーが期待通り。

M3: 再接続
- session_id/epoch の導入。
- SESSION_RESTORE と READY による復旧。
- req_id の冪等性と再送抑止。
 受け入れ基準
 - 後段再起動後に I/O が復旧する。
 - タイムアウト済み req_id は再送されない。

M4: 追加オペレーション
- readdir/rename/unlink/mkdir/rmdir/open/release/flush/fsync。
- COPY 相当の RPC を実装。
 受け入れ基準
 - 追加オペレーションが RPC 経由で動作する。
 - COPY が正常に完了する。

M5: 運用/観測
- kafsctl で restart/status/compat/env。
- メトリクスとログ出力の整備。
- 互換性チェックと警告/拒否動作。
 受け入れ基準
 - kafsctl の status/compat/env が動作する。
 - 互換性不一致で警告または拒否が発生する。

M6: テスト・品質
- 再接続テスト、タイムアウトテスト、キュー上限テスト。
- 断線中の umount/mount の挙動確認。
 受け入れ基準
 - 主要テストが PASS。
 - 断線中の umount/mount の方針が確認できる。

## ゲート条件

- M1: HELLO/READY が安定して成功。
- M2: read/write が通常/断線時に期待通り。
- M3: 後段再起動で I/O が復旧。
- M5: kafsctl の status/compat が正しく表示。

## リスク

- RPC 境界の性能劣化。
- 断線時のデータ整合性。
- 互換性判断の誤り。
