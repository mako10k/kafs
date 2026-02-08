# KAFS ホットプラグ 実装チケット案

最終更新: 2026-02-07

## チケット一覧

T1: 前段/後段のプロセス分割
- 対象: M1
- 目的: 前段/後段の骨格分離
- 完了条件: UDS で HELLO/READY が疎通

T2: UDS RPC 基盤
- 対象: M1/M2
- 目的: 共通ヘッダと req_id の基盤
- 完了条件: 前段/後段で request/response が往復

T3: getattr/read/write/truncate RPC
- 対象: M2
- 目的: 主要 I/O 経路の切り出し
- 完了条件: data_mode 切替で read/write 成功

T4: 断線待機/タイムアウト/キュー上限
- 対象: M2/M6
- 目的: 断線時の挙動を確定
- 完了条件: 断線時に期待通りのエラー

T5: session_id/epoch/SESSION_RESTORE
- 対象: M3
- 目的: 再接続の基盤
- 完了条件: 後段再起動で I/O 復旧
- 補足: 初期実装は open_handle_count=0 の SESSION_RESTORE のみ (ハンドル表は後続)

T6: req_id 冪等性
- 対象: M3
- 目的: 再送抑止と重複排除
- 完了条件: 再送時に二重適用なし

T7: 追加オペレーション
- 対象: M4
- 目的: readdir/rename/unlink/mkdir/rmdir/open/release/flush/fsync
- 完了条件: 追加オペレーションが RPC 経由で成功

T8: COPY RPC
- 対象: M4
- 目的: copy_file_range 相当の RPC 実装
- 完了条件: COPY が成功

T9: kafsctl hotplug
- 対象: M5
- 目的: restart/status/compat/env を実装
- 完了条件: 各サブコマンドが動作

T10: 互換性チェック
- 対象: M5
- 目的: version/feature_flags の判断
- 完了条件: 非互換で警告/拒否

T11: ログ/メトリクス
- 対象: M5
- 目的: 観測性整備
- 完了条件: 指標が出力される

T12: テストスイート
- 対象: M6
- 目的: 再接続/タイムアウト/キュー上限
- 完了条件: テストが PASS
