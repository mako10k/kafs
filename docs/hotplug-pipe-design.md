# Hotplug Pipe Design

## 1. 目的
- socketpair で前後段を接続し、前段が後段の起動と再起動を管理する。
- ユーザは `kafs` のみを意識する。

## 2. IPC 構成
- 前段が `socketpair(AF_UNIX, SOCK_STREAM, 0, fds)` を生成。
- 前段は `fds[0]` を使用、後段は `fds[1]` を使用。
- FD の引き渡しは `fork` 後に `exec` するため継承で十分。
- `KAFS_HOTPLUG_BACK_FD` のような環境変数で番号を通知するか、固定番号化する。

## 3. 起動フロー
1. 前段起動。
2. socketpair 作成。
3. fork。
4. 子: `kafs-back` を exec。
5. 親: RPC ハンドシェイク (HELLO -> SESSION_RESTORE -> READY)。
6. 通常 I/O。

## 4. 再起動フロー (意図的)
- 前段が再起動要求を起点に子を終了。
- socketpair を再生成。
- fork/exec で子を再起動。
- ハンドシェイク後、epoch をインクリメント。

## 5. 異常終了フロー (意図しない)
- 前段が `waitpid`/poll で異常終了を検知。
- 終了理由 (signal/exit code) を出力。
- 前段は終了。

## 6. 状態管理
- session_id と epoch は前段が管理。
- 後段は SESSION_RESTORE を受信して再構築する。

## 7. 互換性
- 既存 RPC バージョン管理を維持。
- 互換性 NG は前段が拒否/待機。

## 8. デバッグ
- 起動/再起動/異常終了は INFO で出力。
- 互換性不一致は ERROR で出力。

## 9. 残課題
- FD の引き渡し方式 (env vs 固定 fd)。
- 再起動トリガの API 設計 (kafsctl など)。
