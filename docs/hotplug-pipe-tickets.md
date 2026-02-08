# Hotplug Pipe Tickets

T0: docs
- hotplug-pipe-plan/requrements/design/milestones/tickets を作成。

T1: socketpair 起動
- 前段で socketpair 作成。
- 前段が fork/exec で kafs-back を起動。
- 後段は socketpair FD を使用して RPC を開始。

T2: ProcessGroup
- 親子を同一 ProcessGroup に所属させる。
- 親終了時の後始末を確認。

T3: 再起動フロー
- 意図的再起動: 子終了 -> socketpair 再生成 -> 再起動。
- epoch をインクリメント。

T4: 異常終了フロー
- waitpid で子終了を検知。
- signal/exit code を表示。
- 前段は終了。

T5: kafs-back 単体
- socketpair なしでの単体起動が従来どおり動作することを確認。

T6: テスト
- 手動再接続確認。
- 回帰テスト (make check)。
