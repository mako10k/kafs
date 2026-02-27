# Hotplug IPC Requirements

必須要件
- IPC は socketpair を使用する。
- UDS への依存は廃止・削除対象とする。
- 後段の起動は前段が行う (fork/exec)。
- 前段/後段は同一 ProcessGroup に所属する。
- 親終了時は socketpair close 連鎖で子は終了する。
- 子の意図しない終了は前段が検知し、理由を表示して前段も終了する。
- 子の意図的終了 (再起動) は前段が socketpair を再生成し、再起動する。
- 既存の RPC 仕様は極力維持する。
- kafs-back の単体起動は維持する。

運用要件
- ユーザは `kafs` 実行のみで利用できる。
- 後段の起動/再起動はユーザ操作を不要にする。

互換性要件
- session_id/epoch の意味は維持する。
- 既存の `kafsctl hotplug status` 表示は維持する。

失敗時挙動
- 子の異常終了: 前段は理由を表示して終了。
- 互換性不一致: 前段は拒否/待機の方針に従う。
