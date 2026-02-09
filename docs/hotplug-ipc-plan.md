# Hotplug IPC Plan

目的
- UDS ベースから socketpair ベースへ再設計する。
- 後段のプロセス管理を前段で完結させる。
- ユーザはアーキテクチャを意識せずに利用できるようにする。

注記
- IPC は socketpair のみを使用する。UDS 依存は廃止・削除対象。

関連ドキュメント
- hotplug-ipc-requirements.md
- hotplug-ipc-design.md
- hotplug-ipc-milestones.md
- hotplug-ipc-tickets.md

スコープ
- 前段が socketpair を生成し、後段を fork/exec で起動。
- 既存 RPC を socketpair 上で利用。
- 再起動/異常終了の扱いを明確化。
- kafs-back の単体起動は維持。

非スコープ
- SHM 共有メモリデータ転送。
- 大規模な RPC 互換性破壊。

前提
- FUSE のマウントは従来どおり前段が担当。
- 画像ファイルは前段が管理する（既存方針を踏襲）。

ゴール
- 再接続が安定し、再起動時に epoch が更新される。
- 意図しない後段終了時は前段も終了し、明確な理由を表示。
