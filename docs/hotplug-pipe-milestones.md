# Hotplug Pipe Milestones

M1: 設計確定
- hotplug-pipe-plan/requirements/design を作成。
- socketpair + ProcessGroup 方針に合意。

M2: 基本起動経路
- 前段から後段の fork/exec を実装。
- socketpair 経由で RPC ハンドシェイク。
- 通常 I/O が動作。

M3: 再起動経路
- 意図的再起動のフロー確立。
- epoch 更新の確認。

M4: 異常終了ハンドリング
- 異常終了時の理由表示。
- 前段の終了を確認。

M5: 運用/テスト
- 手動再接続テスト。
- 主要テストの合格。
