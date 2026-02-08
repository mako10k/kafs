# KAFS ホットプラグ 設計

最終更新: 2026-02-07

関連ドキュメント
- hotplug-plan.md
- hotplug-requirements.md
- hotplug-pipe-plan.md
- hotplug-pipe-design.md

注記
- 現行実装は socketpair (AF_UNIX) で前後段を接続し、前段が後段を fork/exec する。
- UDS 前提の記述は過去設計として残している。詳細は hotplug-pipe-*.md を参照。

目次
- 1. コンポーネント
- 2. 役割境界
- 3. RPC プロトコル概要
- 4. 代表的な RPC フロー
- 5. 再接続プロトコル
- 6. 冪等性
- 7. 断線時挙動
- 8. kafsctl 制御フロー
- 9. 状態管理
- 10. 互換性
- 11. デバッグログ設計
- 12. 再接続ステートマシン設計
- 13. 前段/後段の責務分解
- 14. 監視/運用設計

用語
- FUSE-FRONT: /dev/fuse を保持する常駐前段。
- KAFS-BACK: ロジック特化の後段。
- socketpair: 前後段を接続する AF_UNIX ソケットペア。
- RPC: 前段/後段間のリクエスト/レスポンス。
- req_id: 冪等性のための一意リクエスト ID。

## 1. コンポーネント

- FUSE-FRONT
  - FUSE オペレーションの受け口。
  - 画像ファイル I/O とジャーナル/HRL を担当。
  - 再接続管理、req_id 管理、I/O タイムアウト管理を担当。

- KAFS-BACK
  - inode/dirent/権限/パス解決などのロジック。
  - 物理 I/O は行わず、操作計画を返す。

- socketpair RPC
  - 前段と後段を接続するローカル IPC。
- kafsctl
  - 前段に対する制御インタフェース。
  - 後段の再起動要求、再接続状態の確認を行う。

## 2. 役割境界

前段が担当する処理
- 画像ファイルの open/mmap/pread/pwrite。
- HRL 参照の増減、ブロックの物理確保と解放。
- FUSE コンテキスト (uid/gid/pid) の取得。

後段が担当する処理
- パス解決、権限判定、inode/dirent 更新ロジック。
- ブロック参照の計画 (どの iblock を更新するか) を決定。

## 3. RPC プロトコル概要

ヘッダ
- magic
- version
- op
- req_id
- payload_len
- session_id
- epoch

レスポンス
- req_id
- result (0 or -errno)
- payload_len
- payload

互換性情報
- front_version (major.minor)
- back_version (major.minor)
- feature_flags

## 3.1 RPC メッセージ定義 (概要)

共通ヘッダ (固定長)
- magic (u32)
- version (u16)
- op (u16)
- flags (u32)
- req_id (u64)
- session_id (u64)
- epoch (u32)
- payload_len (u32)

エンディアン方針
- UDS ではホストエンディアンとする。
- 将来 TCP/UDP に拡張する場合はネットワークバイトオーダー (big-endian) を使用する。
- ヘッダに endian フラグを設け、プロトコルの段階的移行を可能にする。

共通レスポンス (固定長)
- req_id (u64)
- result (s32)
- payload_len (u32)

op 一覧 (初期)
- HELLO
- READY
- SESSION_RESTORE
- STATUS
- RESTART_BACK
- GETATTR
- READ
- WRITE
- TRUNCATE
- UNLINK
- RENAME
- MKDIR
- RMDIR
- OPENDIR
- READDIR
- OPEN
- RELEASE
- FLUSH
- FSYNC
- COPY

payload 例
- HELLO: { back_version, feature_flags }
- READY: { }
- SESSION_RESTORE: { open_handle_count } (T5 は open_handle_count=0 のみ送信)
- STATUS: { state, wait_queue_len, wait_timeout_ms, last_error }
- GETATTR: { ino, uid, gid, pid }
- READ: { ino, off, size, uid, gid, pid, data_mode }
- WRITE: { ino, off, size, data[], uid, gid, pid, data_mode }
- TRUNCATE: { ino, size }
- READDIR: { ino, off, max_count }
- COPY: { src_ino, src_off, dst_ino, dst_off, size }

レスポンス payload 例
- GETATTR: { stat }
- READ: { size, data[] } (data_mode=INLINE の場合)
- WRITE: { size }
- READDIR: { entry_count, [ino, name]... }

data_mode
- INLINE: data[] を RPC に含める。
- PLAN_ONLY: 後段は計画のみ返す。
- SHM: 共有メモリ経由で data を受け渡す。

実装メモ (T3)
- 現状の試作では後段が画像を開いて GETATTR/READ/WRITE/TRUNCATE を実行している。
- 本来の目標は「前段のみが I/O を担当し、後段はロジックのみ」に分離すること。

## 3.2 ペイロード注意事項

- 可変長フィールドや配列の扱いは実装で一貫性を保つこと。
- サイズ上限は運用と安全性を見て設定し、過大な payload を拒否すること。
- エンディアンの扱いは transport ごとに統一し、混在させないこと。
- READ/WRITE の data_mode は運用で切り替え可能にすること。

## 4. 代表的な RPC フロー

READ
1. 前段: READ(ino, off, size, uid/gid/pid) を送信。
2. 後段: data_mode に応じて計画を返すか、データ返却を選ぶ。
3. 前段: 物理 I/O を行い結果を返す (計画モードの場合)。

WRITE
1. 前段: WRITE(ino, off, data, uid/gid/pid) を送信。
2. 後段: data_mode に応じて計画を返すか、必要なメタ更新のみ返す。
3. 前段: 物理 I/O とメタ更新を実施。

TRUNCATE
1. 前段: TRUNCATE(ino, size) を送信。
2. 後段: 解放計画を返す。
3. 前段: 解放を実行。

## 5. 再接続プロトコル

- 前段は session_id と epoch を保持。
- 後段は再起動時に HELLO を送信。
- 前段は SESSION_RESTORE を返し、open ハンドル一覧を送る。
- 後段は再構築完了後に READY を通知。

互換性判定
- major が異なる場合は非互換と判断し、再接続を拒否。
- minor が異なる場合は feature_flags を参照し、互換不可なら警告を出して拒否。
- 非互換時は前段が拒否理由を返し、kafsctl から確認できる。

## 6. 冪等性

- req_id は前段で採番し、後段は短期キャッシュで重複排除。
- 書き込み系は req_id の再送で同一結果になることを保証。
- タイムアウト後の req_id は復旧後も再送しない。

## 7. 断線時挙動

- 前段は I/O を待機キューに入れる。
- 待機時間を超えた要求は EIO を返す。
- 待機キューが上限に達した場合は即時エラーを返す。
- 断線中の umount は優先して処理し、新規 mount は拒否する。

## 8. kafsctl 制御フロー

再起動要求
1. kafsctl が前段に RESTART_BACK を送る。
2. 前段が後段停止を指示し、再接続待機へ移行する。
3. 後段が起動して HELLO を送信。
4. 互換性判定後に SESSION_RESTORE を送信。
5. READY 受領で復旧完了。

状態取得
- kafsctl は STATUS を送信し、前段は接続状態、待機キュー長、互換性情報を返す。

## 8.1 kafsctl コマンド仕様

コマンド
- kafsctl hotplug status
  - 前段の接続状態、再接続待機時間、待機キュー長を表示する。
- kafsctl hotplug restart-back
  - 後段の再起動を要求する。
- kafsctl hotplug compat
  - 前段/後段のバージョンと互換性判定結果を表示する。
- kafsctl hotplug set-timeout <ms>
  - 断線待機タイムアウトを更新する。
- kafsctl hotplug env list
  - 後段再起動に渡す環境変数の一覧を表示する。
- kafsctl hotplug env set <key>=<value>
  - 後段再起動用の環境変数を更新する。
- kafsctl hotplug env unset <key>
  - 後段再起動用の環境変数を削除する。

出力 (status/compat)
- front_version
- back_version
- compat_result (ok|warn|reject)
- last_error
- reconnect_count
- wait_queue_len
- wait_timeout_ms

戻り値
- 0: 成功
- 1: 互換性警告 (warn)
- 2: 互換性非対応 (reject)
- 3: 前段未起動

環境変数の扱い
- 前段は後段再起動時に渡す環境変数を保持する。
- env set/unset は前段の保持情報のみ更新し、再起動時に反映される。

## 9. 状態管理

- open ハンドル表は前段で保持する。
- 後段は必要な情報を再接続時に受け取る。
- メタデータの永続状態は画像ファイルとジャーナルに依存する。

## 10. 互換性

- RPC version で互換性を管理する。
- 前段が後段の version を検証し、互換不可なら待機か拒否する。

## 11. デバッグログ設計

目的
- 再接続、互換性判定、待機キューの状態を観測可能にする。
- 不具合時に前段/後段の役割境界を追跡できるようにする。

ログレベル
- ERROR: 互換性拒否、再接続失敗、タイムアウト。
- WARN: 互換性警告、再送抑止。
- INFO: 接続/切断、再接続成功、kafsctl 操作。
- DEBUG: RPC 送受信、req_id、待機キュー長。

ログ項目 (共通)
- timestamp
- component (front/back/kafsctl)
- session_id
- epoch
- req_id (該当時)
- op
- result

ログポリシー
- 前段と後段で同一 req_id を必ず記録する。
- 待機キュー長は一定間隔で INFO に出力する。
- DEBUG は環境変数で有効化する (例: KAFS_HOTPLUG_DEBUG=1)。

サンプル (INFO)
- front: reconnect success session=... epoch=...
- back: hello version=... feature_flags=...

## 12. 再接続ステートマシン設計

前段 (FUSE-FRONT)
- INIT: 起動直後。
- RUNNING: 後段と接続済みで通常運転。
- WAIT_BACK: 後段再起動待ち。I/O は待機キューへ。
- RESYNC: SESSION_RESTORE 済みで再構築中。
- DEGRADED: タイムアウト発生。I/O は EIO で失敗。

遷移 (前段)
- INIT -> RUNNING: 後段接続完了。
- RUNNING -> WAIT_BACK: 断線検知 or kafsctl restart-back。
- WAIT_BACK -> RESYNC: HELLO 受信と互換性 OK。
- RESYNC -> RUNNING: READY 受信。
- WAIT_BACK -> DEGRADED: 待機タイムアウト。
- DEGRADED -> WAIT_BACK: 後段が再接続を開始。

後段 (KAFS-BACK)
- INIT: 起動直後。
- HELLO_SENT: HELLO 送信済み。
- RESTORING: SESSION_RESTORE 受信、状態構築中。
- READY: 通常運転。

遷移 (後段)
- INIT -> HELLO_SENT: 前段へ HELLO 送信。
- HELLO_SENT -> RESTORING: SESSION_RESTORE 受信。
- RESTORING -> READY: READY 送信。
- READY -> HELLO_SENT: 再接続開始。

kafsctl
- STATUS 取得で前段の状態を表示。
- RESTART_BACK で前段を WAIT_BACK に遷移させる。

## 13. 前段/後段の責務分解

前段 (FUSE-FRONT)
- FUSE 受信、FUSE コンテキストの取得。
- 画像ファイルの open/mmap/pread/pwrite。
- HRL/bitmap/journal などの物理 I/O を伴う更新。
- ブロックハッシュ計算は前段で行うことを推奨。
- 再接続管理、req_id 発行、待機キュー管理。
- kafsctl からの制御を受け付ける。

後段 (KAFS-BACK)
- パス解決、権限判定、inode/dirent のロジック更新。
- ブロック割当の計画 (どの iblock を更新/解放するか)。
- 互換性判定と feature_flags の宣言。
- 再接続時のセッション復元処理。

## 14. 監視/運用設計

メトリクス (前段)
- reconnect_count
- reconnect_last_ms
- wait_queue_len
- wait_timeout_ms
- compat_result
- rpc_error_count

メトリクス (後段)
- last_hello_ms
- restore_duration_ms
- ready_count

ログ出力先
- 既定は stderr。
- 追加の出力先は環境変数で切替できるようにする。

アラート条件 (目安)
- 連続再接続失敗が閾値超過。
- wait_queue_len が上限を越えた。
- compat_result が reject。

## 15. リファクタ計画 (front が全 I/O、back はロジックのみ)

制約
- front の I/O を優先し、back から画像アクセスを段階的に撤去する。
- 既存 INLINE は維持し、PLAN_ONLY を先に有効化する。
- 互換性情報は status に追加し、既存出力は維持する。

移行ステップ
- S1: READ/WRITE の PLAN_ONLY を導入し、front がローカル I/O を実行。
- S2: back の READ/WRITE から画像アクセスを削除し、計画応答のみ返す。
- S3: TRUNCATE/COPY なども計画応答化し、front が実行。

補足
- KAFS_BACK_ENABLE_IMAGE を有効にした場合のみ back が画像へ直接アクセスする。

タスク分解
- RPC: PLAN_ONLY 応答の最小メタデータ定義と互換性チェック。
- front: PLAN_ONLY 受信時にローカル I/O を実行。
- back: PLAN_ONLY で計画のみ返す (画像アクセスなし)。
- kafsctl: status に front/back 互換性情報を出力。
- テスト: PLAN_ONLY と INLINE の両方で基本 read/write を確認。
