# KAFS: ブロックレベル重複排除（Dedup）設計計画

最終更新: 2025-08-25

## 目的と背景

- 目的: 物理データブロックの重複を排除し、ディスク使用量を削減する。頻出データの共有により書き込み・読み出しの効率化（キャッシュヒット向上）も狙う。
- 背景: 現状の KAFS は inode からデータブロック（直接/単重/二重/三重間接）へ直接参照している。重複検知は行っていない。
- 方針: inode→[新]ハッシュ参照レイヤ→物理ブロック という一段の間接層を追加し、ブロック内容に基づく内容アドレッサ化（Content-Addressed）を導入する。

非目標（初期段階）:
- サブブロック（可変長）単位の重複排除
- ジャーナリング/トランザクションの本格導入
- 圧縮

## 現状の概要（サマリ）

- superblock: 最大 inode 数、総/予約含むブロック数、空きブロック数、最初のデータブロック、log2 ブロックサイズなど。
- ブロックビットマップ: `kafs_blkmask_t` 群でブロックの使用状態を管理。
- inode: `i_blkreftbl[15]`（12 直接 + 1/2/3 重間接）でデータブロック番号を参照。小サイズ（60B 以下）は inline。
- 読み書き: `kafs_ino_iblk_read/write/release` が（間接を辿って）物理ブロック番号を取得し、`pread/pwrite` を実施。

## 新設計の全体像（後方互換なし）

- 新レイヤ: ハッシュ参照レイヤ（Hash Reference Layer; HRL）を inode と物理ブロックの間に挿入する（旧フォーマットからの互換は持たない）。
  - inode の `i_blkreftbl` は「物理ブロック番号」ではなく「ハッシュ参照 ID（HRID）」を保持する。
  - HRL は HRID → { 物理ブロック番号, 参照カウント, 強ハッシュ } を引ける。
  - HRL 内部では「高速ハッシュ」をインデックスに利用し、衝突検証には「強ハッシュ」を用いる。

- ハッシュ方式（推奨）:
  - 高速ハッシュ: xxHash64（64bit）。インデックスキー・一次フィルタ用。
  - 強ハッシュ: BLAKE3-256（32bytes）。衝突回避用の確定比較。

- 書き込みフロー（1 ブロック単位）:
  1) 書き込むブロック内容から [高速ハッシュ, 強ハッシュ] を計算
  2) 高速ハッシュをキーに HRL のインデックスを探索（同一高速ハッシュ候補群）
  3) 候補中の強ハッシュ一致エントリがあれば、それを再利用（参照カウント++、既存の物理ブロックを共有）。
  4) なければ新規物理ブロックを確保・書き込みし、HRL に新規エントリ（HRID）を追加（参照カウント=1）。
  5) inode の該当 i ブロック参照を HRID に更新。

- 読み出しフロー:
  - inode の HRID を HRL で引いて物理ブロック番号を得る → 物理ブロックを `pread`。

- 解放（truncate/trim/unlink）:
  - inode の HRID を HRL で引き、参照カウント--。0 になれば物理ブロックを解放、インデックスから該当エントリを削除。

- 特別扱い:
  - 全ゼロブロックは「ゼロの共有ブロック」を 1 つだけ持ち、HRID を固定値にする（物理を持たず論理的にゼロ扱いでもよい）。
  - 60B 以下の inline データは当面従来通り（非 dedup）。将来は可変長 dedup の検討余地。

## オンディスク構造（新規・互換なし）

領域の拡張: 既存の superblock/bitmap/inode table の後に HRL のための領域を確保する。

- Superblock 拡張フィールド（確定方針）
  - `s_magic`: フォーマット識別子（例: 0x4B414653 = 'KAFS'）
  - `s_format_version`: フォーマットバージョン（例: 2 = HRL 採用版）
  - `s_hash_algo_fast`: 高速ハッシュ識別子（固定: xxh64）
  - `s_hash_algo_strong`: 強ハッシュ識別子（固定: BLAKE3-256）
  - `s_hrl_index_offset`, `s_hrl_index_size`: ハッシュインデックス領域（オープンアドレッシング配列）
  - `s_hrl_entry_offset`, `s_hrl_entry_cnt`: HR エントリ表（HRID → 実体）

- ハッシュインデックス（固定長バケット配列）
  - バケット構造（例）:
    - `fast_hash` (u64)
    - `hrid` (u32 or u64; エントリ表のインデックス)
    - `state` (u8; 空/占有/削除 tombstone)
  - 解決法: 線形探索 or Robin Hood ハッシング
  - 補助: ロードファクタ上限を 0.7 程度に設定

- HR エントリ表（HRID → 実体, 固定長）
  - 構造（例）:
    - `strong_hash[32]` (BLAKE3-256)
    - `phys_blo` (u32) 物理ブロック番号
    - `refcnt` (u32)
    - `flags` (u8)（例: ZERO ブロック, DIRTY 等）

- ID 空間
  - HRID: 32bit で十分（テーブル上限 ~4G エントリ）。将来拡張性を考え 64bit も検討可。

## メモリ上のキャッシュ

- 起動時にインデックス全体を mmap して直接参照可能。
- 熱データ用に LRU キャッシュ（fast_hash→HRID）を持つと再検索コストが低減。

## 主要アルゴリズム

- 検索（lookup by content）
  - 入力: fast_hash, strong_hash
  - インデックス: fast_hash でヒットしたバケットから線形/Robin Hood で探索
  - strong_hash が一致する HRID を返す。無ければ NotFound。

- 追加（put）
  - 空きバケットを探索して fast_hash, hrid を配置
  - エントリ表に strong_hash, phys_blo, refcnt=1 を書き込む

- 削除（dec/erase）
  - refcnt を減らし 0 → 物理ブロック解放 → エントリ表クリア → インデックスの該当 fast_hash バケットを tombstone 化

- 物理ブロック管理との連携
  - 既存の `kafs_blk_alloc`, `kafs_blk_release`, `kafs_blk_set_usage` をそのまま流用

## API/コード変更点（ドラフト）

新規 API（`kafs_hash.h` 新設想定）
- `int kafs_hl_lookup(struct kafs_context*, const void* data, kafs_blksize_t, kafs_hrid_t* out_hrid, kafs_blkcnt_t* out_blo)`
- `int kafs_hl_put(struct kafs_context*, const void* data, kafs_blksize_t, kafs_hrid_t* out_hrid, kafs_blkcnt_t* out_blo)`
- `int kafs_hl_inc(struct kafs_context*, kafs_hrid_t)`
- `int kafs_hl_dec(struct kafs_context*, kafs_hrid_t)`

既存コード修正（破壊的変更）
- `kafs_ino_ibrk_run` 系: 戻り値を "物理ブロック番号" ではなく "HRID" に切替。
- `kafs_ino_iblk_read`: HRID→HRL 参照→phys_blo→`kafs_blk_read`
- `kafs_ino_iblk_write`: HRL 参照/登録→HRID を i_blkreftbl に設定（必要なら旧 HRID の `dec`）
- `kafs_ino_iblk_release`: HRID の `dec` のみ（refcnt==0 なら HRL 内で物理解放）
- Superblock: 新規必須フィールド（magic/version/hash/HRL offset 等）の初期化/読み書き（旧フォーマット読込は非対応）

破壊的変更点一覧
- inode の `i_blkreftbl` 参照先を「物理ブロック番号」→「HRID」に変更
- superblock に magic/version および HRL 関連フィールドを追加
- 旧イメージ（v1 相当）はマウント不可（フォーマット不一致として扱う）

（互換性オプションは無し。常に HRL フォーマットで動作する）

## 整合性と障害耐性

- 最小限の書き込み順序保証:
  1) 物理ブロック書き込み完了
  2) HR エントリ表（phys_blo, strong_hash, refcnt）更新
  3) インデックス（fast_hash, hrid）挿入
  4) inode の HRID 更新
- 削除時は逆順で（inode→HRL refcnt→0→インデックス tombstone→物理解放）。
- 将来: 小規模 redo log（リング）を superblock 直後に確保してクラッシュ回復容易化。

## パフォーマンス/チューニング

- ハッシュ計算コスト: xxh64 は高速、BLAKE3 も SIMD で高速。計測し、必要なら強ハッシュは候補一致時のみ遅延計算（2 段階計算）も可能。
- インデックス: Robin Hood 方式で探索長のばらつきを抑制。
- 全ゼロブロックの特別扱いはヒット率が高く効果的。
- マルチスレッド: FUSE の `-s` 無効（マルチスレッド）時は HRL への更新にスピンロック/ミューテックスを導入。

## 容量計画

- HR エントリ表容量 = 想定最大ユニークブロック数 × エントリサイズ（~40B）
- インデックス容量 = エントリ数 / 負荷率（0.7） × バケットサイズ（~16B）
- 例: 1GiB, 4KiB ブロック → 最大 ~262,144 ブロック
  - 50% ユニークと仮定 → 131,072 エントリ → エントリ表 ~5.2MiB, インデックス ~3.0MiB

## テスト計画

- 単体
  - 同一内容ブロックを複数回書き込む→物理ブロック数は 1、refcnt が増えること
  - truncate/unlink で refcnt が減り 0 で解放されること
  - すべてゼロのブロックが共有されること
- 機能
  - 直接/単重/二重/三重間接の各境界を跨ぐ読み書きの整合性
  - 大量ファイル生成・削除の後でもインデックス整合性維持
- 耐障害（将来）
  - 更新途中のクラッシュからの回復（順序保証で影響最小化）

## 移行・互換

- 後方互換なし。新規に初期化されたイメージのみサポート。
- 既存イメージからの自動移行ツールは本計画の対象外（必要なら別途ユーティリティとして検討）。

## 段階的導入手順（実装タスク）

1) メタ構造追加: superblock 拡張、HR エントリ表/インデックスの領域確保
2) HRL API 実装: lookup/put/inc/dec とインデックス（Robin Hood or 線形探索）
3) `kafs_ino_*` を HRID ベースへ置換（読み/書き/解放）
4) 全ゼロブロックの特別扱い実装
5) マルチスレッド対応のロック（簡易版）
6) 単体/機能テスト追加
7) パフォーマンス計測とパラメータ調整（バケット数、ロードファクタ等）

---

質問/レビュー歓迎。実装中は feature フラグで既存モードと切替可能にし、早期検証を回しながら仕上げる方針。
