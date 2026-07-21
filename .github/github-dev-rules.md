# GitHub 開発ルール

最終更新: 2026-07-16

## ブランチ運用

- 変更は必ずブランチで行う。
- ブランチ名は目的が分かる短い名前にする。

## コミット

- コミットは小さく、目的が明確になるように分割する。
- メッセージは "<type>: <summary>" を推奨する。
  - 例: docs: add hotplug milestones

### レビュー範囲と WIP コミット

- 実装中のレビュー単位は file または hunk とし、レビュー対象を明示する。
- file/hunk のレビューが完了したら、その範囲だけを stage し、
  `WIP: review <scope>` コミットを作る。コミット本文の
  `Reviewed-scope:` に file path と、必要なら function/section/hunk の説明を記録する。
- WIP コミットには未レビューの変更を混ぜない。`git diff --cached` でレビュー済み範囲だけで
  あることを確認し、関連する最小テスト結果があれば `Validation:` に記録する。
- ひとつの論理的な確定コミット単位が完成したら、WIP コミットを
  squash/fixup、amend、または interactive rebase で整理し、通常の
  `<type>: <summary>` コミットにする。PR-ready history や確定コミット列に `WIP:` を残さない。
- 履歴の整理後は、確定コミットの diff と関連テストを再確認する。
- rebase/amend などの履歴書き換えは未共有の作業ブランチに限定する。共有済み・公開済みの
  commit を書き換える場合は、対象と影響を明示して事前承認を得る。

## プルリクエスト

- PR には目的、変更点、影響範囲、テスト結果を記載する。
- 破壊的変更は明示し、移行手順が必要なら追記する。

## マージ方針

- リベースマージとスカッシュマージは基本的に行わない。
- 優先順位は FF マージ、次に通常マージとする。
- 例外的にリベースマージまたはスカッシュマージを使ってよいが、その場合は残った作業ブランチを早めに整理することを条件とする。
- 上記は PR のマージ方式に関する方針であり、レビュー済み WIP を PR-ready commit に整理する
  ローカルの squash/fixup、amend、rebase は対象外とする。

## レビュー

- レビュー指摘がある場合は修正か理由を記載する。
- 重大な変更は、実装前にprimary agent自身によるTask Start Gateの`PASS`を必須とする。
- subagent利用が明示的に許可されている場合は、Gatekeeperによる独立した開始・終了判定も行う。
  Gatekeeperを利用できないことはprimary agentの開始判定を省略する理由にならない。

## CI/テスト

- 変更に関連するテストを実行する。
- 失敗したテストは修正または理由を明記する。

## Issue/タスク

- 実装はチケットと対応付ける。
- マイルストーンと整合する範囲で進める。
- handoff、backlog、ticketの「次」は実装候補であり、開始許可または最新の完了条件とは扱わない。
- 非自明な実装・refactorは、`AGENTS.md`のTask Start Gateでcurrent checkoutのevidence、前提、
  状態・不変条件、再定義した終了条件を確認し、`PASS`となってから開始する。
- ticketとの対応やmilestoneとの整合だけではTask Start Gateの代替にならない。
- 次waveの名前、推奨、優先順位を示す前に、`AGENTS.md`のGoal And Critical Path Gateと
  `docs/pert-task-selection.md`に従ってcapability-level PERTを作成する。候補の提案・了承後にPERTを
  後付けしてはならない。
- 優先順位はPERTのcritical path、slack、runnable critical frontierから決定する。file、静的解析件数、
  既存ticket順、直前waveの形、差分の小ささ、着手容易性を優先順位にしない。
- 各wave終了時にPERTをcurrent evidenceから再構築し、critical frontierを提示してから次waveを提案する。
  external blockerはgraphから除外せず、blocker解消またはcritical path短縮に寄与しない局所改善を
  次waveへ機械的に継続しない。
- `git blame`と履歴は設計意図・制約・変更文脈を確認するBlameCheckに限定し、責任転嫁、優先度低下、
  対応除外には使用しない。
- 完了済みRCAの対策実行では、accepted product goalを入力として維持し、goal再選択や包括的なcurrent
  capability棚卸しを対策開始の前提にしない。RCA対策と同根影響を先に収集し、goalまでの距離と
  correctness/durability制約からwaveを統合・分割・廃止・追加する。必要なRCA対策を閉じた後に、
  current capabilityの包括的な再baselineと後続product計画を行う。

## ラベル運用 (推奨)

- type: docs/feat/fix/refactor
- scope: front/back/rpc/kafsctl
- status: in-progress/blocked/ready-for-review

## リリース

- リリース前に変更点と互換性の影響を確認する。
- リスクがある場合は明示する。
