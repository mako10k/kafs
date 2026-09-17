# KAFS v7 small-operation global-work audit — 2026-09-17

Scope: the v7 FUSE request path and runtime statistics in
`fix/arm32-portability` at `159af1097cfeb7e2d807a6919f9a24e3dc2f63b7`,
with the local `fsstat` worktree diff. No Moxa/card mutation or function-level
Moxa profile was performed for this audit. The prior interrupted KINGMAX run
and controlled file-image append remain the incident evidence, not a new test.

## Evidence ledger

| ID | Observation and source | Status | Consequence / discriminator |
| --- | --- | --- | --- |
| E1 | The prior same-host, same-source 64 KiB append used 30,179 `pread64` on a 2 GB/16-group image versus 4,821 on a 128 MB/2-group image; sync counts were 169 versus 154. Both images passed subsequent fsck. | CONFIRMED for those two runs | Geometry-sensitive read work is present; a per-function trace/profile is needed to allocate its cost. |
| E2 | Runtime transaction, sequence and checkpoint paths call `kafs_v7_validate_image_fd` before/after small mutations (`src/kafs_v7_runtime_transaction.c:205`, `:217`, `:531`; `src/kafs_v7_sequence.c:129`; `src/kafs_v7_checkpoint.c:138`, `:227`, `:329`). The validator checks inode references and namespace across groups (`src/kafs_v7_layout.c:2643`, `:2645`). | CONFIRMED call path; INFERRED latency contribution | Repeated image-wide validation is the strongest evidenced geometry-dependent mechanism; exact wall-time share remains UNKNOWN. |
| E3 | `kafs_v7_journal_analyze_fd` iterates every group and segment (`src/kafs_v7_journal.c:738`). It is nested in validation and separately called by COW reachability and journal writing (`src/kafs_v7_runtime_transaction.c:412`, `:536`; `src/kafs_v7_journal_writer.c:170`). | CONFIRMED | This is all-group journal work, not a second independent full inode scan. Profile separately to avoid double-counting E2. |
| E4 | Block retirement proves non-reference by scanning all inode and HRL shards (`src/kafs_v7_runtime_transaction.c:946`), and FUSE retires retained blocks one at a time (`src/kafs_v7_fuse_write.c:812`). | CONFIRMED path; UNKNOWN incident share | Overwrite/truncate with many retired blocks can repeat image-wide proof and closeout per block. Compare retirement-call count and time. |
| E5 | COW reachability recursively walks the modified inode's indirect-reference tree until expected old/new references are found (`src/kafs_v7_runtime_transaction.c:397`, `:619`). | CONFIRMED path; INFERRED size scaling | A tail reference can require visiting earlier file references; this is file-wide, not image-wide. Compare append at increasing file offsets while holding image geometry fixed. |
| E6 | The allocator loads and validates a group bitmap/summary once per batch, then rebuilds the group summary for each planned block (`src/kafs_v7_data_cow.c:181`, `:244`, `:312`). | CONFIRMED | Group-wide work; its cost relative to E2 is UNKNOWN. Profile batch size and group geometry separately. |
| E7 | `fsstat -v` walks every inode and HRL entry on each request (`src/kafs_shared_fuse_runtime.c:7799`); default `fsstat` skips these scans. The v7 inode accessor maps v7 shards, so a legacy inode-table crash is not established. | CONFIRMED path; UNKNOWN incident contribution | `watch -n 2 ... -v` can add observer load; compare polling enabled/disabled before attributing card latency to it. |
| E8 | Journal, checkpoint and COW code contain multiple `fdatasync` barriers (`src/kafs_v7_journal_writer.c:570`, `:574`; `src/kafs_v7_checkpoint.c:152`, `:163`; `src/kafs_v7_data_cow.c:451`). | CONFIRMED calls; UNKNOWN media cost | These are whole-file persistence barriers, not full reads. Similar sync counts in E1 do not explain its geometry-dependent read difference by themselves. |

Lower-priority bounded scans: file creation searches the parent inode shard
from its beginning (`src/kafs_v7_fuse_write.c:1565`), and a direct directory
update copies at most twelve blocks (`:1667`). Neither is a confirmed cause of
the observed real-card latency. Mount admission and offline fsck deliberately
perform full validation; the avoidable concern is repeated full work on small
runtime operations.

The defect-producing performance condition supported by current evidence is
repeated full-layout validation on small v7 transactions. All-group journal
analysis, per-block retirement proof, file-tree reachability, group-summary
rebuilds, and sync barriers are distinct possible contributors. The real-card
workload and 1 KiB block size exposed the cost; their individual shares and
the effect of verbose polling are UNKNOWN. Small-image tests lacking a
real-size latency gate explain possible late detection, not the producing
condition. No global check is removed by this audit.

## Alternatives assessed (not implemented)

| Candidate | Repeated work it could remove | Required invariant / cost / limitation |
| --- | --- | --- |
| Transaction-local validation after one full admission/recovery scan | Repeated image-wide E2 scans | Start from a fully validated state; prove every mutation changes only declared records and preserves all bitmap, reference, namespace, checksum, and sequence invariants. Re-run the full check on recovery or detected external change. This is the largest potential geometry-dependent gain and the largest proof/qualification burden. An unguarded validator bypass is not equivalent. |
| Generation-coherent journal/group state | E3 all-group journal parsing for one-group mutations | Cache the selected descriptor, checkpoint, visible sequence and per-group journal state; invalidate on publication failure, recovery, unexpected generation, or external medium change. Cross-group ordering and crash replay must remain equivalent. A cache without invalidation is unsafe. |
| Batch retirement of a set of old blocks | E4 repeated full unreferenced scans and closeouts per block | Check all candidate blocks in one all-inode/HRL pass and commit one group-consistent retirement batch. This retains the existing global proof but amortizes it; it helps overwrite/truncate paths, not pure new-block append. Retained/shared references and partial failures need explicit handling. |
| Exact-path COW reachability | E5 walk of the target file's preceding reference tree | Pass each planned file index and verify before/after references along its direct/indirect path, including newly staged index blocks. Prove no alias or unreferenced staged block can pass. This targets large-file tail appends but needs an interface change between FUSE and transaction layers. |
| Incremental group allocator summary | E6 full group-summary rebuild for each planned block | Update the affected bitmap leaf and its summary ancestors, while retaining full summary validation at admission/recovery or another defined checkpoint. Group-local change with lower apparent risk; benefit may be small relative to E2. |
| Lightweight monitoring by default | E7 optional full `fsstat -v` scans | Poll `kafsctl fsstat` without `-v`; request `-v` only for tombstone/HRL detail. This changes no filesystem semantics and does not remove write-path work. |
| Persistence-barrier coalescing | E8 repeated `fdatasync` calls | Only combine barriers where data-before-journal, journal-before-apply, and checkpoint-publication ordering is proven unchanged by crash-injection tests. SD-card flush cost is unmeasured; do not lower the accepted fsync policy merely to improve a benchmark. |

The first design to evaluate for the image-size effect is transaction-local
validation, because E1/E2 distinguish size-dependent reads while sync counts
were similar. That is a proposal, not an accepted replacement: the current
validator also detects unrelated corruption on every mutation. A cheaper
prototype can batch retirement or update the allocator summary incrementally,
but neither should be reported as solving the E2 path without a measured
end-to-end result. Before selecting an implementation, obtain per-category
timing on the 2 GB/1 KiB file image and an owner decision on the intended
workload, latency budget, and whether per-mutation detection of unrelated
corruption is part of the required contract. Then verify the chosen candidate
with crash/recovery and offline fsck comparisons before real-media retesting.
