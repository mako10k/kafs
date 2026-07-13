# KAFS format v7 inception deck

Date: 2026-07-07
Accepted: 2026-07-13
Status: accepted

## Purpose

This deck fixes the accepted decision frame for the format v7 raw image layout.
Format v7 exists because the v6 work
proved useful descriptor-backed entrypoint and validation boundaries, but also
made clear that a long-lived raw layout should not grow by preserving
experimental v6 shape or v5 prefix assumptions.

The accepted raw layout specification is
[sd-card-wear-format-v7-raw-layout.md](sd-card-wear-format-v7-raw-layout.md).
`mkfs.kafs --format-version 7`, `fsck.kafs`, `kafsdump`, and `kafs-v7` must be
implemented against its v7-owned descriptor version 2 records, not against the
pre-specification version 1 scaffold.

## Why Are We Here?

KAFS needs an SD-card-friendly image layout that reduces metadata hot spots
without making recovery and offline validation ambiguous.  v4/v5 production
images keep compatibility and operational simplicity, but their prefix-oriented
metadata model pulls later work toward contiguous bitmap, inode table, journal,
and HRL assumptions.  v6 demonstrated descriptor-backed discovery and runtime
admission as an experiment.  v7 is the point where the raw layout can be chosen
as a breaking change.

The decision is not only "descriptor or no descriptor".  Different priorities
produce different layouts:

- prioritizing compatibility above recovery pulls the layout toward v5 prefix
  geometry;
- prioritizing implementation containment above wear distribution pulls the
  layout toward a small descriptor wrapper around existing records;
- prioritizing write distribution above compatibility pulls the layout toward
  grouped or sharded metadata away from a single hot prefix;
- prioritizing recovery above small-image efficiency pulls the layout toward a
  small deterministic anchor, replicated descriptors, generation counters,
  checksums, and fsck-first validation.

## Product Boundary

In scope:

- choose the format v7 raw image layout before expanding v7 mkfs/runtime work;
- make breaking raw-layout changes when they serve the v7 goals;
- keep v7 entrypoints and source ownership separate from v5-and-earlier and
  frozen experimental v6 resources;
- define what offline tools must prove before successful runtime admission.

Out of scope:

- preserving experimental v6 raw-layout behavior as a compatibility contract;
- successful v7 runtime paths through production `kafs`;
- v5 prefix fallback inside a successful v7 path;
- in-place metadata relocation for existing images;
- expanding the write surface before fsck and dump validation understand the
  final v7 raw layout.

## Stakeholders

| Stakeholder | Need |
| --- | --- |
| Operators | Predictable tools, clear rejection messages, and offline migration path. |
| `mkfs.kafs` | A deterministic layout builder that is not inferred from v6 scaffolding. |
| `fsck.kafs` | A single authoritative way to discover, validate, and report v7 metadata. |
| `kafsdump` | Observable layout reports for descriptor replicas, shards, and coverage. |
| `kafs-v7` | Fail-closed admission unless the selected v7 descriptor and shard coverage are valid. |
| Maintainers | Format-specific ownership boundaries that prevent v5/v6 compatibility shortcuts. |
| SD-card wear work | Metadata placement that avoids concentrating repeated writes in one prefix. |
| Journal/recovery users | Journal header and data placement that avoids one hot segment and remains replayable after torn writes. |
| HRL users | HRL index and entry placement that avoids a single hot metadata region while remaining fsck-verifiable. |

## Decision Model

The deck separates gates from weights:

- gates are required properties; a candidate that fails a gate is not a v7 raw
  layout candidate;
- weights compare candidates that already pass the gates;
- the coverage matrix is a decision checklist, not a priority ranking.

## Admission Gates

| Gate | Required result |
| --- | --- |
| v7 ownership | Successful v7 mkfs, validation, and runtime admission use v7-owned records and entrypoints, not v5/v6 fallback. |
| fsck-first admission | `fsck.kafs` and `kafsdump` can discover, validate, and report the selected v7 layout before `kafs-v7` admits it. |
| metadata coverage | Every metadata region in the coverage matrix is placed, retired, or fail-closed before any successful write path can use it. |
| recovery floor | Descriptor, checkpoint, journal, and mutable metadata updates have enough generation/checksum/coverage information for fsck to recover or reject torn writes deterministically. |
| offline migration | Compatibility is provided by offline migration/rebuild, not by making the v7 raw layout imitate v5/v6. |

## Decision Weights

Use these scores only after the gates above pass.  Higher score wins; ties are
resolved by the table order.

| Score | Axis | Raw layout pressure |
| ---: | --- | --- |
| 5 | Recoverability and fsck determinism | Prefer layouts fsck can validate, recover, or reject deterministically, even if they reserve more metadata. |
| 5 | SD-card wear distribution | Prefer metadata groups, shard-local allocation state, and journal placement that avoid one hot prefix. |
| 4 | Journal locality and replay safety | Treat journal header/data segments as first-class layout records with generation, checksum, segment pairing, and replay invariants. |
| 4 | Core mutation routing | Bitmap, inode, and allocator summary updates must route to exactly one descriptor-owned shard and be fsck-checkable. |
| 4 | HRL locality and durability | Treat HRL index and entries as first-class mutable metadata with explicit shard placement, coverage, and recovery invariants. |
| 4 | Root/checkpoint stability | Keep the bootstrapping anchor small, deterministic, and recoverable without making it the main mutable hot spot. |
| 3 | Tool observability | Prefer explicit descriptor records that `kafsdump` can report without reconstructing hidden geometry. |
| 3 | Runtime hot-path simplicity | Prefer a selected descriptor cached in the runtime context and stable shard maps after admission. |
| 2 | Implementation containment | Permit staged delivery, but only if the staging shape is already v7-owned. |
| 2 | Small-image overhead | Keep overhead bounded, but do not let tiny-image efficiency force a global hot prefix. |
| 1 | Legacy raw-layout compatibility | Compatibility belongs to offline migration, not v7 runtime admission. |

Recoverability/fault tolerance and SD-card wear distribution are joint highest
priorities.  Wear distribution cannot weaken the recovery-floor gate: when a
placement cannot be recovered or rejected deterministically after a torn write,
it is not an admissible wear-leveling design.

## Tradeoff Rules

Use these rules when layout quality and implementation cost conflict:

1. If a simple implementation would make fsck recovery ambiguous, reject that
   layout shape.
2. If a layout improves wear distribution but fsck cannot validate its coverage
   and update ordering, keep the idea but require more descriptor/recovery
   structure before accepting it.
3. If a v7 feature is too large for the first implementation slice, encode the
   final v7-owned shape and temporarily fail closed for unsupported paths rather
   than reusing a v5/v6 prefix path.
4. If small-image support conflicts with descriptor replicas, journal safety, or
   metadata-group invariants, let `mkfs.kafs` reject too-small v7 images instead
   of weakening the layout.
5. If runtime simplicity conflicts with the raw layout, prefer admission-time
   validation and cached shard maps over collapsing the layout back to a
   contiguous prefix.
6. If existing v5/v6 record shapes are reused for implementation containment,
   the v7 spec must say which parts are deliberately adopted.  Anything not
   adopted remains scaffold, not the v7 contract.
7. If observability requires extra descriptor fields or reportable IDs, prefer
   explicit reporting unless it would violate the recovery or wear goals above.

## Layout Implications

| If this is weighted higher | The format tends toward | Risk if over-weighted |
| --- | --- | --- |
| Compatibility | v5-like prefix metadata with a descriptor veneer | Reintroduces the constraints that justified v7. |
| Implementation speed | Single metadata group and existing record shapes | May accidentally preserve v6/v5 assumptions as the public layout. |
| Wear distribution | Multiple descriptor-described metadata groups and sharded metadata | More fsck and migration complexity. |
| Journal replay safety | Paired journal header/data shards with explicit segment ids, generations, checksums, and group ownership | More update protocol and recovery tests. |
| HRL durability | Descriptor-owned HRL index and entry shards, with bucket and entry-id coverage independent of v5 prefix offsets | More descriptor records and stricter admission checks. |
| Core mutation correctness | Descriptor-owned bitmap, inode, and allocator summary shards with exact logical coverage and alignment rules | More admission checks before the runtime can write. |
| Recoverability | Immutable or rarely written anchor plus replicated descriptors | More reserved space and update protocol work. |
| Runtime performance | Precomputed shard maps and simple descriptor-selected offsets | More strict admission requirements before mount. |
| Observability | Verbose descriptor records and explicit coverage maps | Larger metadata footprint. |

## Observed Carry-Over Constraints

These are the constraints that v7 should either reject explicitly or carry only
after an explicit v7 decision:

- v4/v5 prefix metadata makes the superblock the owner of contiguous bitmap,
  inode table, HRL, allocator, and journal geometry.
- Superblock/checkpoint data is both a bootstrap dependency and a write
  concentration risk.  v7 separates replicated immutable root locators from
  authoritative rotated mutable checkpoint replicas; mutable free counts do not
  remain authoritative at offset 0.
- Journal header/data placement is not solved by having a descriptor.  The raw
  layout must define segment pairing, generation ordering, checksums, and replay
  selection.
- HRL is not a secondary reporting concern.  It is mutable metadata whose index
  and entry regions can become hot and whose bucket/entry coverage must be
  validated before runtime admission.
- Bitmap, inode table, and allocator summary are the core mutation path.  v7
  must define exact logical coverage, alignment rules, and rebuild/validation
  behavior before write admission.
- Pending log and tail metadata cannot remain hidden v5-era write paths.  v7
  must either define descriptor-owned placement for them or explicitly disable
  and fail closed for paths that would use them.
- v4/v5 journal and metadata-delta helpers assume prefix-oriented or contiguous
  metadata regions.
- v6 descriptor discovery proved that bitmap, inode, allocator, HRL, and
  journal regions can be validated by descriptor coverage instead of v5 prefix
  offsets.
- v6 also reused several existing record shapes and scaffold names while
  proving the runtime boundary.  Those names and shapes are not automatically
  v7 raw-layout decisions.
- Production `kafs` must remain a v4/v5 runtime.  v7 admission belongs to
  `kafs-v7` after v7 offline validation succeeds.

## Coverage Matrix

The coverage matrix remains the conformance checklist for every implementation.
The accepted raw layout makes an explicit decision for every region below.
"Not implemented in the first slice" is acceptable only when the successful v7
runtime path fails closed before using that region.

| Region | Class | Accepted v7 contract |
| --- | --- | --- |
| `superblock_checkpoint` | discovery/recovery gate | Immutable primary/tail `K7SA` roots plus 2-or-3 rotated `K7CP` replicas; legacy primary mutable fields are non-authoritative. |
| `layout_descriptor` | discovery/recovery gate | Deterministic primary/tail/optional-midpoint `K7LD` replicas, generation/CRC validation, and byte-identical same-generation selection. |
| `block_bitmap` | write-admission gate | Exact logical coverage with little-endian 64-bit words and paired allocator-summary validation. |
| `inode_table` | write-admission gate | Exact `[0, s_inocnt)` coverage and a packed 128-byte v7 inode with logical-block-plus-one references. |
| `allocator_summary` | write-admission gate | One L1/L2 summary per bitmap shard, exactly rebuildable from the authoritative bitmap. |
| `hrl_index` | write-admission gate | Packed 4-byte bucket heads with exact coverage and group-local chain ownership. |
| `hrl_entries` | write-admission gate | Packed 24-byte entries, exact entry-id coverage, bounded acyclic chains, and group-local data references. |
| `journal_header` | recovery gate | At least two segment ids with rotating 64-byte `K7JH` slots and independently validated data prefixes. |
| `journal_data` | recovery gate | Structured checksummed transactions merged across all valid segments by filesystem-global no-wrap sequence. |
| `pending_log` | explicit policy | Shard absent; every dependent path fails before mutation. |
| `tail_metadata` | explicit policy | Shard and inode tail payload absent/zero; packing, normalization, reclaim, and GC fail before mutation. |

`unknown` remains a counter bucket, not an on-disk v7 region.

## Non-Metadata Raw Layout Decisions

The accepted v7 raw layout also fixes the following non-metadata shape; mkfs
conformance must validate it before generating an admissible image:

| Area | Accepted v7 contract |
| --- | --- |
| Data block address space | Group-local linear logical-to-physical mapping with contiguous data inside each group and exact non-overlap reservations. |
| Feature/version flags | Six required incompat bits; missing required or unknown incompat bits fail closed. |
| Byte order and alignment | Packed little-endian records, exact sizes/padding, block alignment, checked arithmetic, and one common CRC-32 algorithm. |
| Update atomicity | Byte-identical descriptor/checkpoint replication plus write-ahead structured journal transactions; ambiguity fails closed. |
| Small-image policy | At least two descriptor/checkpoint replicas, two journal segments, one group, and one usable data block; otherwise mkfs refuses the image. |

## Candidate Layout Families

### A. Descriptor-rooted grouped metadata

This is the accepted baseline.  The image has deterministic primary and tail
v7 root locators, at least primary and tail descriptor/checkpoint replicas, and
descriptor-owned metadata groups.
Bitmap, inode, allocator summary, HRL, and journal regions are resolved through
v7-owned descriptor records, with pending log and tail metadata absent and
explicitly fail-closed.  The initial implementation may choose
one group for containment only if the raw layout encodes it as a v7 metadata
group, not as a v5 prefix compatibility mode.

HRL remains visible inside this family: the raw layout describes HRL
index and entry shards explicitly, including bucket coverage, entry-id coverage,
and the recovery checks needed before `kafs-v7` admission.

Journal and core mutation paths remain equally visible: the raw layout
describes journal header/data segments, bitmap shards, inode shards, and
allocator summary shards with the same fail-closed coverage discipline used for
HRL.

Expected consequences:

- best alignment with the decision model;
- fsck can validate descriptor replicas and group coverage before runtime
  admission;
- later wear-distribution work can add placement policy without changing the
  entrypoint boundary;
- the single-group offline slice proves the placement/recovery foundation but
  does not claim that wear leveling is complete;
- implementation must avoid treating the current v6 scaffold as the public v7
  layout.

### B. Descriptor veneer over v5 prefix layout

The image mostly keeps v5 geometry and adds descriptor records for discovery.
This is attractive for quick implementation, but it keeps the hot-prefix and
contiguous-table pressures that v7 is meant to escape.

Expected consequences:

- fastest path to a nominal v7 mkfs;
- poorest fit for the breaking-change rationale;
- high risk of future compatibility-driven code growth.

### C. Segment-first log layout

The image is organized around log or segment allocation from the start.  This
could give a stronger wear and crash model, but it is a larger change than the
current descriptor-backed work has proven.

Expected consequences:

- strongest break from v5 prefix metadata;
- largest fsck/runtime implementation leap;
- likely too broad unless the project chooses to pause descriptor-group work.

### D. Staged v7 group layout

The raw layout is the descriptor-rooted grouped layout, but the first mkfs/fsck
slice supports only one metadata group and a restricted placement policy.  This
is acceptable only if the on-disk structures, diagnostics, and admission rules
are v7-owned from the beginning.

Expected consequences:

- practical first implementation path;
- preserves the final direction if the single-group restriction is explicit;
- next requires offline mkfs/fsck/kafsdump conformance to the accepted raw-layout
  specification before runtime admission.

## Accepted Decision Order

Use the following decision order unless the project explicitly accepts an
incompatible direction change:

1. reject candidates that fail any admission gate or leave a coverage-matrix
   region undecided;
2. among passing candidates, jointly optimize fault tolerance/fsck determinism
   and wear distribution, never accepting ambiguous recovery to gain placement
   spread;
3. next, prefer designs that make journal replay, core mutation routing, HRL
   durability, and root/checkpoint stability explicit in that order;
4. use observability and runtime simplicity to choose between otherwise similar
   layouts;
5. keep implementation containment as a staging concern, not as a reason to
   inherit v5/v6 raw-layout contracts;
6. keep legacy compatibility low and handle migration offline;
7. accept a staged first implementation only when it is encoded as a strict
   subset of the final v7 raw layout.

Under this decision order, the accepted raw layout uses candidate A, with
candidate D as the first implementation delivery.

## Accepted Raw-Layout Decisions

The raw-layout questions are closed by the accepted specification:

- byte-identical primary and reserved-final-block tail `K7SA` version 2 locators
  remove offset 0 as the only descriptor-discovery root and carry the block size
  needed for independent tail recovery;
- accepted `K7LD` descriptor version 2 requires primary and tail descriptor and
  checkpoint replicas, adding midpoint replicas whenever deterministic
  non-overlap permits them;
- explicit v7-owned 96-byte group and shard records describe metadata physical
  spans, data logical/physical spans, storage class, and exact coverage;
- group-local linear data mapping and fixed little-endian 64-bit bitmap words
  replace platform-dependent or prefix-derived mapping;
- rotated `K7CP` replicas own mutable free-count checkpoints; the primary
  superblock remains immutable identity/discovery state;
- journal header/data placement is group-local while segment sequence and replay
  ordering are filesystem-global; all valid segments are merged rather than
  selecting one highest-generation segment;
- HRL bucket and entry chains must remain group-consistent and fsck-verifiable;
- inode, allocator-summary, journal, and HRL records have self-contained v7
  field offsets, sizes, little-endian semantics, and padding rules; no host or
  v5/v6 wire typedef is normative;
- the journal uses rotated 64-byte `K7JH` slots and structured, checksummed
  begin/mutation/commit-or-abort records with filesystem-global no-wrap
  sequences;
- pending log and tail metadata are absent and fail-closed in descriptor version
  2;
- unknown flags, record types, storage classes, gaps, overlaps, overflow, and
  same-generation divergence fail admission;
- compatibility is an offline rebuild/migration concern, not v7 runtime
  fallback.

Deferred capabilities are not unresolved baseline behavior.  Cross-group HRL,
multi-group transaction atomicity, pending/tail metadata, and non-linear data
mapping each require an explicit incompatible flag or later format version plus
their own mapping, recovery, and fsck proof.  Until then, affected runtime paths
fail before mutation.

## Exit Criteria

The decision-model exit criterion is met: the accepted specification lets
`mkfs.kafs`, `fsck.kafs`, `kafsdump`, and `kafs-v7` list the exact v7-owned
records they must produce, validate, report, or admit, including root locators,
checkpoint and descriptor replicas, bitmap, inode, allocator summary, HRL
index/entries, journal header/data, and explicit pending/tail rejection.

Implementation conformance remains gated separately.  The current pre-spec
scaffold is not accepted merely because these documents are accepted; each
successful tool/runtime path must prove descriptor version 2 behavior first.
