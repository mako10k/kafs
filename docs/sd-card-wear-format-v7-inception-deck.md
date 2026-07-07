# KAFS format v7 inception deck

Date: 2026-07-07
Status: draft

## Purpose

This deck fixes the decision frame for the format v7 raw image layout before
the layout is specified or implemented.  Format v7 exists because the v6 work
proved useful descriptor-backed entrypoint and validation boundaries, but also
made clear that a long-lived raw layout should not grow by preserving
experimental v6 shape or v5 prefix assumptions.

The next accepted artifact after this deck should be a format v7 raw layout
specification.  `mkfs.kafs --format-version 7`, `fsck.kafs`, `kafsdump`, and
`kafs-v7` should then be implemented against that v7-owned raw layout, not
against an accidental copy of the v6 scaffold.

The current raw layout draft is
[sd-card-wear-format-v7-raw-layout.md](sd-card-wear-format-v7-raw-layout.md).

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
  concentration risk.  v7 must separate the small discovery anchor from mutable
  checkpoint/update state where possible.
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

The raw layout specification must make an explicit decision for every metadata
region below.  "Not implemented in the first slice" is acceptable only when the
successful v7 runtime path fails closed before using that region.

| Region | Class | Required v7 decision |
| --- | --- | --- |
| `superblock_checkpoint` | discovery/recovery gate | Root anchor location, checkpoint replicas, generation/checksum semantics, and which fields may remain mutable. |
| `layout_descriptor` | discovery/recovery gate | Replica placement, descriptor generation, checksum, candidate selection, and descriptor update protocol. |
| `block_bitmap` | write-admission gate | Exact block namespace coverage, shard ownership, update alignment, and bitmap/allocator consistency checks. |
| `inode_table` | write-admission gate | Exact inode number coverage, root inode coverage, record size, and whether v7 keeps or replaces the current inode record shape. |
| `allocator_summary` | write-admission gate | Relationship to bitmap/data groups, rebuild source, L1/L2 or v7-native summary shape, and stale-summary recovery. |
| `hrl_index` | write-admission gate | Bucket coverage, group ownership, chain head lookup, and fsck validation. |
| `hrl_entries` | write-admission gate | Entry-id coverage, record size/alignment, chain bounds, loop detection, and group consistency with index shards. |
| `journal_header` | recovery gate | Segment id coverage, header record shape, generation/checksum, and pairing with journal data. |
| `journal_data` | recovery gate | Segment byte-span coverage, replay scan order, torn-write handling, and selected-segment update rules. |
| `pending_log` | explicit policy | Descriptor-owned placement and recovery rules, or explicit disabled/fail-closed policy for all users. |
| `tail_metadata` | explicit policy | v7-native packing/normalization rules, or explicit retirement/disabled policy for all users. |

`unknown` remains a counter bucket, not an on-disk v7 region.

## Non-Metadata Raw Layout Decisions

The v7 raw layout spec must also decide the following non-metadata shape before
mkfs generation:

| Area | Required v7 decision |
| --- | --- |
| Data block address space | Logical filesystem block to physical span mapping, metadata reservations, group ownership, and whether data extents are contiguous inside each group. |
| Feature/version flags | Which bits are incompatible, read-only-compatible, or debug-only, and how older tools fail closed. |
| Byte order and alignment | Endianness, padding, block alignment, fixed-record alignment, and overflow rules for every v7-owned record. |
| Update atomicity | Which records are copy-update, generation-selected, or rewritten in place, and which torn-write cases fsck must recover or reject. |
| Small-image policy | Minimum viable image size, minimum group count, descriptor overhead bounds, and when mkfs must refuse a too-small v7 image. |

## Candidate Layout Families

### A. Descriptor-rooted grouped metadata

This is the recommended baseline.  The image has a small deterministic v7
anchor, one or more descriptor replicas, and descriptor-owned metadata groups.
Bitmap, inode, allocator summary, HRL, and journal regions are resolved through
v7-owned descriptor records, with pending log and tail metadata either
descriptor-owned or explicitly disabled.  The initial implementation may choose
one group for containment only if the raw layout encodes it as a v7 metadata
group, not as a v5 prefix compatibility mode.

HRL must remain visible inside this family: the raw layout should describe HRL
index and entry shards explicitly, including bucket coverage, entry-id coverage,
and the recovery checks needed before `kafs-v7` admission.

Journal and core mutation paths must remain equally visible: the raw layout
should describe journal header/data segments, bitmap shards, inode shards, and
allocator summary shards with the same fail-closed coverage discipline used for
HRL.

Expected consequences:

- best alignment with the decision model;
- fsck can validate descriptor replicas and group coverage before runtime
  admission;
- later wear-distribution work can add placement policy without changing the
  entrypoint boundary;
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
- still requires the raw layout spec before mkfs generation.

## Working Decision

Use the following decision order unless the project explicitly changes
direction:

1. reject candidates that fail any admission gate or leave a coverage-matrix
   region undecided;
2. among passing candidates, optimize first for recoverability/fsck
   determinism, then wear distribution;
3. next, prefer designs that make journal replay, core mutation routing, HRL
   durability, and root/checkpoint stability explicit in that order;
4. use observability and runtime simplicity to choose between otherwise similar
   layouts;
5. keep implementation containment as a staging concern, not as a reason to
   inherit v5/v6 raw-layout contracts;
6. keep legacy compatibility low and handle migration offline;
7. accept a staged first implementation only when it is encoded as a strict
   subset of the final v7 raw layout.

Under this decision order, the next raw layout spec should start from candidate
A, with candidate D allowed as the first implementation slice.

## Questions To Resolve Before The Raw Layout Spec

- Is the v7 root anchor a reserved superblock field, a separate anchor block,
  or both?
- What is the descriptor replica placement policy for small and large images?
- What is the metadata group placement algorithm, and how many groups are
  created by default?
- How are logical filesystem blocks mapped to physical data spans once metadata
  groups reserve non-data space?
- Are journal regions global descriptor-selected segments, per-group segments,
  or both?
- What journal segment generation/checksum and replay selection rules are part
  of the v7 raw layout rather than inherited from v5/v6 code?
- Are HRL index and entry shards colocated with the data/allocator group they
  describe, separated into their own durability group, or selected by a distinct
  wear-distribution policy?
- What HRL bucket coverage, entry-id coverage, chain integrity, and checksum or
  generation invariants must fsck validate?
- Does v7 keep the existing inode record shape initially, or define a
  v7-native inode record now?
- Are bitmap shard logical starts and physical offsets word-aligned, or does v7
  require byte-granular bitmap mutation logic?
- Is allocator summary still the v3-style L1/L2 model per shard, or a new
  v7-native summary record?
- Are pending log and tail metadata descriptor-backed v7 features, disabled
  runtime features, or retired from the v7 write surface?
- What v7 feature flags, byte order, record alignment, and overflow rules are
  part of the raw layout contract?
- What exact coverage invariants must fsck validate before `kafs-v7` can admit
  an image?
- Is v5-to-v7 migration limited to offline rebuild through
  `kafsresize --migrate-create --format-version 7`?

## Exit Criteria

The inception deck is complete enough when maintainers can choose the layout
family and decision order without reading implementation code.  The v7 raw
layout spec is complete enough when `mkfs.kafs`, `fsck.kafs`, `kafsdump`, and
`kafs-v7` can each list the exact v7-owned records they must produce, validate,
report, or admit, including superblock/checkpoint, descriptor replicas, bitmap,
inode, allocator summary, HRL index/entries, journal header/data, pending log,
and tail metadata decisions.
