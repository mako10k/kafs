# Operator Diagnostics Minimum Slice Plan

This document defines the minimum next slice for operator-facing diagnostics
without immediately changing runtime control paths.

## Problem

Current operator diagnostics are spread across multiple offline tools and manual
workflows:

- `kafs-info` for a compact summary
- `kafsdump` for structured offline detail
- `fsck.kafs` for validation and repair-oriented checks
- ad-hoc scripts for repeated observation

This is workable for developers, but it is not a clean first-line operator UX.

## Goal

Land the smallest next slice that improves operator discoverability and keeps
future implementation risk controlled.

## Options

### Option A: New standalone `kafs-inspect`

Pros:

- clear operator-facing name
- easy to document as the primary offline inspection entrypoint
- avoids overloading existing subcommand trees

Cons:

- adds a new binary and packaging surface
- requires new man page and install plumbing immediately

### Option B: New `kafsctl inspect` offline subcommand

Pros:

- keeps operational tooling under one familiar entrypoint
- reduces binary count growth
- can share usage and help patterns with existing `kafsctl`

Cons:

- extends `kafsctl` beyond its current mixed runtime/path-control role
- changes control surface and should be treated as a design checkpoint

## Recommended Minimum Slice

Do not implement a new control path yet.

Stage the work in two steps:

1. Document the operator workflow and required output shape first.
2. Ask for an explicit design choice between standalone `kafs-inspect` and `kafsctl inspect` before implementation.

## Proposed Output Scope

The first implementation slice should cover only read-only offline inspection:

- superblock version and geometry
- tail metadata region presence and basic validation summary
- inode and HRL aggregate counts
- journal header presence, sequence, and CRC status
- a concise overall health summary with pointers to deeper tools

The minimum slice should not include repair, replay, or runtime RPC changes.

## Recommended Workflow

The operator story should become:

1. Run the inspect command for a compact offline summary.
2. Use `kafsdump --json` when structured detail is needed.
3. Use `fsck.kafs` when validation or repair actions are required.

## Follow-up Work

- choose the command surface through a design checkpoint
- add the first read-only implementation slice
- add a man page and README/docs index links
- consider journal-specific drill-down only after the compact inspect view exists