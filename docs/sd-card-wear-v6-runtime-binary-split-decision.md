# KAFS format v6 runtime binary split decision

Date: 2026-07-01
Status: accepted

## Decision

Future format v6 runtime write work must move behind a separate v6 runtime
entrypoint instead of broadening the production `kafs` mount binary.

The current `kafs` binary remains the production runtime for v4/v5 images. Its
existing v6 inspection and controlled-write paths are retained only as bounded
diagnostic and smoke-test surfaces until they can be retired, hidden, or routed
through the dedicated v6 entrypoint.

Until explicit v6 production cutover, v6 does not carry a backward
compatibility promise. Format and feature changes may be drastic when the v6
target design requires it. This policy does not loosen v4/v5 compatibility.

Runtime binaries are final deliverables only. Shared implementation may be
factored into common object files, static libraries, or future shared libraries
that both runtimes can link. The split is an entrypoint and policy split, not a
request to duplicate filesystem logic or create helper executables.

## Context

The previous v6 runtime path assumed that format v6 could remain inside the
same `kafs` binary while descriptor-backed metadata placement was introduced
under the existing filesystem operations. That is only valid if the v6 work is
mostly a lower-layer routing change and the v4/v5 production path does not gain
experimental policy branches.

The current controlled-write path no longer satisfies that boundary:

- v6 admission has separate mount options and fail-closed rules.
- v6 write support currently allows only a narrow regular-file surface.
- directory mutation, rename, unlink, truncate, chmod/chown/utimens, links,
  copy/reflink, background mutation, hotplug write delegation, and repair write
  are still rejected or out of scope.
- v6 descriptor-backed inode, bitmap, allocator, HRL, and journal routing are
  not just a transparent v5 metadata placement extension.
- keeping the experiment inside `kafs` makes the production binary carry
  v6-specific gates, wording, and policy decisions before v6 has v5-level
  user-facing behavior.

That mismatch made the roadmap look like production cutover work while basic
v5-visible filesystem behavior was still absent from the v6 runtime surface.

## Consequences

- Do not add new user-facing v6 write operations to the production `kafs`
  binary as the next step.
- Introduce a dedicated v6 runtime binary or front-end before expanding v6
  write semantics beyond the existing controlled-write smoke.
- Keep common code shared through libraries or common Automake object lists.
  Candidate shared areas include descriptor parsing and validation, inode
  helpers, block allocation helpers, journal helpers, fsck/dump shared readers,
  and common FUSE operation utilities where the policy boundary is explicit.
- The acceptable shared artifacts are implementation artifacts such as `.o`,
  `.a`, and `.so`, not additional user-facing binaries.
- Treat v5 parity as a v6 runtime goal, but validate it through the dedicated
  v6 entrypoint once that entrypoint exists.
- Keep `kafsresize --migrate-create --format-version 6`, `kafsdump`, and
  `fsck.kafs` behavior as offline/staging tooling unless a later decision
  explicitly changes those tool boundaries.

## Next Implementation Boundary

The next implementation slice is not `mkdir` or broader v6 write-surface
expansion. It is the split plan:

1. define the v6 runtime binary name and CLI contract;
2. identify which current `kafs` v6 admission code moves to that entrypoint;
3. define shared `.o` / `.a` / `.so` boundaries in `Makefile.am`;
4. add a narrow smoke that proves v4/v5 `kafs` behavior is unchanged while the
   v6 entrypoint owns v6 runtime admission.
