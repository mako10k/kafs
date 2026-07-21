# PERT task-selection record

Use this record before naming, recommending, accepting, or assigning priority
to a new implementation wave. It separates task selection from the later Task
Start Gate: PERT chooses the capability wave; the Task Start Gate determines
whether that selected wave has a coherent and safe implementation boundary.

Do not begin with a preferred task and draw a graph around it. Begin with the
accepted end goal and current capabilities, enumerate credible paths, calculate
the critical frontier, and only then name the selected wave.

## Record identity

- Record ID and time:
- Current branch and HEAD:
- Worktree state:
- Evidence freshness:
- Accepted end goal:
- Current capability position:
- Goal, evidence, dependency, estimate, or blocker changes since the prior
  record:

## Capability nodes

Use one duration unit consistently within the record. Estimates describe
implementation duration under the stated resource assumption, not confidence
scores. Keep external calendar wait as a named blocker; do not hide it inside an
implementation estimate. State the resource assumption when runnable nodes
cannot actually proceed in parallel.

`TE = (O + 4M + P) / 6`

| ID | Capability or mandatory outcome | Causal predecessors | O | M | P | TE | Confidence | State/blocker | Downstream unlock |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| G0 | Accepted end goal |  |  |  |  |  |  |  |  |

Node rules:

- Describe an externally meaningful capability, a required invariant, or a
  mandatory correctness/durability outcome.
- Do not create nodes from file boundaries, functions, tickets, individual
  fixtures, finding counts, or convenient diff sizes.
- Add a correctness finding as a predecessor only when current evidence states
  the causal edge to the protected downstream capability.
- Record unknown dependencies or estimates as `UNKNOWN`; do not silently assign
  favorable values. If uncertainty can change path selection, include the path
  as near-critical.

## Network

Draw the directed predecessor network. Mermaid is preferred when the renderer is
available; an ASCII network is acceptable.

```mermaid
flowchart LR
  A[Current capability] --> B[Capability prerequisite]
  B --> G[Accepted end goal]
```

## PERT calculation

| ID | ES | EF | LS | LF | Slack | Runnable now | Critical or near-critical |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
|  |  |  |  |  |  |  |  |

- Critical path or paths:
- Uncertainty-overlapping near-critical paths:
- External blockers retained in the network:
- Runnable zero-slack or least-slack frontier:

If exact timing is too uncertain for a defensible backward pass, publish the
topological critical paths with `UNKNOWN` timing and a provisional frontier.
Do not replace missing estimates with implementation ease.

## Ordering comparison

| Ordering | Critical-path effect | Capability unlocked | Future rework or qualification debt | Evidence and uncertainty |
| --- | --- | --- | --- | --- |
|  |  |  |  |  |

Explicitly answer:

1. Was any candidate proposed or accepted before this network was built? If yes,
   discard that preference and rebuild the comparison.
2. Is the preferred node merely adjacent to the last changed code or tests?
3. Is a real safety finding being treated as automatically highest priority
   without a causal critical-path edge?
4. Is a complex capability being penalized for effort while its downstream
   unlock is omitted or described at lower resolution?
5. If the critical node is blocked, does the fallback reduce the blocker or
   critical-path duration, or is it only locally easy?
6. Does the ordering add work that must be repeated in qualification, migration,
   or recovery after a later capability expansion?

## Selection

- Selected runnable capability node:
- Why it is on the critical or least-slack frontier:
- Predecessor closed by this wave:
- Downstream nodes unlocked:
- Opportunity cost versus the strongest alternative:
- PERT decision: `SELECT`, `REPLAN`, or `BLOCKED`:

Only after `SELECT` may a Task Start Record derive implementation scope, exit
criteria, and non-goals. User agreement with a pre-PERT proposal is not selection
evidence.

## Closeout and refresh

At wave closeout:

1. verify whether the selected capability edge actually closed;
2. update actual effort and any estimate error that changes remaining paths;
3. refresh current state and external blockers;
4. rebuild ES/EF/LS/LF/slack and the runnable frontier; and
5. publish the new PERT record before mentioning the next task.

Rebuild rather than amend around a preferred candidate when the accepted goal,
HEAD, relevant evidence, dependencies, estimates, or blocker state changes.

## Required effectiveness replay

The post-T52 v7 state is the regression case for this control. M7 real-media
qualification was externally blocked, M8-C indirect mutation remained an
explicit capability gap, and a tentative T53 bounded directory-graph validation
candidate was adjacent to the completed direct validators. A valid pre-proposal
PERT record must not select that directory task merely because it is coherent,
safe, or locally easy. It must retain the M7 blocker, model M8-C and other
credible goal paths at equal resolution, and show a causal zero/least-slack edge
before any wave can be selected. Without that evidence the decision is
`REPLAN`, not `SELECT`.
