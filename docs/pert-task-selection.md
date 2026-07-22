# `perttool` task-selection gate

Use this gate before naming, recommending, accepting, or assigning priority to
a new implementation wave. `perttool` selects the capability wave; the later
Task Start Gate decides whether that wave has a coherent and safe implementation
boundary.

Do not begin with a preferred task and draw a graph around it. Begin with the
accepted end goal and current capabilities, encode the residual network, run the
tool, and only then name a selected wave.

## Normative artifacts

- `plans/current.pert` is the repository-default, current accepted-goal plan.
- A scoped `.pert` plan may replace it only when the selection record names the
  file and explains why the accepted goal differs.
- `./scripts/pert-next-task.sh [plan.pert]` is the standard read-only entrypoint.
- Markdown tables, Mermaid diagrams, tickets, and handoffs are explanatory or
  ownership records. They are not calculation authorities.

The `.pert` input and the exact `perttool` output form the selection evidence.
If prose conflicts with the tool, the decision is `REPLAN`; do not silently
choose whichever result supports a preferred task.

## Mandatory sequence

Before mentioning a next-task candidate:

1. refresh the accepted goal, current capability frontier, dependencies,
   estimates, resources, task status, and external blockers from current
   evidence;
2. update the applicable `.pert` plan before naming a candidate;
3. run:

   ```sh
   ./scripts/pert-next-task.sh plans/current.pert
   ```

   This must execute the following operations successfully and in order:

   ```sh
   perttool dsl check plans/current.pert
   perttool dag analyze plans/current.pert --schedule both --format text
   perttool dag next plans/current.pert --format text
   ```

4. compare the tool-selected frontier with credible alternative orderings and
   their future rework or qualification debt; and
5. issue `SELECT`, `REPLAN`, or `BLOCKED` from the rules below.

If `perttool` is missing, `dsl check` fails, or evidence shows the plan is stale
or incomplete, the result is `REPLAN`. A task must not be selected from a stale
last-known output.

## Plan modeling rules

- Use one `project` whose `finish` is the accepted end goal. Root milestones
  represent directly observed current capabilities and must be `state reached`.
- Model tasks as capability outcomes or mandatory correctness/durability work,
  not files, functions, tickets, test examples, finding counts, or convenient
  diffs.
- Include every credible causal path to `finish` at comparable resolution.
  Never omit a difficult path or describe its downstream unlock at lower
  resolution than an easy alternative.
- Use PERT estimates for remaining implementation effort. External calendar
  wait is not duration: mark the task `status blocked`, record the exact
  `blocked_reason`, and estimate only the work after the external input becomes
  available. Treat `CONDITIONAL_ON_BLOCKS_RESOLVED true` as a qualifier, not a
  completion forecast.
- Encode actual renewable capacity and task requirements. The default primary
  implementation stream has capacity one unless current evidence supports more
  parallel capacity.
- Do not set `priority` to represent ease, code proximity, age, authorship,
  ticket order, or a task chosen before analysis. A nonzero priority needs an
  evidence-backed product constraint in the companion record.
- A confirmed off-goal finding remains owned in a linked backlog or narrative
  with a disposition. Do not add a false edge to `finish` merely to make
  `perttool` accept it, and do not select it as though it shortened the accepted
  goal. If it becomes part of the accepted goal, rebuild the plan with the real
  causal edge.
- When the remaining work itself cannot be estimated well enough to distinguish
  paths, record the uncertainty and return `REPLAN` or `BLOCKED`; do not invent
  favorable numeric values to obtain a runnable answer.

## Tool-result decision rules

Use `dag analyze` for expected duration, ES/EF/LS/LF, total float, precedence
critical paths, and resource-schedule effects. Use `dag next` for current task
classification.

- `SELECT`: every selected task appears in `RUNNABLE NOW`, is on the zero-slack
  or least-slack frontier, and fits the encoded current resource capacity. When
  capacity permits multiple tasks, the selected wave may contain the
  `RUNNABLE NOW` set justified by the resource schedule.
- `REPLAN`: the graph fails validation, is stale/incomplete, represents
  candidates at unequal capability resolution, relies on unjustified priority
  values, or a proposed task is absent from `RUNNABLE NOW`.
- `BLOCKED`: the valid plan has no runnable critical or least-slack work. State
  the critical `BLOCKED NOW`, resource wait, or unresolved predecessor from the
  tool output. Do not substitute a locally easy off-path task.

`ACTIVE` work remains part of the current allocation. `READY / WAITING
RESOURCE` is not runnable under the encoded capacity. `UPCOMING` has an
unsatisfied causal predecessor. None of these classifications can be promoted
to `SELECT` by prose.

## Companion selection record

The short narrative accompanying the tool output must record:

- record ID/time, branch, HEAD, worktree state, plan path, `perttool --version`,
  and evidence freshness;
- accepted end goal and current capability position;
- changes to goal, evidence, dependency, estimate, resource, or blocker state;
- estimate confidence and external waits excluded from task duration;
- the precedence and resource critical paths reported by `dag analyze`;
- `ACTIVE`, `RUNNABLE NOW`, `BLOCKED NOW`, and relevant `UPCOMING` results from
  `dag next`;
- the capability unlocked by each selected task; and
- comparison with credible alternative orderings, including repeated
  qualification, migration, or recovery work.

Do not recalculate TE, float, or the runnable set in Markdown. Quote or attach
the tool result and interpret it. Only after `SELECT` may a Task Start Record
derive implementation scope, exit criteria, and non-goals.

## Closeout and refresh

At every wave closeout:

1. verify from current evidence whether the selected capability edge closed;
2. update task/milestone state and remaining estimates in the `.pert` plan;
3. refresh resource capacity and external blockers;
4. rerun `dsl check`, `dag analyze --schedule both`, and `dag next`; and
5. publish the new result before mentioning another task.

Rebuild rather than edit around a preferred candidate when the accepted goal,
HEAD, evidence, dependencies, estimates, resources, or blockers change.

## Required effectiveness replay

The post-T52 v7 state remains the regression case for this control. M7
real-media qualification was externally blocked, indirect mutation remained a
goal-path capability gap, and a directory-graph validator was merely adjacent
to completed work. A reconstructed `.pert` plan must model those goal paths at
equal capability resolution and `dag next` must select the runnable indirect
critical predecessor. If that result cannot be obtained without a false edge,
arbitrary priority, or invented estimate, the decision is `REPLAN`, not a
directory-task `SELECT`.

The T59 start replan is also a capability-resolution replay. It must not
hide a missing offline import surface inside one final rehearsal node merely
because a disposable destination scaffold exists. For
`plans/current.pert`, the expected classification is:

- `V5_V7_MIGRATION_REHEARSAL` alone in `RUNNABLE NOW`, total float 0.333 days
  and resource-critical;
- no node in `READY / WAITING RESOURCE`;
- `HARDWARE_APPROVAL` and `VHDX_HOST_RECOVERY_RUN` in `BLOCKED NOW`; and
- physical execution, independent review, and cutover decision in `UPCOMING`.

T59-A closed the lifecycle/evidence predecessor and T59-B closed the v7-owned
offline construction predecessor, so this result requires
`SELECT V5_V7_MIGRATION_REHEARSAL`: it is the only runnable least-slack task.
Selection authorizes a fresh Task Start Gate, not production data access,
runtime mutation expansion, or cutover. The VHDX execution task becomes
eligible only after the user identifies a safe maintenance window and the plan
is refreshed from a new host preflight.
