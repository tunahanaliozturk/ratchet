# 2. Replay, with positions taken at call time

## Context

A workflow is ordinary async Python. To survive a crash it must be able to start again from the top and arrive at the
same place without redoing work that already happened. That needs a way to match "this call, on this run" to "that
recorded outcome, from an earlier run".

## Decision

Every durable call (`ctx.step`, `ctx.sleep`, `ctx.wait_for_signal`, `ctx.now`, `ctx.uuid`, `ctx.side_effect`) takes the
next integer position in the workflow's history. If the history has an outcome at that position, the call returns it.
If not, the call does the work and records the outcome there.

Positions are taken when the call is *made*, not when it is awaited. `ctx.step` is a plain function that takes a
position and returns a coroutine. So in

```python
async with asyncio.TaskGroup() as group:
    a = group.create_task(ctx.step(charge, order))
    b = group.create_task(ctx.step(reserve, order))
```

`charge` is always position n and `reserve` always n+1, whichever finishes first on whichever run.

Each recorded outcome carries the kind of call and the activity name. A replay that finds something else at a position
raises `NonDeterminismError`. ADR 6 covers what happens then.

Results are stored as JSON through the activity's return annotation (a pydantic `TypeAdapter`), and the live run gets
back the same validated JSON a replay would. A function returning a tuple returns a tuple on the first run and on every
replay, rather than a tuple once and a list forever after.

## Consequences

- Workflow code must be deterministic between durable calls. No `datetime.now()`, no `random`, no I/O: those go through
  `ctx.now()`, `ctx.uuid()`, `ctx.side_effect()` or an activity. The README says so next to the first example.
- Every wake replays from the top, so a wake costs time linear in the history. Measured at about 1.3 microseconds per
  recorded step, or 13 ms for 10,000 steps. `RATCHET_MAX_HISTORY` fails a workflow that grows past a limit rather than
  letting it get slower forever.
- Waits take two positions: one for the deadline, fixed the first time the code reaches it, and one for the outcome
  (fired, signal, timeout or cancelled). The deadline is recorded so that a replay a day later computes the same one.

## Alternatives

- **Positions assigned at await time.** Simpler to write, but parallel branches would get positions in completion
  order, which differs between runs. Replay would break exactly when it is needed.
- **Outcomes matched by a key the caller supplies** (`ctx.step("charge-1", ...)`). Survives reordering, but it
  moves the burden of uniqueness to every call site, and a typo replays the wrong result without a word.
- **Snapshotting coroutine state.** Python cannot serialise a suspended coroutine, and pretending otherwise ends in
  pickle.
