# 3. Fencing tokens, and only the database's clock

## Context

A worker holds a lease on a workflow while it runs it. Leases expire so that a dead worker's work is picked up. But a
worker that is merely slow (a long pause, a VM migration, a laptop lid) does not know its lease ran out. When it wakes
it will happily record the outcome of a step that another worker has since run and recorded too.

Separately, workers on different machines have different clocks. "Has this lease expired" and "is this timer due" must
get the same answer everywhere.

## Decision

Every claim increments `fence` on the workflow row and hands the new value to the worker. Every write a run makes checks
`fence = $mine and status = 'running'` against the row, locked `FOR SHARE`, in the same statement or transaction as the
write. A claim takes its rows with `FOR UPDATE SKIP LOCKED`, so while that write holds the row a takeover skips it, and
it gets the row on a later pass, after the write has committed, with the fence bumped. Any later write by the old
worker finds the wrong fence and raises `LeaseLost`. The primary key on `(workflow_id, seq)` is a second line
of defence: if two writers ever reached the same position, one insert would fail.

All time comes from `now()` in Postgres: lease expiry, timer deadlines, retry times, `ctx.now()`. The worker's clock is
never compared with anything.

Waking a parked workflow uses the same idea. A signal or a cancel bumps `signal_seq`, and parking checks the value the
run started with. If a signal arrives between the run's check for it and its decision to sleep, the park is refused and
the run reads again (`test_a_signal_landing_between_the_check_and_the_park_is_not_lost`).

## Consequences

- A zombie worker can execute an activity a second time (activities are at-least-once, ADR 5) but can never record a
  second outcome, overwrite a final status, or park a workflow someone else owns. Each of those writes has a test that
  presents a stale fence (`test_every_fenced_write_refuses_a_stale_fence`).
- Clock drift between workers does not matter. Drift between the database and real time would, but that is one clock to
  keep honest instead of N.
- The append is one statement (`with owner as (select ... for share) insert ... select from owner`), so the hot path
  stays at one round trip per step.

## Alternatives

- **Advisory locks held for the length of a run.** They tie the lease to a connection, so a run would hold a connection
  for as long as its slowest activity, and a connection reset drops the lock without telling anyone.
- **Compare-and-set on a version only at the end of a run.** Lets a zombie record intermediate steps, which later
  replay as if they were genuine.
