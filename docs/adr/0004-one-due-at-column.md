# 4. One scheduling column: `due_at`

## Context

Several different things make a workflow need attention: it was just started, a timer fired, a retry backoff ended, a
signal arrived, a lease ran out. A claim query that checks each with its own condition becomes an `OR` the planner cannot
serve from one index, and it gets slower with each case added.

## Decision

Every workflow that is not final has at most one time it next needs looking at, stored in `due_at`:

| status | due_at |
|---|---|
| ready | now |
| running | when the lease runs out (the heartbeat pushes it forward) |
| sleeping | the earliest timer or retry, or NULL when only a signal or a cancel can wake it |
| completed, failed, cancelled | NULL, enforced by a check constraint |

The claim is `where due_at <= now() order by due_at limit n for update skip locked`, served by one partial index on
`due_at where due_at is not null`. Final rows are not in that index at all, so a table with millions of finished
workflows claims as fast as an empty one.

## Consequences

- Crash recovery needs no separate sweeper. A dead worker's leases simply come due.
- A signal sets `due_at = now()` only if the parked workflow is waiting for that signal name, so an unrelated signal
  does not cause a pointless replay.
- Ordering is by when work became due, which is roughly FIFO. There are no priorities. If they are ever needed they
  belong in the sort key of the same index, not in a second queue.
