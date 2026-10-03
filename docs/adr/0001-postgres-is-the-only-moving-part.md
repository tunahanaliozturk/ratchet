# 1. Postgres is the only moving part

## Context

A workflow engine needs four things: a durable record of what happened, a way for many workers to share work without
two of them doing the same thing, timers, and a way to wake a worker when something becomes due. The usual answer is a
database plus a queue (Redis, RabbitMQ) plus a scheduler. That is three systems to run, back up and reason about
together, and three places where "committed" can mean different things.

The teams this is for already run Postgres. Most of them do not want a Temporal cluster for a few dozen workflow types.

## Decision

Everything lives in Postgres:

- the history is a table with `(workflow_id, seq)` as its primary key;
- work sharing is `SELECT ... FOR UPDATE SKIP LOCKED` on one partial index;
- timers, retry backoff and lease expiry are all one column, `due_at` (ADR 4);
- waking idle workers is `LISTEN`/`NOTIFY`, with a poll as the fallback.

Workers and the API are stateless processes. The database is the only thing that needs a backup.

## Consequences

- A state change and its history entry commit in one transaction. There is no window where the queue says one thing
  and the database another.
- Throughput is bounded by what one Postgres primary can do. The published numbers (docs/benchmark-results) are about
  900 three-step workflows a second on a laptop, which covers the intended use by a wide margin. This is not a design
  for a hundred thousand workflows a second.
- `NOTIFY` is only a hint. If one is lost (a listener reconnecting, say), the worker's poll picks the work up within
  `RATCHET_IDLE_WAIT_MAX_SECONDS`. Correctness never depends on a notification arriving.

## Alternatives

- **Redis streams for dispatch.** Faster to hand out work, but a second source of truth whose commit is not the
  database's commit. The outbox needed to keep the two in step is more code than the engine.
- **Temporal or Restate.** Far more capable, and far more to operate. The point of this project is to show the
  mechanics at a size one person can read in an afternoon.
