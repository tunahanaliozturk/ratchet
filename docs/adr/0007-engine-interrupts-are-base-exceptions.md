# 7. Engine interrupts are BaseExceptions

## Context

The engine has to unwind a running workflow for reasons that are none of the workflow's business: it must park
(`Suspend`), another worker owns it now (`LeaseLost`), or the database cannot record anything (`JournalUnavailable`).

The first benchmark run showed what goes wrong when those are ordinary exceptions. Four workers, each with a connection
pool sized to its concurrency, ran Postgres out of connections. The `TooManyConnectionsError` raised while recording a
step travelled up through the workflow like any exception, the worker recorded it as the workflow's failure, and a
capacity problem became permanent data loss for every workflow in flight.

## Decision

All three derive from `EngineInterrupt(BaseException)`. Workflow code that catches `Exception` cannot swallow them, a
`Saga` does not compensate for them, and the worker handles each one explicitly, recording nothing for the two failure
cases. Every database call a run makes goes through one decorator that turns connection and server errors into
`JournalUnavailable`.

The worker's connection pool is sized on its own (`RATCHET_POOL_SIZE`) rather than from its concurrency, because a run
holds a connection only while it writes.

## Consequences

- An outage leaves workflows `running` with a lease that will expire, and they continue afterwards from their last
  recorded position. `test_losing_the_database_mid_step_leaves_the_workflow_for_another_run_instead_of_failing_it`
  covers it, and fails if the decorator is removed.
- Workflow authors who write `except BaseException` can still break this. The docs say not to, as they would for
  `asyncio.CancelledError`.
