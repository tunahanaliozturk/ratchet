# Changelog

## 0.1.0 (2026-10-03)

First release.

- Workflows as async functions, replayed by position; `ctx.step`, `ctx.sleep`, `ctx.wait_for_signal`, `ctx.now`,
  `ctx.uuid`, `ctx.side_effect`; parallel branches with `asyncio.TaskGroup`.
- Activities with retry policies (exponential backoff, jitter, durable between attempts), timeouts, non-retryable
  errors and a stable idempotency key.
- `Saga` for compensation in reverse order; cancellation delivered as `WorkflowCancelled`.
- Leases with heartbeats and fencing tokens; database time only; signals that cannot be lost to a race.
- Changed code and unknown workflow names stall a workflow instead of failing it.
- Database failures during a run never fail a workflow; values Postgres refuses fail it once.
- Parked branches wait for siblings' live calls instead of cancelling them; malformed signals are set aside.
- HTTP API with bearer auth, problem+json errors, keyset pagination and a body limit; `ratchet migrate | worker | api`.
- Tests: unit replay rules, Postgres integration, a Hypothesis crash-schedule property, a real SIGKILL test, the order
  example end to end, and the README quick start against the compose stack in CI.
