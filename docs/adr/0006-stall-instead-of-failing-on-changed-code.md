# 6. Changed code stalls a workflow instead of failing it

## Context

Two things can stop a worker from replaying a workflow it claimed. The workflow's name may not be registered in this
worker (a rolling deploy where the new workflow type has not reached every worker yet). Or the code may no longer make
the calls the history recorded (`NonDeterminismError`).

Both are deploy problems, not workflow problems. Failing the workflow would throw away its progress because of a bug
that a redeploy fixes in five minutes.

## Decision

The worker parks the workflow as `sleeping`, records the reason in its `error` field, and comes back to it every minute.
A worker that knows the name, or code that matches the history again, picks it up from where it was. The next successful
park or finish clears `error`.

## Consequences

- An operator finds stalled workflows with `GET /v1/workflows?status=sleeping` and a non-null `error`, and the
  `ratchet.runs` counter has an `outcome=stalled` series to alert on (docs/operations.md).
- A genuinely wrong deploy keeps those workflows parked until someone acts. That is the point: nothing is lost while a
  person decides.
- There is no versioning API yet (`ctx.patched(...)` in Temporal terms). Changing the code of a workflow with running
  instances safely means only adding calls after the last position any instance has reached, or registering the new
  version under a new name and letting old instances drain. The README lists this under limitations.
