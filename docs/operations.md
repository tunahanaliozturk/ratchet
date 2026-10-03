# Operations

## Processes

| command | what it does | how many |
|---|---|---|
| `ratchet migrate` | brings the schema up to date, under an advisory lock, then exits | once per deploy, before the rest |
| `ratchet worker` | claims due workflows and runs them | as many as you like; they share work through row locks |
| `ratchet api --host --port` | the HTTP API | as many as you like; stateless |

All three read `RATCHET_APP` (`module:attribute` of your `Registry`) and must load the same registry version, or
workflows stall (ADR 6).

## Configuration

| variable | default | meaning |
|---|---|---|
| `RATCHET_DATABASE_URL` | required | `postgresql://user:password@host:port/db` |
| `RATCHET_APP` | required | `module:attribute` of the Registry |
| `RATCHET_API_TOKEN` | required for `api` | bearer token, at least 16 characters, compared in constant time; there is no anonymous mode |
| `RATCHET_CONCURRENCY` | 32 | workflows one worker runs at once |
| `RATCHET_POOL_SIZE` | 10 | Postgres connections per process (minimum 3: listener, claim loop, writes) |
| `RATCHET_LEASE_SECONDS` | 30 | how long a crashed worker's workflows wait before another worker takes them; heartbeats run at a third of this |
| `RATCHET_IDLE_WAIT_MAX_SECONDS` | 5 | longest an idle worker waits without a NOTIFY before polling |
| `RATCHET_MAX_HISTORY` | 10000 | a workflow with more recorded events fails rather than replaying ever more slowly |
| `RATCHET_SHUTDOWN_GRACE_SECONDS` | 20 | on SIGTERM, how long in-flight runs get before they are handed back |
| `RATCHET_MAX_PAYLOAD_BYTES` | 262144 | largest request body the API accepts |
| `RATCHET_WORKER_ID` | host-pid-random | shows up as `lease_owner` |
| `RATCHET_OTLP_ENDPOINT` | unset | base URL of an OTLP/HTTP collector; traces and metrics are off without it |
| `RATCHET_LOG_LEVEL` | INFO | JSON logs on stdout |

**Connections.** Total connections are roughly `(workers + api processes) x RATCHET_POOL_SIZE`. Keep that under
Postgres `max_connections` (100 by default) with room for everything else. ADR 7 tells the story of what happens
otherwise.

## What to watch

| signal | why | alert when |
|---|---|---|
| `ratchet.runs{outcome="stalled"}` | a deploy left workflows that no worker can replay (ADR 6) | any, for 10 minutes |
| `ratchet.runs{outcome="journal_unavailable"}` | runs that could not record; the database is struggling | sustained above zero |
| `ratchet.runs{outcome="crashed"}` | a run hit an error that is neither the workflow's nor an outage: a bug, logged with its traceback | any |
| `ratchet.leases.lost` | runs abandoned because another worker took over; activities are running twice | rate above the usual baseline (it should be near zero) |
| `ratchet.workflows.finished{status="failed"}` | business failures | per workflow type, against your own expectation |
| `ratchet.step.duration` per activity | slow dependencies | p99 above the activity's timeout divided by two |
| due backlog: `select count(*) from ratchet_workflows where due_at < now() - interval '1 minute'` | work that is due and nobody is taking | above zero for 5 minutes |

## Runbooks

**Workflows stuck in `sleeping` with an error.** `GET /v1/workflows?status=sleeping` and look at `error.type`.
`UnknownWorkflow` means some worker runs an old registry: finish the rollout. `NonDeterminismError` means the code no
longer matches the history at `error.message`'s position: roll back, or ship code that makes the recorded calls in the
recorded order up to that point. They retry every minute on their own; nothing needs to be reset.

**A worker was killed.** Nothing to do. Its workflows come due when their leases run out (`RATCHET_LEASE_SECONDS`) and
any worker continues them. The step that was in flight runs again, with the same idempotency key.

**Database outage.** Workers log `database unavailable during a run` and `cannot claim work; retrying`, and keep
retrying. Workflows are left `running` with leases that expire, and continue from their last recorded position once the
database is back. Nothing is marked failed because of an outage.

**Backlog growing.** Add workers, or raise `RATCHET_CONCURRENCY` if the activities are I/O-bound. Check connections
first: more workers means more connections.

**A workflow ignores a signal.** Check whether it was rejected: `select id, payload, rejected_at from ratchet_signals
where workflow_id = '...' order by id`. A rejected signal's payload did not validate against the type the workflow
waits for. Send a corrected one; the workflow is still waiting.

**Cancelling a lot of workflows.** `POST /v1/workflows/{id}/cancel` per workflow. Each sees `WorkflowCancelled` at its
next durable call and runs its compensations.

## Retention

Finished workflows stay in the table with their history. They cost nothing on the claim path (the due index excludes
them) but they take disk. Delete old ones in batches; the history goes with them through `on delete cascade`:

```sql
delete from ratchet_workflows
where id in (
    select id from ratchet_workflows
    where status in ('completed', 'failed', 'cancelled') and finished_at < now() - interval '30 days'
    limit 10000
);
```

## Known limitations

- **Activities are at-least-once.** Use `activity_info().idempotency_key` on the other side (ADR 5).
- **No workflow versioning API.** See ADR 6 for the safe ways to change code with instances running.
- **No child workflows, no continue-as-new.** A workflow that loops forever grows its history until
  `RATCHET_MAX_HISTORY` stops it.
- **One Postgres primary is the ceiling.** Roughly 900 three-step workflows a second on a laptop through Docker Desktop
  (docs/benchmark-results). Sharding is not on the table.
- **A cancel is a request.** A workflow that completes before it next makes a durable call completes; cancelling it
  afterwards gets a 409.
- **The API has a single shared token.** Put it behind your own gateway if you need per-caller identity.
