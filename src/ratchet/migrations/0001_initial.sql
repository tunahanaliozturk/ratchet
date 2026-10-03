-- One row per workflow. due_at is the single scheduling column: when somebody has to look at this row next.
--   ready     due now
--   running   when the lease runs out
--   sleeping  when the earliest timer or retry fires, or NULL if only a signal or a cancel can wake it
--   final     NULL, always
-- One partial index on it serves every claim.
create table ratchet_workflows (
    id               text        primary key check (length(id) between 1 and 200),
    name             text        not null,
    input            jsonb       not null,
    status           text        not null check (status in ('ready', 'running', 'sleeping', 'completed', 'failed', 'cancelled')),
    result           jsonb,
    error            jsonb,
    due_at           timestamptz,
    waiting_signals  text[]      not null default '{}',
    lease_owner      text,
    fence            bigint      not null default 0,
    signal_seq       bigint      not null default 0,
    cancel_requested boolean     not null default false,
    created_at       timestamptz not null default now(),
    updated_at       timestamptz not null default now(),
    finished_at      timestamptz,
    constraint final_rows_are_never_due check (status not in ('completed', 'failed', 'cancelled') or due_at is null),
    constraint running_rows_have_an_owner check (status <> 'running' or (lease_owner is not null and due_at is not null))
);

create index ratchet_workflows_due on ratchet_workflows (due_at) where due_at is not null;
create index ratchet_workflows_by_status on ratchet_workflows (status, created_at, id);
create index ratchet_workflows_by_age on ratchet_workflows (created_at, id);

-- The history. Append-only: nothing in the engine updates or deletes a row here. The primary key is the last line of
-- defence against two writers recording the same position; the fence check in front of every insert is the first.
create table ratchet_events (
    workflow_id text        not null references ratchet_workflows (id) on delete cascade,
    seq         integer     not null check (seq >= 0),
    kind        text        not null,
    name        text        not null,
    payload     jsonb,
    recorded_at timestamptz not null default now(),
    primary key (workflow_id, seq)
);

create table ratchet_signals (
    id           bigint generated always as identity primary key,
    workflow_id  text        not null references ratchet_workflows (id) on delete cascade,
    name         text        not null,
    payload      jsonb,
    dedupe_key   text,
    consumed_seq integer,
    rejected_at  timestamptz,  -- the payload did not validate against what the workflow waits for; never delivered
    sent_at      timestamptz not null default now(),
    unique (workflow_id, dedupe_key)
);

create index ratchet_signals_pending on ratchet_signals (workflow_id, name, id)
    where consumed_seq is null and rejected_at is null;

-- Failed attempts of a step that will be retried. Mutable on purpose: it is bookkeeping, not history. The final
-- outcome, success or failure, goes into ratchet_events like any other.
create table ratchet_step_retries (
    workflow_id text        not null references ratchet_workflows (id) on delete cascade,
    seq         integer     not null,
    attempts    integer     not null check (attempts > 0),
    next_at     timestamptz not null,
    last_error  jsonb       not null,
    primary key (workflow_id, seq)
);
