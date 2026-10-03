"""Everything that touches Postgres. Every statement is parameterised; no SQL is built from values.

Two rules hold for every write a run makes:

* It carries the fence it was given when it claimed the workflow, and checks it under a row lock in the same statement
  or transaction. A worker whose lease was taken over cannot write anything, however late it wakes up.
* Time comes from ``now()`` in the database, never from the worker's clock, so workers with drifting clocks still agree
  on what is due and whose lease has run out.
"""

import functools
import json
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal

import asyncpg

from ratchet.context import Event, RetryState, RunState
from ratchet.definitions import Json
from ratchet.errors import (
    JournalUnavailable,
    LeaseLost,
    UnstorableValue,
    WorkflowFinished,
    WorkflowIdConflict,
    WorkflowNotFound,
)

CHANNEL = "ratchet_due"
FINAL = ("completed", "failed", "cancelled")
type Status = Literal["ready", "running", "sleeping", "completed", "failed", "cancelled"]


@dataclass(frozen=True, slots=True)
class Jsonb:
    """A JSON value on its way into a jsonb column.

    asyncpg sends a bare Python ``None`` as SQL NULL without asking the codec, and ``None`` is a perfectly good workflow
    input or activity result. Wrapping every JSON value keeps the two apart: ``Jsonb(None)`` is stored as JSON ``null``,
    a bare ``None`` as SQL NULL, and anything unwrapped is refused rather than guessed at.
    """

    value: Json


def _encode(value: object) -> str:
    if not isinstance(value, Jsonb):
        raise TypeError(f"jsonb parameters must be wrapped in Jsonb, got {type(value).__name__}")
    return json.dumps(value.value, separators=(",", ":"))


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec("jsonb", encoder=_encode, decoder=json.loads, schema="pg_catalog")


def _maybe(value: Json) -> Jsonb | None:
    """For columns where SQL NULL means "none": result and error."""
    return None if value is None else Jsonb(value)


async def _keep_session(_: asyncpg.Connection) -> None:
    """Skip asyncpg's reset on release, which costs a round trip every time a connection goes back to the pool.

    Safe because nothing here leaves session state behind on a pooled connection: transactions are closed by their
    context managers, the one advisory lock (migrations) is released in a finally, and the listener removes itself.
    """


async def create_pool(dsn: str, *, min_size: int = 2, max_size: int = 20) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn, min_size=min_size, max_size=max_size, init=_init_connection, reset=_keep_session
    )


@dataclass(frozen=True)
class Claim:
    """A workflow this worker holds the lease on, and the fence that proves it."""

    workflow_id: str
    name: str
    input: Json
    fence: int
    signal_seq: int
    cancel_requested: bool
    now: datetime


@dataclass(frozen=True)
class WorkflowRecord:
    id: str
    name: str
    status: Status
    input: Json
    result: Json
    error: Json
    waiting_signals: list[str]
    due_at: datetime | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None


_RECORD_COLUMNS = "id, name, status, input, result, error, waiting_signals, due_at, created_at, updated_at, finished_at"


def _record(row: asyncpg.Record) -> WorkflowRecord:
    return WorkflowRecord(**dict(row))


# What says "the database cannot do this right now" rather than "this value or this statement is wrong": a lost or
# refused connection (SQLSTATE class 08), exhausted resources (53), an operator or shutdown (57), a deadlock or
# serialisation failure (40), and the client-side equivalents. Only these are retried as outages.
UNREACHABLE = (
    asyncpg.exceptions.PostgresConnectionError,
    asyncpg.exceptions.InsufficientResourcesError,
    asyncpg.exceptions.OperatorInterventionError,
    asyncpg.exceptions.TransactionRollbackError,
    asyncpg.InterfaceError,
    OSError,
    TimeoutError,
)


def _journalled[**P, R](method: Callable[P, Coroutine[Any, Any, R]]) -> Callable[P, Coroutine[Any, Any, R]]:
    """Turns a database failure during a run into :class:`JournalUnavailable`.

    Without this, a dropped connection inside ``ctx.step`` would surface in the workflow as an ordinary exception, and
    the worker would record it as the workflow's failure: an outage would permanently fail every workflow that was
    running through it.

    The opposite mistake matters as much. A value Postgres refuses (a NUL inside a string, say) fails the same way on
    every attempt, so it becomes :class:`UnstorableValue`, an ordinary exception the workflow sees and fails on, instead
    of an "outage" that re-runs the activity on every lease forever.
    """

    @functools.wraps(method)
    async def translated(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await method(*args, **kwargs)
        except UNREACHABLE as exc:
            raise JournalUnavailable(f"{type(exc).__name__}: {exc}") from exc
        except asyncpg.exceptions.DataError as exc:
            raise UnstorableValue(f"{type(exc).__name__}: {exc}") from exc

    return translated


# Every fenced write starts from this: the row, still running, still at our fence, locked against a takeover until the
# write commits. A claim needs a stronger lock than FOR SHARE, so it waits for us, then sees the new state.
_OWNED = "select 1 from ratchet_workflows where id = $1 and fence = $2 and status = 'running' for share"


class RunJournal:
    """The fenced side of the store for one claimed workflow. Implements :class:`ratchet.context.Journal`."""

    def __init__(self, pool: asyncpg.Pool, claim: Claim, lease: timedelta) -> None:
        self._pool = pool
        self.claim = claim
        self._lease = lease

    @property
    def _key(self) -> tuple[str, int]:
        return self.claim.workflow_id, self.claim.fence

    @asynccontextmanager
    async def _owned(self) -> AsyncIterator[asyncpg.Connection]:
        async with self._pool.acquire() as conn, conn.transaction():
            if await conn.fetchval(_OWNED, *self._key) is None:
                raise LeaseLost(self.claim.workflow_id)
            yield conn

    def first_state(self) -> RunState | None:
        """The state of a workflow on its very first claim, without asking the database.

        Only a lease holder can record anything, and fence 1 means nobody held the lease before, so the history and
        the retry bookkeeping are empty; the claim itself returned the clock, the signal counter and the cancel flag.
        """
        if self.claim.fence != 1:
            return None
        return RunState(self.claim.workflow_id, {}, {}, self.claim.now, self.claim.cancel_requested)

    @_journalled
    async def load(self) -> RunState:
        """The history and retry bookkeeping, plus the clock and cancel flag as they are now.

        One statement, so one snapshot and one round trip, however long the history is.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "select w.signal_seq, w.cancel_requested, now() as now,"
                " (select coalesce(jsonb_agg(jsonb_build_array(e.seq, e.kind, e.name, e.payload)), '[]')"
                "    from ratchet_events e where e.workflow_id = w.id) as events,"
                " (select coalesce(jsonb_agg(jsonb_build_array(r.seq, r.attempts, r.next_at)), '[]')"
                "    from ratchet_step_retries r where r.workflow_id = w.id) as retries"
                " from ratchet_workflows w where w.id = $1 and w.fence = $2 and w.status = 'running'",
                *self._key,
            )
        if row is None:
            raise LeaseLost(self.claim.workflow_id)
        self.claim = Claim(
            workflow_id=self.claim.workflow_id,
            name=self.claim.name,
            input=self.claim.input,
            fence=self.claim.fence,
            signal_seq=row["signal_seq"],
            cancel_requested=row["cancel_requested"],
            now=row["now"],
        )
        return RunState(
            workflow_id=self.claim.workflow_id,
            history={seq: Event(seq, kind, name, payload) for seq, kind, name, payload in row["events"]},
            retries={
                seq: RetryState(attempts, datetime.fromisoformat(next_at)) for seq, attempts, next_at in row["retries"]
            },
            now=row["now"],
            cancel_requested=row["cancel_requested"],
        )

    # Journal

    @_journalled
    async def append(self, seq: int, kind: str, name: str, payload: Json) -> Json:
        # The hot path, once per step, so it is one statement and one round trip. It returns the payload as stored,
        # because jsonb normalises (object keys come back sorted) and the live run must see what a replay will see.
        try:
            async with self._pool.acquire() as conn:
                written = await conn.fetchrow(
                    f"with owner as ({_OWNED}) "  # noqa: S608  # a constant, not a value
                    "insert into ratchet_events (workflow_id, seq, kind, name, payload)"
                    " select $1, $3, $4, $5, $6 from owner returning payload",
                    *self._key,
                    seq,
                    kind,
                    name,
                    Jsonb(payload),
                )
        except asyncpg.UniqueViolationError:
            # Somebody recorded this position already, which only a takeover can explain.
            raise LeaseLost(self.claim.workflow_id) from None
        if written is None:
            raise LeaseLost(self.claim.workflow_id)
        return written["payload"]

    @_journalled
    async def append_now(self, seq: int) -> datetime:
        async with self._owned() as conn:
            now: datetime = await conn.fetchval(
                "insert into ratchet_events (workflow_id, seq, kind, name, payload)"
                " values ($1, $2, 'now', 'now', to_jsonb(now())) returning recorded_at",
                self.claim.workflow_id,
                seq,
            )
            return now

    @_journalled
    async def append_deadline(
        self, seq: int, kind: str, name: str, delay: timedelta | None
    ) -> tuple[datetime | None, datetime]:
        async with self._owned() as conn:
            row = await conn.fetchrow(
                "insert into ratchet_events (workflow_id, seq, kind, name, payload)"
                " values ($1, $2, $3, $4, jsonb_build_object('deadline', now() + $5::interval))"
                " returning (payload->>'deadline')::timestamptz as deadline, recorded_at as now",
                self.claim.workflow_id,
                seq,
                kind,
                name,
                delay,
            )
            return row["deadline"], row["now"]

    @_journalled
    async def take_signal(self, seq: int, name: str, accept: Callable[[Json], bool]) -> tuple[bool, Json]:
        async with self._owned() as conn:
            pending = await conn.fetch(
                "select id, payload from ratchet_signals where workflow_id = $1 and name = $2"
                " and consumed_seq is null and rejected_at is null order by id for update",
                self.claim.workflow_id,
                name,
            )
            rejected = []
            signal = None
            for candidate in pending:
                if accept(candidate["payload"]):
                    signal = candidate
                    break
                rejected.append(candidate["id"])
            if rejected:
                # Kept for inspection, never delivered. A malformed request must not fail the workflow waiting for it.
                await conn.execute(
                    "update ratchet_signals set rejected_at = now() where id = any($1::bigint[])", rejected
                )
            if signal is None:
                return False, None
            await conn.execute("update ratchet_signals set consumed_seq = $2 where id = $1", signal["id"], seq)
            await conn.execute(
                "insert into ratchet_events (workflow_id, seq, kind, name, payload) values ($1, $2, 'signal', $3, $4)",
                self.claim.workflow_id,
                seq,
                name,
                Jsonb(signal["payload"]),
            )
            return True, signal["payload"]

    @_journalled
    async def schedule_retry(self, seq: int, attempts: int, delay: timedelta, error: Json) -> datetime:
        async with self._owned() as conn:
            next_at: datetime = await conn.fetchval(
                "insert into ratchet_step_retries (workflow_id, seq, attempts, next_at, last_error)"
                " values ($1, $2, $3, now() + $4::interval, $5)"
                " on conflict (workflow_id, seq) do update"
                " set attempts = excluded.attempts, next_at = excluded.next_at, last_error = excluded.last_error"
                " returning next_at",
                self.claim.workflow_id,
                seq,
                attempts,
                delay,
                Jsonb(error),
            )
            return next_at

    # The lease

    @_journalled
    async def heartbeat(self) -> bool:
        """Push the lease out. False means it is gone and the run must stop."""
        async with self._pool.acquire() as conn:
            renewed = await conn.fetchval(
                "update ratchet_workflows set due_at = now() + $3::interval, updated_at = now()"
                " where id = $1 and fence = $2 and status = 'running' returning 1",
                *self._key,
                self._lease,
            )
        return renewed is not None

    @_journalled
    async def finish(self, status: Literal["completed", "failed", "cancelled"], result: Json, error: Json) -> None:
        async with self._pool.acquire() as conn:
            done = await conn.fetchval(
                "with done as ("
                "  update ratchet_workflows set status = $3, result = $4, error = $5, due_at = null,"
                "  waiting_signals = '{}', lease_owner = null, finished_at = now(), updated_at = now()"
                "  where id = $1 and fence = $2 and status = 'running' returning id"
                "), tidied as ("
                "  delete from ratchet_step_retries r using done where r.workflow_id = done.id"
                ") select count(*) from done",
                *self._key,
                status,
                Jsonb(result) if status == "completed" else None,
                _maybe(error),
            )
        if done == 0:
            raise LeaseLost(self.claim.workflow_id)

    @_journalled
    async def suspend(self, wake_at: datetime | None, signals: frozenset[str], error: Json = None) -> bool:
        """Park the workflow until ``wake_at`` or one of ``signals``.

        Returns False, and parks nothing, if a signal or a cancel arrived since this run last read the workflow. The
        caller reloads and runs again; otherwise that signal could land between our check and our sleep and be missed.
        """
        async with self._pool.acquire() as conn:
            parked = await conn.fetchval(
                "update ratchet_workflows set status = 'sleeping', due_at = $4, waiting_signals = $5, error = $6,"
                " lease_owner = null, updated_at = now()"
                " where id = $1 and fence = $2 and status = 'running' and signal_seq = $3 returning 1",
                *self._key,
                self.claim.signal_seq,
                wake_at,
                sorted(signals),
                _maybe(error),
            )
            if parked is not None:
                return True
            if await conn.fetchval(_OWNED.removesuffix(" for share"), *self._key) is None:
                raise LeaseLost(self.claim.workflow_id)
            return False

    @_journalled
    async def release(self) -> None:
        """Hand the workflow back straight away, on a graceful shutdown, instead of making others wait out the lease."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                "update ratchet_workflows set status = 'ready', due_at = now(), lease_owner = null, updated_at = now()"
                " where id = $1 and fence = $2 and status = 'running'",
                *self._key,
            )
            await conn.execute("select pg_notify($1, '')", CHANNEL)


class Store:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    # Workers

    async def claim(self, owner: str, lease: timedelta, limit: int) -> list[Claim]:
        """Take up to ``limit`` due workflows. SKIP LOCKED lets many workers do this at once without waiting."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "with due as ("
                "  select id from ratchet_workflows where due_at <= now()"
                "  order by due_at limit $1 for update skip locked"
                ") "
                "update ratchet_workflows w set status = 'running', lease_owner = $2, due_at = now() + $3::interval,"
                " fence = w.fence + 1, waiting_signals = '{}', updated_at = now() "
                "from due where w.id = due.id "
                "returning w.id, w.name, w.input, w.fence, w.signal_seq, w.cancel_requested, now() as now",
                limit,
                owner,
                lease,
            )
        return [
            Claim(r["id"], r["name"], r["input"], r["fence"], r["signal_seq"], r["cancel_requested"], r["now"])
            for r in rows
        ]

    def journal(self, claim: Claim, lease: timedelta) -> RunJournal:
        return RunJournal(self.pool, claim, lease)

    async def seconds_until_due(self) -> float | None:
        async with self.pool.acquire() as conn:
            value: float | None = await conn.fetchval(
                "select extract(epoch from min(due_at) - now())::float8 from ratchet_workflows where due_at is not null"
            )
        return value

    @asynccontextmanager
    async def listen(self, on_due: Callable[[], None]) -> AsyncIterator[None]:
        """Call ``on_due`` whenever something becomes due now. Holds one connection for as long as it is open."""
        async with self.pool.acquire() as conn:

            def callback(*_: Any) -> None:
                on_due()

            await conn.add_listener(CHANNEL, callback)
            try:
                yield
            finally:
                await conn.remove_listener(CHANNEL, callback)

    # Clients

    async def start(self, workflow_id: str, name: str, input_: Json) -> tuple[WorkflowRecord, bool]:
        """Start a workflow, or return the one already started under this id with the same name and input."""
        async with self.pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "insert into ratchet_workflows (id, name, input, status, due_at)"  # noqa: S608  # constant columns
                " values ($1, $2, $3, 'ready', now())"
                f" on conflict (id) do nothing returning {_RECORD_COLUMNS}",
                workflow_id,
                name,
                Jsonb(input_),
            )
            if row is not None:
                await conn.execute("select pg_notify($1, '')", CHANNEL)
                return _record(row), True
            existing = await conn.fetchrow(
                f"select {_RECORD_COLUMNS} from ratchet_workflows where id = $1",  # noqa: S608  # constant columns
                workflow_id,
            )
        record = _record(existing)
        if record.name != name or record.input != input_:
            raise WorkflowIdConflict(workflow_id)
        return record, False

    async def signal(self, workflow_id: str, name: str, payload: Json, dedupe_key: str | None) -> bool:
        """Deliver a signal. Returns False if ``dedupe_key`` was used before, in which case nothing changes."""
        async with self.pool.acquire() as conn, conn.transaction():
            # Locking the row first orders us against a run that is about to park the workflow; see RunJournal.suspend.
            status = await conn.fetchval("select status from ratchet_workflows where id = $1 for update", workflow_id)
            if status is None:
                raise WorkflowNotFound(workflow_id)
            if status in FINAL:
                raise WorkflowFinished(workflow_id)
            inserted = await conn.fetchval(
                "insert into ratchet_signals (workflow_id, name, payload, dedupe_key) values ($1, $2, $3, $4)"
                " on conflict (workflow_id, dedupe_key) do nothing returning id",
                workflow_id,
                name,
                Jsonb(payload),
                dedupe_key,
            )
            if inserted is None:
                return False
            await conn.execute(
                "update ratchet_workflows set signal_seq = signal_seq + 1, updated_at = now(),"
                " due_at = case when status = 'sleeping' and $2 = any(waiting_signals) then now() else due_at end"
                " where id = $1",
                workflow_id,
                name,
            )
            await conn.execute("select pg_notify($1, '')", CHANNEL)
            return True

    async def cancel(self, workflow_id: str) -> None:
        """Ask a workflow to stop. It sees :class:`WorkflowCancelled` at its next durable call and may compensate."""
        async with self.pool.acquire() as conn, conn.transaction():
            status = await conn.fetchval("select status from ratchet_workflows where id = $1 for update", workflow_id)
            if status is None:
                raise WorkflowNotFound(workflow_id)
            if status in FINAL:
                raise WorkflowFinished(workflow_id)
            await conn.execute(
                "update ratchet_workflows set cancel_requested = true, signal_seq = signal_seq + 1, updated_at = now(),"
                " due_at = case when status = 'sleeping' then now() else due_at end where id = $1",
                workflow_id,
            )
            await conn.execute("select pg_notify($1, '')", CHANNEL)

    async def get(self, workflow_id: str) -> WorkflowRecord:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"select {_RECORD_COLUMNS} from ratchet_workflows where id = $1",  # noqa: S608  # constant columns
                workflow_id,
            )
        if row is None:
            raise WorkflowNotFound(workflow_id)
        return _record(row)

    async def history(self, workflow_id: str) -> list[tuple[Event, datetime]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "select seq, kind, name, payload, recorded_at from ratchet_events where workflow_id = $1 order by seq",
                workflow_id,
            )
            if not rows and await conn.fetchval("select 1 from ratchet_workflows where id = $1", workflow_id) is None:
                raise WorkflowNotFound(workflow_id)
        return [(Event(r["seq"], r["kind"], r["name"], r["payload"]), r["recorded_at"]) for r in rows]

    async def list(self, status: Status | None, after: tuple[datetime, str] | None, limit: int) -> list[WorkflowRecord]:
        """Oldest first, keyset-paginated on (created_at, id) so deep pages cost the same as the first."""
        after_at, after_id = after if after is not None else (None, None)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"select {_RECORD_COLUMNS} from ratchet_workflows"  # noqa: S608  # constant columns
                " where ($1::text is null or status = $1)"
                " and ($2::timestamptz is null or (created_at, id) > ($2, $3))"
                " order by created_at, id limit $4",
                status,
                after_at,
                after_id,
                limit,
            )
        return [_record(r) for r in rows]
