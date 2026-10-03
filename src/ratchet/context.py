"""Replay. The workflow function runs from the top on every wake, and this is what makes that safe.

Every durable call takes the next position in the workflow's history. If the history already holds an outcome for
that position, the call returns it without doing anything. If it does not, the call does the work, records the outcome
under a fence that only the lease holder passes, and returns that. Positions are handed out when the call is made, not
when it is awaited, so ``asyncio.TaskGroup`` branches get the same positions on every run.

A branch that cannot go on (a timer not yet due, a signal not yet sent, a retry backing off) does not unwind the run
straight away. It waits until no other branch is in the middle of a live call, so a sibling's activity is never cut off
halfway by somebody else's sleep. Then the run unwinds once, carrying every parked branch's wake condition.

This module knows nothing about Postgres. It talks to a :class:`Journal`, which the store implements.
"""

import asyncio
import contextlib
import random
import time
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, NoReturn, Protocol
from uuid import UUID, uuid7

from pydantic import TypeAdapter, ValidationError

from ratchet import telemetry
from ratchet.definitions import Activity, ActivityInfo, Json
from ratchet.errors import (
    ActivityError,
    HistoryTooLong,
    NonDeterminismError,
    NonRetryableError,
    SignalTimeout,
    Suspend,
    Wakeup,
    WorkflowCancelled,
)

_ANY: TypeAdapter[Any] = TypeAdapter(Any)
_UUID = TypeAdapter(UUID)
_MAX_ERROR_MESSAGE = 2000


@dataclass(frozen=True)
class Event:
    """One recorded outcome. ``kind`` and ``name`` are what replay checks the code against."""

    seq: int
    kind: str
    name: str
    payload: Json


@dataclass(frozen=True)
class RetryState:
    attempts: int
    next_at: datetime


@dataclass(frozen=True)
class RunState:
    """Everything a run needs, read in one go when the lease is taken."""

    workflow_id: str
    history: Mapping[int, Event]
    retries: Mapping[int, RetryState]
    now: datetime
    """The database clock when the lease was taken. Time only ever comes from Postgres."""
    cancel_requested: bool


class Journal(Protocol):
    """Fenced writes into one workflow's history.

    Each raises ``LeaseLost`` if another worker owns the workflow now, ``JournalUnavailable`` if the database cannot be
    reached, and ``UnstorableValue`` if Postgres refuses the value itself.
    """

    async def append(self, seq: int, kind: str, name: str, payload: Json) -> Json:
        """Record an outcome and return the payload as stored, which is exactly what a replay will read back."""
        ...

    async def append_now(self, seq: int) -> datetime:
        """Record the database clock at ``seq`` and return it."""
        ...

    async def append_deadline(
        self, seq: int, kind: str, name: str, delay: timedelta | None
    ) -> tuple[datetime | None, datetime]:
        """Record ``now + delay`` (or no deadline) at ``seq``. Returns the deadline and the database clock."""
        ...

    async def take_signal(self, seq: int, name: str, accept: Callable[[Json], bool]) -> tuple[bool, Json]:
        """Consume the oldest signal called ``name`` that ``accept`` takes, and record it at ``seq``, in one
        transaction. Signals ``accept`` refuses on the way are set aside as rejected, not left to block the queue."""
        ...

    async def schedule_retry(self, seq: int, attempts: int, delay: timedelta, error: Json) -> datetime:
        """Remember a failed attempt of the step at ``seq`` and when to try again. Returns that time."""
        ...


def clean(text: str) -> str:
    """Postgres text and jsonb cannot hold NUL. Error messages are evidence, so mark it instead of losing the value."""
    return text.replace("\x00", "\\u0000")


def _error(exc: BaseException, attempts: int) -> dict[str, Json]:
    return {"type": type(exc).__qualname__, "message": clean(str(exc))[:_MAX_ERROR_MESSAGE], "attempts": attempts}


def _utc(value: datetime) -> datetime:
    """One offset for every instant a workflow sees, so formatting a time cannot differ between run and replay."""
    return value.astimezone(UTC)


def _when(value: Json) -> datetime | None:
    return None if value is None else _utc(datetime.fromisoformat(value))


class WorkflowContext:
    """The only way a workflow touches the outside world. Everything else in a workflow must be deterministic."""

    def __init__(self, state: RunState, journal: Journal, *, max_history: int | None = None) -> None:
        self._state = state
        self._journal = journal
        self._max_history = max_history
        self._next_seq = 0
        self._now = state.now
        self._last_recorded = max(state.history, default=-1)
        self._cancel_pending = state.cancel_requested and not any(e.kind == "cancelled" for e in state.history.values())
        self.cancel_delivered = False
        """True once this run has recorded a cancel. The worker then never parks the workflow without re-reading it."""
        self._live = 0
        self._quiet = asyncio.Event()
        self._quiet.set()
        self._parked: list[Suspend] = []

    @property
    def workflow_id(self) -> str:
        return self._state.workflow_id

    @property
    def is_replaying(self) -> bool:
        """True while the next call will be answered from history. Use it to keep logs from repeating on every wake."""
        return self._next_seq <= self._last_recorded

    def unreached_history(self) -> list[int]:
        """Positions the history holds but this run never reached. Non-empty after a return means the code changed."""
        return sorted(seq for seq in self._state.history if seq >= self._next_seq)

    # Durable calls. Each takes its position(s) synchronously, then returns the coroutine that does the work.

    def step[**P, R](self, activity: Activity[P, R], /, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
        """Run an activity once, durably. Arguments are not recorded; the result is, and replays return it."""
        return self._step(self._take(), activity, *args, **kwargs)

    def sleep(self, duration: timedelta) -> Coroutine[Any, Any, None]:
        """A timer that survives restarts. The worker is free to do other work, or to die, while it runs."""
        return self._sleep(self._take(), self._take(), duration)

    def wait_for_signal[T](
        self, name: str, *, timeout: timedelta | None = None, payload_type: type[T] | None = None
    ) -> Coroutine[Any, Any, T]:
        """Wait for a signal sent through the API or the client. Raises :class:`SignalTimeout` at the deadline.

        A signal whose payload does not validate against ``payload_type`` is set aside as rejected and the wait goes
        on, so a malformed request cannot fail the workflow.
        """
        adapter: TypeAdapter[T] = TypeAdapter(payload_type) if payload_type is not None else _ANY
        return self._wait_for_signal(self._take(), self._take(), name, timeout, adapter)

    def now(self) -> Coroutine[Any, Any, datetime]:
        """The database clock in UTC, recorded the first time, so every replay sees the same instant."""
        return self._now_at(self._take())

    def side_effect(self, fn: Callable[[], Json], *, name: str | None = None) -> Coroutine[Any, Any, Json]:
        """Run a small local, non-deterministic function once and record its value. Not for I/O: use an activity.

        The value comes back in its stored JSON form, on the first run as on every replay.
        """
        return self._side_effect(self._take(), fn, name or str(getattr(fn, "__name__", "side_effect")))

    def uuid(self) -> Coroutine[Any, Any, UUID]:
        """A time-ordered UUID that stays the same across replays."""
        return self._uuid(self._take())

    # The machinery behind them.

    def _take(self) -> int:
        seq = self._next_seq
        if self._max_history is not None and seq >= self._max_history:
            raise HistoryTooLong(self._max_history)
        self._next_seq += 1
        return seq

    def _recorded(self, seq: int, kind: str, name: str) -> Event | None:
        event = self._state.history.get(seq)
        if event is None:
            return None
        if event.kind == "cancelled":
            raise WorkflowCancelled
        if event.kind != kind or event.name != name:
            raise NonDeterminismError(seq, f"{event.kind} {event.name!r}", f"{kind} {name!r}")
        return event

    @contextlib.asynccontextmanager
    async def _live_call(self, seq: int) -> AsyncIterator[None]:
        """Marks a call that is doing real work, so that a parking sibling waits for it to finish and be recorded."""
        self._live += 1
        self._quiet.clear()
        try:
            await self._deliver_cancel(seq)
            yield
        finally:
            self._live -= 1
            if self._live == 0:
                self._quiet.set()

    async def _park(self, wake_at: datetime | None, signals: frozenset[str] = frozenset()) -> NoReturn:
        """Stop this branch until ``wake_at`` or a signal. Unwinds the run only once no branch is mid-way through a live
        call, and with every parked branch's condition merged, so no sibling's wake-up is lost or cut short."""
        self._parked.append(Suspend(wake_at, signals))
        while self._live:
            await self._quiet.wait()
        wakeup = Wakeup.merge(self._parked)
        raise Suspend(wakeup.wake_at, wakeup.signals)

    async def _park_unless_cancelled(self, seq: int, wake_at: datetime) -> NoReturn:
        """Park, unless a cancel is waiting to be delivered: a cancel must not sit out a timer or a backoff."""
        if self._cancel_pending:
            async with self._live_call(seq):
                pass  # delivers the cancel, which raises
        await self._park(wake_at)

    async def _deliver_cancel(self, seq: int) -> None:
        """A requested cancel lands at the first live call, exactly once, and is recorded there like anything else."""
        if self._cancel_pending:
            self._cancel_pending = False
            await self._journal.append(seq, "cancelled", "", None)
            self.cancel_delivered = True
            raise WorkflowCancelled

    async def _step[**P, R](self, seq: int, activity: Activity[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        event = self._recorded(seq, "step", activity.name)
        if event is not None:
            if "error" in event.payload:
                e = event.payload["error"]
                raise ActivityError(activity.name, e["type"], e["message"], e["attempts"])
            return activity.result.validate_python(event.payload["result"])

        retry = self._state.retries.get(seq)
        if retry is not None and retry.next_at > self._now:
            await self._park_unless_cancelled(seq, retry.next_at)  # woken early, by a signal for another branch
        attempt = (retry.attempts if retry is not None else 0) + 1
        retry_at: datetime | None = None
        async with self._live_call(seq):
            info = ActivityInfo(self.workflow_id, seq, attempt)
            started = time.perf_counter()
            attributes = {"activity": activity.name}
            try:
                with telemetry.tracer.start_as_current_span(
                    f"ratchet.activity {activity.name}", attributes={"activity.seq": seq, "activity.attempt": attempt}
                ):
                    timeout = None if activity.timeout is None else activity.timeout.total_seconds()
                    async with asyncio.timeout(timeout):
                        value = await activity.invoke(info, *args, **kwargs)
            except Exception as exc:
                telemetry.step_duration.record(time.perf_counter() - started, attributes)
                telemetry.steps.add(1, attributes | {"outcome": "error"})
                error = _error(exc, attempt)
                if not isinstance(exc, NonRetryableError) and attempt < activity.retry.max_attempts:
                    sample = random.random()  # noqa: S311  # jitter, not security
                    delay = activity.retry.delay_after(attempt, sample)
                    retry_at = await self._journal.schedule_retry(seq, attempt, delay, error)
                else:
                    await self._journal.append(seq, "step", activity.name, {"error": error})
                    raise ActivityError(activity.name, error["type"], error["message"], attempt) from exc
            else:
                telemetry.step_duration.record(time.perf_counter() - started, attributes)
                telemetry.steps.add(1, attributes | {"outcome": "ok"})
                # Hand back what the database stored, not what the activity returned: jsonb reorders object keys, and
                # a replay must not see a dict in a different order from the live run.
                stored = await self._journal.append(
                    seq, "step", activity.name, {"result": activity.result.dump_python(value, mode="json")}
                )
                return activity.result.validate_python(stored["result"])
        return await self._park(retry_at)

    async def _deadline(self, seq: int, kind: str, name: str, delay: timedelta | None) -> datetime | None:
        event = self._recorded(seq, kind, name)
        if event is not None:
            return _when(event.payload["deadline"])
        async with self._live_call(seq):
            deadline, now = await self._journal.append_deadline(seq, kind, name, delay)
        self._now = max(self._now, now)
        return None if deadline is None else _utc(deadline)

    async def _sleep(self, start: int, outcome: int, duration: timedelta) -> None:
        fire_at = await self._deadline(start, "timer", "sleep", duration)
        assert fire_at is not None  # noqa: S101  # a sleep always has a deadline
        if self._recorded(outcome, "timer_fired", "sleep") is not None:
            return
        if fire_at > self._now:
            await self._park_unless_cancelled(outcome, fire_at)
        async with self._live_call(outcome):
            await self._journal.append(outcome, "timer_fired", "sleep", None)

    async def _wait_for_signal[T](
        self, start: int, outcome: int, name: str, timeout: timedelta | None, adapter: TypeAdapter[T]
    ) -> T:
        deadline = await self._deadline(start, "signal_wait", name, timeout)
        event = self._state.history.get(outcome)
        if event is not None:
            if event.kind == "signal" and event.name == name:
                return adapter.validate_python(event.payload)
            if event.kind == "timeout" and event.name == name:
                raise SignalTimeout(name)
            self._recorded(outcome, "signal", name)  # raises: cancelled, or a mismatch

        def accept(payload: Json) -> bool:
            try:
                adapter.validate_python(payload)
            except ValidationError:
                return False
            return True

        async with self._live_call(outcome):
            found, payload = await self._journal.take_signal(outcome, name, accept)
            if not found and deadline is not None and deadline <= self._now:
                await self._journal.append(outcome, "timeout", name, None)
                raise SignalTimeout(name)
        if found:
            return adapter.validate_python(payload)
        return await self._park(deadline, frozenset({name}))

    async def _now_at(self, seq: int) -> datetime:
        event = self._recorded(seq, "now", "now")
        if event is not None:
            return _utc(datetime.fromisoformat(event.payload))
        async with self._live_call(seq):
            now = _utc(await self._journal.append_now(seq))
        self._now = max(self._now, now)
        return now

    async def _side_effect(self, seq: int, fn: Callable[[], Json], name: str) -> Json:
        event = self._recorded(seq, "side_effect", name)
        if event is not None:
            return event.payload
        async with self._live_call(seq):
            return await self._journal.append(seq, "side_effect", name, _ANY.dump_python(fn(), mode="json"))

    async def _uuid(self, seq: int) -> UUID:
        event = self._recorded(seq, "side_effect", "uuid")
        if event is not None:
            return _UUID.validate_python(event.payload)
        async with self._live_call(seq):
            value = uuid7()
            await self._journal.append(seq, "side_effect", "uuid", str(value))
        return value
