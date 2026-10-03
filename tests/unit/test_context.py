"""Replay rules, against an in-memory journal. The Postgres side of the same promises is in tests/integration."""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from ratchet import (
    ActivityError,
    NonDeterminismError,
    NonRetryableError,
    Registry,
    RetryPolicy,
    Saga,
    WorkflowCancelled,
)
from ratchet.context import Event, RetryState, RunState, WorkflowContext
from ratchet.errors import SignalTimeout, Suspend

T0 = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)


class MemoryJournal:
    def __init__(self, signals: dict[str, list[Any]] | None = None) -> None:
        self.events: dict[int, Event] = {}
        self.retries: dict[int, tuple[int, timedelta]] = {}
        self.signals = signals or {}
        self.now = T0

    def _put(self, seq: int, kind: str, name: str, payload: Any) -> None:
        assert seq not in self.events, f"position {seq} written twice"
        self.events[seq] = Event(seq, kind, name, payload)

    async def append(self, seq: int, kind: str, name: str, payload: Any) -> None:
        self._put(seq, kind, name, payload)

    async def append_now(self, seq: int) -> datetime:
        self._put(seq, "now", "now", self.now.isoformat())
        return self.now

    async def append_deadline(
        self, seq: int, kind: str, name: str, delay: timedelta | None
    ) -> tuple[datetime | None, datetime]:
        deadline = None if delay is None else self.now + delay
        self._put(seq, kind, name, {"deadline": None if deadline is None else deadline.isoformat()})
        return deadline, self.now

    async def take_signal(self, seq: int, name: str) -> tuple[bool, Any]:
        queue = self.signals.get(name) or []
        if not queue:
            return False, None
        payload = queue.pop(0)
        self._put(seq, "signal", name, payload)
        return True, payload

    async def schedule_retry(self, seq: int, attempts: int, delay: timedelta, error: Any) -> datetime:
        self.retries[seq] = (attempts, delay)
        return self.now + delay


def state(
    history: dict[int, Event] | None = None,
    *,
    retries: dict[int, RetryState] | None = None,
    cancel: bool = False,
    now: datetime = T0,
) -> RunState:
    return RunState("wf-1", history or {}, retries or {}, now, cancel)


registry = Registry()
calls: list[str] = []


@registry.activity()
async def pair(a: int, b: int) -> tuple[int, int]:
    calls.append("pair")
    return a, b


@registry.activity()
async def other() -> str:
    calls.append("other")
    return "other"


@registry.activity(retry=RetryPolicy(max_attempts=3, initial_delay=timedelta(seconds=2), jitter=0))
async def flaky() -> str:
    calls.append("flaky")
    raise ConnectionError("down")


class Fatal(NonRetryableError):
    pass


@registry.activity(retry=RetryPolicy(max_attempts=5))
async def fatal() -> str:
    calls.append("fatal")
    raise Fatal("no")


@registry.activity()
async def undo(label: str) -> None:
    calls.append(f"undo {label}")


@pytest.fixture(autouse=True)
def _reset_calls() -> None:
    calls.clear()


@pytest.mark.asyncio
async def test_a_recorded_step_returns_its_recorded_result_without_running_the_activity() -> None:
    history = {0: Event(0, "step", "pair", {"result": [7, 8]})}
    ctx = WorkflowContext(state(history), MemoryJournal())

    assert await ctx.step(pair, 1, 2) == (7, 8)
    assert calls == []


@pytest.mark.asyncio
async def test_a_live_step_hands_back_what_a_replay_would_hand_back() -> None:
    journal = MemoryJournal()
    live = await WorkflowContext(state(), journal).step(pair, 1, 2)
    replayed = await WorkflowContext(state(journal.events), MemoryJournal()).step(pair, 1, 2)

    assert journal.events[0].payload == {"result": [1, 2]}
    assert live == replayed == (1, 2)
    assert type(live) is type(replayed)


@pytest.mark.asyncio
async def test_replay_refuses_code_that_calls_a_different_activity_at_a_recorded_position() -> None:
    history = {0: Event(0, "step", "pair", {"result": [1, 2]})}
    ctx = WorkflowContext(state(history), MemoryJournal())

    with pytest.raises(NonDeterminismError, match="position 0 recorded step 'pair'"):
        await ctx.step(other)
    assert calls == []


@pytest.mark.asyncio
async def test_parallel_branches_get_the_same_positions_on_every_run() -> None:
    async def body(ctx: WorkflowContext) -> list[Any]:
        async with asyncio.TaskGroup() as group:
            first = group.create_task(_after(0.02, ctx.step(pair, 1, 1)))
            second = group.create_task(_after(0.0, ctx.step(other)))
        return [first.result(), second.result()]

    journal = MemoryJournal()
    live = await body(WorkflowContext(state(), journal))
    # The second branch finished first, yet each branch kept the position it was given when it was called.
    assert [journal.events[0].name, journal.events[1].name] == ["pair", "other"]

    calls.clear()
    assert await body(WorkflowContext(state(journal.events), MemoryJournal())) == live
    assert calls == []


async def _after(delay: float, work: Any) -> Any:
    await asyncio.sleep(delay)
    return await work


@pytest.mark.asyncio
async def test_a_failing_activity_with_attempts_left_schedules_a_retry_and_suspends() -> None:
    journal = MemoryJournal()

    with pytest.raises(Suspend) as suspended:
        await WorkflowContext(state(), journal).step(flaky)

    assert journal.retries == {0: (1, timedelta(seconds=2))}
    assert suspended.value.wake_at == T0 + timedelta(seconds=2)
    assert journal.events == {}, "nothing is recorded until the step succeeds or gives up"


@pytest.mark.asyncio
async def test_a_retry_that_is_not_due_yet_suspends_without_running_the_activity() -> None:
    retries = {0: RetryState(1, T0 + timedelta(seconds=5))}

    with pytest.raises(Suspend):
        await WorkflowContext(state(retries=retries), MemoryJournal()).step(flaky)
    assert calls == []


@pytest.mark.asyncio
async def test_the_last_attempt_records_the_failure_and_raises_activity_error() -> None:
    journal = MemoryJournal()
    retries = {0: RetryState(2, T0 - timedelta(seconds=1))}

    with pytest.raises(ActivityError) as failed:
        await WorkflowContext(state(retries=retries), journal).step(flaky)

    assert failed.value.attempts == 3
    assert journal.events[0].payload["error"]["type"] == "ConnectionError"


@pytest.mark.asyncio
async def test_a_non_retryable_error_fails_the_step_on_the_first_attempt() -> None:
    journal = MemoryJournal()

    with pytest.raises(ActivityError, match="Fatal"):
        await WorkflowContext(state(), journal).step(fatal)

    assert calls == ["fatal"]
    assert journal.retries == {}


@pytest.mark.asyncio
async def test_a_recorded_failure_is_raised_again_on_replay() -> None:
    error = {"type": "Fatal", "message": "no", "attempts": 1}
    ctx = WorkflowContext(state({0: Event(0, "step", "fatal", {"error": error})}), MemoryJournal())

    with pytest.raises(ActivityError) as failed:
        await ctx.step(fatal)
    assert (failed.value.error_type, calls) == ("Fatal", [])


@pytest.mark.asyncio
async def test_a_cancel_lands_once_at_the_first_live_call_and_replays_from_history() -> None:
    history = {0: Event(0, "step", "other", {"result": "other"})}
    journal = MemoryJournal()
    ctx = WorkflowContext(state(history, cancel=True), journal)

    assert await ctx.step(other) == "other", "replayed calls are not interrupted"
    with pytest.raises(WorkflowCancelled):
        await ctx.step(other)
    assert await ctx.step(other) == "other", "delivered once; compensation can still run steps"
    assert journal.events[1].kind == "cancelled"

    replay = WorkflowContext(state(history | journal.events, cancel=True), MemoryJournal())
    await replay.step(other)
    with pytest.raises(WorkflowCancelled):
        await replay.step(other)


@pytest.mark.asyncio
async def test_a_sleep_suspends_until_its_deadline_then_records_that_it_fired() -> None:
    journal = MemoryJournal()
    with pytest.raises(Suspend) as suspended:
        await WorkflowContext(state(), journal).sleep(timedelta(minutes=5))
    assert suspended.value.wake_at == T0 + timedelta(minutes=5)

    later = state(journal.events, now=T0 + timedelta(minutes=5))
    await WorkflowContext(later, journal).sleep(timedelta(minutes=5))
    assert journal.events[1].kind == "timer_fired"


@pytest.mark.asyncio
async def test_a_signal_already_waiting_is_taken_without_suspending() -> None:
    journal = MemoryJournal({"approval": [{"approved": True}]})

    payload = await WorkflowContext(state(), journal).wait_for_signal("approval", payload_type=dict)

    assert payload == {"approved": True}
    assert [e.kind for e in journal.events.values()] == ["signal_wait", "signal"]


@pytest.mark.asyncio
async def test_waiting_for_a_signal_suspends_on_its_deadline_and_its_name() -> None:
    with pytest.raises(Suspend) as suspended:
        await WorkflowContext(state(), MemoryJournal()).wait_for_signal("approval", timeout=timedelta(hours=1))
    assert suspended.value.wake_at == T0 + timedelta(hours=1)
    assert suspended.value.signals == {"approval"}


@pytest.mark.asyncio
async def test_a_signal_wait_past_its_deadline_records_a_timeout_and_raises() -> None:
    journal = MemoryJournal()
    with pytest.raises(Suspend):
        await WorkflowContext(state(), journal).wait_for_signal("approval", timeout=timedelta(hours=1))

    with pytest.raises(SignalTimeout):
        await WorkflowContext(state(journal.events, now=T0 + timedelta(hours=2)), journal).wait_for_signal(
            "approval", timeout=timedelta(hours=1)
        )
    assert journal.events[1].kind == "timeout"


@pytest.mark.asyncio
async def test_now_and_uuid_are_stable_across_replays() -> None:
    journal = MemoryJournal()
    ctx = WorkflowContext(state(), journal)
    first = (await ctx.now(), await ctx.uuid())

    journal.now = T0 + timedelta(days=1)
    replay = WorkflowContext(state(journal.events), MemoryJournal())
    assert (await replay.now(), await replay.uuid()) == first


@pytest.mark.asyncio
async def test_a_saga_undoes_completed_steps_newest_first_when_the_block_fails() -> None:
    ctx = WorkflowContext(state(), MemoryJournal())

    with pytest.raises(ActivityError):
        async with Saga(ctx) as saga:
            await ctx.step(other)
            saga.on_failure(undo, "first")
            await ctx.step(other)
            saga.on_failure(undo, "second")
            await ctx.step(fatal)

    assert calls[-2:] == ["undo second", "undo first"]


@pytest.mark.asyncio
async def test_a_saga_does_not_compensate_when_the_workflow_only_suspends() -> None:
    ctx = WorkflowContext(state(), MemoryJournal())

    with pytest.raises(Suspend):
        async with Saga(ctx) as saga:
            saga.on_failure(undo, "first")
            await ctx.sleep(timedelta(minutes=1))

    assert "undo first" not in calls


@pytest.mark.asyncio
async def test_unreached_history_shows_code_that_now_stops_early() -> None:
    history = {0: Event(0, "step", "other", {"result": "other"}), 1: Event(1, "step", "other", {"result": "other"})}
    ctx = WorkflowContext(state(history), MemoryJournal())
    await ctx.step(other)

    assert ctx.unreached_history() == [1]
