"""Edge cases an outside review found, that need no database. Each failed against the code before its fix."""

import asyncio
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest

from ratchet.api import build_app
from ratchet.context import RunState, WorkflowContext
from ratchet.errors import HistoryTooLong, JournalUnavailable
from ratchet.store import RunJournal, Store
from ratchet.worker import Worker
from tests.unit.test_context import MemoryJournal, other

ISTANBUL = timezone(timedelta(hours=3))


class IstanbulJournal(MemoryJournal):
    """A database whose session time zone is not UTC."""

    async def append_now(self, seq: int) -> datetime:
        local = self.now.astimezone(ISTANBUL)
        self._put(seq, "now", "now", local.isoformat())
        return local


@pytest.mark.asyncio
async def test_now_is_utc_on_the_live_run_and_on_replay_whatever_the_database_time_zone() -> None:
    journal = IstanbulJournal()
    live = await WorkflowContext(RunState("w", {}, {}, journal.now, False), journal).now()
    replayed = await WorkflowContext(RunState("w", journal.events, {}, journal.now, False), journal).now()

    assert live.isoformat() == replayed.isoformat()
    assert live.utcoffset() == timedelta(0)


@pytest.mark.asyncio
async def test_a_workflow_that_keeps_recording_is_stopped_at_the_history_limit() -> None:
    journal = MemoryJournal()
    ctx = WorkflowContext(RunState("w", {}, {}, datetime.now(UTC), False), journal, max_history=3)

    for _ in range(3):
        await ctx.step(other)
    with pytest.raises(HistoryTooLong):
        _ = ctx.step(other)


class FlakyHeartbeat:
    def __init__(self) -> None:
        self.beats = 0

    async def heartbeat(self) -> bool:
        self.beats += 1
        if self.beats == 1:
            raise JournalUnavailable("connection reset")
        return False  # the second beat finds the lease gone


@pytest.mark.asyncio
async def test_one_failed_heartbeat_does_not_stop_the_heartbeats() -> None:
    worker = Worker(cast("Store", None), cast("Any", None), worker_id="w", lease=timedelta(seconds=1))
    victim = asyncio.create_task(asyncio.sleep(10))
    lost = asyncio.Event()
    journal = FlakyHeartbeat()

    await asyncio.wait_for(worker._heartbeat(cast("RunJournal", journal), victim, lost), timeout=3)

    assert journal.beats == 2
    assert lost.is_set()
    await asyncio.sleep(0)
    assert victim.cancelled()


@pytest.mark.parametrize("token", ["", "short"])
def test_the_api_refuses_to_start_with_an_empty_or_short_token(token: str) -> None:
    with pytest.raises(ValueError, match="at least 16"):
        build_app(token, max_payload_bytes=4096)
