"""For any set of crash points, the workflow finishes with the same result and the same history as a run with none.

A crash here is a ``BaseException`` raised from inside an activity, which unwinds the worker without writing anything
more, the way a killed process would. It fires either before the activity's side effect or after it (and before the
outcome is recorded), which is the window that makes activities at-least-once. The test then lets the lease run out
and hands the workflow to a fresh worker, as many times as there are crashes.

tests/integration/test_kill.py does the same with a real process and a real SIGKILL, once.
"""

import asyncio
import uuid
from collections import Counter
from datetime import timedelta

import asyncpg
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ratchet import WorkflowContext, activity_info
from ratchet.client import Client
from ratchet.definitions import Registry
from ratchet.store import Store
from tests.integration.conftest import expire_lease, make_worker

registry = Registry()
effects: Counter[tuple[str, int]] = Counter()
crash_at: dict[str, set[tuple[int, str]]] = {}


class Crash(BaseException):
    """The process died. Nothing below the worker catches this."""


def _maybe_crash(workflow_id: str, seq: int, phase: str) -> None:
    pending = crash_at.get(workflow_id, set())
    if (seq, phase) in pending:
        pending.discard((seq, phase))  # a crash point fires once; the rerun gets past it
        raise Crash


@registry.activity()
async def effect(value: int) -> int:
    info = activity_info()
    _maybe_crash(info.workflow_id, info.seq, "before")
    effects[info.workflow_id, info.seq] += 1
    _maybe_crash(info.workflow_id, info.seq, "after")
    return value * 10


@registry.workflow("crashy")
async def crashy(ctx: WorkflowContext, steps: int) -> list[int]:
    results = [await ctx.step(effect, i) for i in range(steps)]
    await ctx.sleep(timedelta(0))
    async with asyncio.TaskGroup() as group:
        branches = [group.create_task(ctx.step(effect, 100 + i)) for i in range(2)]
    return results + [b.result() for b in branches]


def expected(steps: int) -> list[int]:
    return [i * 10 for i in range(steps)] + [1000, 1010]


def positions(steps: int) -> list[int]:
    """Where the activities sit in the history: the sequential steps, the timer's two positions, then the branches."""
    return [*range(steps), steps + 2, steps + 3]


@st.composite
def scenarios(draw: st.DrawFn) -> tuple[int, set[tuple[int, str]]]:
    steps = draw(st.integers(min_value=1, max_value=5))
    points = st.tuples(st.sampled_from(positions(steps)), st.sampled_from(["before", "after"]))
    return steps, draw(st.sets(points, max_size=4))


@pytest.mark.asyncio(loop_scope="session")
@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(scenario=scenarios())
async def test_any_crash_schedule_ends_with_the_uninterrupted_result_and_history(
    scenario: tuple[int, set[tuple[int, str]]], pool: asyncpg.Pool
) -> None:
    steps, crashes = scenario
    store = Store(pool)
    client = Client(store, registry)
    workflow_id = f"crashy-{uuid.uuid4()}"
    crash_at[workflow_id] = set(crashes)
    await client.start("crashy", steps, workflow_id=workflow_id)

    deaths = 0
    async with asyncio.timeout(20):
        while (record := await client.get(workflow_id)).status not in ("completed", "failed"):
            try:
                await make_worker(store, registry, f"worker-{deaths}").run_once()
            except* Crash:
                deaths += 1
                await expire_lease(pool, workflow_id)
            await asyncio.sleep(0.005)

    assert (record.status, record.result) == ("completed", expected(steps))
    # Two parallel branches can die in the same run, so one death may use up two crash points.
    assert (deaths == 0) == (not crashes)
    assert deaths <= len(crashes)

    history = await client.history(workflow_id)
    recorded_steps = [e.seq for e, _ in history if e.kind == "step"]
    assert recorded_steps == positions(steps), "every step recorded exactly once, at its own position"

    crashed_after = {seq for seq, phase in crashes if phase == "after"}
    for seq in range(steps):
        # Sequential steps: at-least-once, and exactly the one extra run a crash after the effect explains.
        assert effects[workflow_id, seq] == (2 if seq in crashed_after else 1), f"position {seq}"
    for seq in positions(steps)[steps:]:
        # Parallel branches: a crash in one branch cancels its sibling, which may already have done its effect.
        assert 1 <= effects[workflow_id, seq] <= 1 + len(crashes), f"branch at position {seq}"
