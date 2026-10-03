"""The engine against a real Postgres: what survives, what is refused, and who wins a race."""

import asyncio
import time
from datetime import timedelta

import asyncpg
import pytest

from ratchet import ActivityError, RetryPolicy, Saga, WorkflowContext, activity_info
from ratchet.client import Client
from ratchet.definitions import Registry
from ratchet.errors import LeaseLost, WorkflowFinished, WorkflowIdConflict
from ratchet.store import Store, create_pool
from tests.integration.conftest import expire_lease, make_worker, run_to_end

registry = Registry()
ran: list[str] = []


@registry.activity()
async def add(a: int, b: int) -> int:
    ran.append(f"add {a} {b}")
    return a + b


@registry.activity(retry=RetryPolicy(max_attempts=4, initial_delay=timedelta(milliseconds=10), jitter=0))
async def flaky_twice() -> int:
    attempt = activity_info().attempt
    ran.append(f"flaky {attempt}")
    if attempt < 3:
        raise ConnectionError("not yet")
    return attempt


@registry.activity()
async def note(label: str) -> None:
    ran.append(label)


@registry.workflow()
async def sums(ctx: WorkflowContext, n: int) -> int:
    total = 0
    for i in range(n):
        total = await ctx.step(add, total, i)
    return total


@registry.workflow()
async def nap(ctx: WorkflowContext, ms: int) -> str:
    await ctx.sleep(timedelta(milliseconds=ms))
    await ctx.step(note, "woke")
    return "rested"


@registry.workflow()
async def retrying(ctx: WorkflowContext, _: None) -> int:
    return await ctx.step(flaky_twice)


@registry.workflow()
async def approval(ctx: WorkflowContext, _: None) -> object:
    return await ctx.wait_for_signal("decision", timeout=timedelta(minutes=5))


@registry.workflow()
async def cancellable(ctx: WorkflowContext, _: None) -> None:
    async with Saga(ctx) as saga:
        await ctx.step(note, "booked")
        saga.on_failure(note, "unbooked")
        await ctx.sleep(timedelta(hours=1))


@registry.workflow()
async def fan_out(ctx: WorkflowContext, n: int) -> list[int]:
    async with asyncio.TaskGroup() as group:
        tasks = [group.create_task(ctx.step(add, i, i)) for i in range(n)]
    return [t.result() for t in tasks]


@pytest.fixture(autouse=True)
def _reset() -> None:
    ran.clear()


@pytest.fixture
def client(store: Store) -> Client:
    return Client(store, registry)


@pytest.mark.asyncio(loop_scope="session")
async def test_a_workflow_runs_to_completion_and_records_each_step_once(store: Store, client: Client) -> None:
    record, created = await client.start("sums", 4, workflow_id="sums-1")
    assert created and record.status == "ready"

    done = await run_to_end(make_worker(store, registry), client, "sums-1")

    assert (done.status, done.result) == ("completed", 6)
    history = await client.history("sums-1")
    assert [(e.seq, e.kind, e.name) for e, _ in history] == [(i, "step", "add") for i in range(4)]
    assert ran == ["add 0 0", "add 0 1", "add 1 2", "add 3 3"]


@pytest.mark.asyncio(loop_scope="session")
async def test_starting_twice_with_one_id_is_idempotent_and_a_different_input_is_refused(client: Client) -> None:
    first, created = await client.start("sums", 3, workflow_id="same")
    again, created_again = await client.start("sums", 3, workflow_id="same")

    assert created and not created_again
    assert again.id == first.id
    with pytest.raises(WorkflowIdConflict):
        await client.start("sums", 4, workflow_id="same")


@pytest.mark.asyncio(loop_scope="session")
async def test_a_sleeping_workflow_holds_no_lease_and_resumes_when_its_timer_fires(
    store: Store, client: Client
) -> None:
    await client.start("nap", 300, workflow_id="nap")
    worker = make_worker(store, registry)

    await worker.run_once()
    parked = await client.get("nap")
    assert parked.status == "sleeping"
    assert parked.due_at is not None
    assert await worker.run_once() == 0, "nothing is due while the timer runs"

    done = await run_to_end(worker, client, "nap")
    assert (done.status, ran) == ("completed", ["woke"])
    assert done.finished_at is not None
    assert done.finished_at - parked.updated_at >= timedelta(milliseconds=300)


@pytest.mark.asyncio(loop_scope="session")
async def test_a_failing_activity_is_retried_durably_until_it_succeeds(store: Store, client: Client) -> None:
    await client.start("retrying", None, workflow_id="retry")

    done = await run_to_end(make_worker(store, registry), client, "retry")

    assert (done.status, done.result) == ("completed", 3)
    assert ran == ["flaky 1", "flaky 2", "flaky 3"]
    assert [e.kind for e, _ in await client.history("retry")] == ["step"], "only the outcome is history"


@pytest.mark.asyncio(loop_scope="session")
async def test_a_signal_sent_before_the_wait_is_kept_and_a_duplicate_is_dropped(store: Store, client: Client) -> None:
    await client.start("approval", None, workflow_id="early")

    assert await client.signal("early", "decision", {"ok": True}, dedupe_key="d-1")
    assert not await client.signal("early", "decision", {"ok": False}, dedupe_key="d-1")

    done = await run_to_end(make_worker(store, registry), client, "early")
    assert done.result == {"ok": True}


@pytest.mark.asyncio(loop_scope="session")
async def test_a_signal_wakes_a_workflow_that_is_parked_waiting_for_it(store: Store, client: Client) -> None:
    await client.start("approval", None, workflow_id="parked")
    worker = make_worker(store, registry)
    await worker.run_once()
    parked = await client.get("parked")
    assert (parked.status, parked.waiting_signals) == ("sleeping", ["decision"])

    await client.signal("parked", "decision", "yes")

    assert await worker.run_once() == 1, "due immediately, not at the five-minute deadline"
    assert (await client.get("parked")).result == "yes"


@pytest.mark.asyncio(loop_scope="session")
async def test_a_signal_landing_between_the_check_and_the_park_is_not_lost(store: Store, client: Client) -> None:
    await client.start("approval", None, workflow_id="race")
    [claim] = await store.claim("w1", timedelta(seconds=30), 1)
    journal = store.journal(claim, timedelta(seconds=30))
    await journal.load()
    await journal.append_deadline(0, "signal_wait", "decision", timedelta(minutes=5))
    assert await journal.take_signal(1, "decision", lambda _: True) == (False, None)

    await client.signal("race", "decision", "late")  # after the check, before the park

    assert not await journal.suspend(None, frozenset({"decision"})), "parking now would sleep through the signal"
    state = await journal.load()
    assert state.history.keys() == {0}
    assert await journal.take_signal(1, "decision", lambda _: True) == (True, "late")


@pytest.mark.asyncio(loop_scope="session")
async def test_a_cancel_runs_the_compensations_and_ends_cancelled(store: Store, client: Client) -> None:
    await client.start("cancellable", None, workflow_id="cancel-me")
    worker = make_worker(store, registry)
    await worker.run_once()
    assert (await client.get("cancel-me")).status == "sleeping"

    await client.cancel("cancel-me")
    done = await run_to_end(worker, client, "cancel-me")

    assert done.status == "cancelled"
    assert ran == ["booked", "unbooked"]
    with pytest.raises(WorkflowFinished):
        await client.cancel("cancel-me")


@pytest.mark.asyncio(loop_scope="session")
async def test_parallel_branches_each_run_once_and_return_in_call_order(store: Store, client: Client) -> None:
    await client.start("fan_out", 5, workflow_id="fan")

    done = await run_to_end(make_worker(store, registry), client, "fan")

    assert done.result == [0, 2, 4, 6, 8]
    assert sorted(ran) == sorted(f"add {i} {i}" for i in range(5))


@pytest.mark.asyncio(loop_scope="session")
async def test_a_worker_whose_lease_was_taken_over_cannot_record_anything(
    pool: asyncpg.Pool, store: Store, client: Client
) -> None:
    gate = asyncio.Event()
    slow = Registry()

    @slow.activity()
    async def wait_for_gate() -> str:
        await gate.wait()
        return activity_info().idempotency_key

    @slow.workflow("gated")
    async def gated(ctx: WorkflowContext, _: None) -> str:
        return await ctx.step(wait_for_gate)

    await Client(store, slow).start("gated", None, workflow_id="gated")
    zombie = make_worker(store, slow, "zombie", lease=timedelta(hours=1))  # heartbeat far away: it never notices
    stalled = asyncio.create_task(zombie.run_once())
    await asyncio.sleep(0.2)  # the zombie is inside the activity now

    await expire_lease(pool, "gated")
    gate.set()  # both runs of the activity may now finish; only one may record
    await make_worker(store, slow, "rescuer").run_once()
    await stalled

    done = await client.get("gated")
    assert (done.status, done.result) == ("completed", "gated:0")
    async with pool.acquire() as conn:
        assert await conn.fetchval("select count(*) from ratchet_events where workflow_id = 'gated'") == 1
        assert await conn.fetchval("select fence from ratchet_workflows where id = 'gated'") == 2


@pytest.mark.asyncio(loop_scope="session")
async def test_every_fenced_write_refuses_a_stale_fence(pool: asyncpg.Pool, store: Store, client: Client) -> None:
    await client.start("sums", 1, workflow_id="fenced")
    [old] = await store.claim("old", timedelta(seconds=30), 1)
    await expire_lease(pool, "fenced")
    [new] = await store.claim("new", timedelta(seconds=30), 1)
    stale = store.journal(old, timedelta(seconds=30))

    assert new.fence == old.fence + 1
    for write in (
        stale.append(0, "step", "add", {"result": 1}),
        stale.append_now(0),
        stale.append_deadline(0, "timer", "sleep", timedelta(seconds=1)),
        stale.take_signal(0, "x", lambda _: True),
        stale.schedule_retry(0, 1, timedelta(seconds=1), {}),
        stale.finish("completed", 1, None),
        stale.suspend(None, frozenset()),
    ):
        with pytest.raises(LeaseLost):
            await write
    assert not await stale.heartbeat()


@pytest.mark.asyncio(loop_scope="session")
async def test_changed_code_stalls_the_workflow_instead_of_failing_it_and_a_fix_resumes_it(store: Store) -> None:
    before, broken, fixed = Registry(), Registry(), Registry()
    for r in (before, broken, fixed):
        r.add_activity(add)
        r.add_activity(note)

    @before.workflow("evolving")
    async def v1(ctx: WorkflowContext, _: None) -> str:
        await ctx.step(add, 1, 1)
        await ctx.sleep(timedelta(milliseconds=50))
        return "v1"

    @broken.workflow("evolving")
    async def v2(ctx: WorkflowContext, _: None) -> str:
        await ctx.step(note, "a new first step")  # position 0 held an add
        return "v2"

    @fixed.workflow("evolving")
    async def v3(ctx: WorkflowContext, _: None) -> str:
        await ctx.step(add, 1, 1)
        await ctx.sleep(timedelta(milliseconds=50))
        await ctx.step(note, "new, but only after the recorded part")
        return "v3"

    client = Client(store, before)
    await client.start("evolving", None, workflow_id="evolving")
    await make_worker(store, before).run_once()

    await asyncio.sleep(0.06)
    await make_worker(store, broken).run_once()
    stalled = await client.get("evolving")
    assert stalled.status == "sleeping"
    assert stalled.error is not None
    assert stalled.error["type"] == "NonDeterminismError"

    async with store.pool.acquire() as conn:  # instead of waiting out the stall delay
        await conn.execute("update ratchet_workflows set due_at = now() where id = 'evolving'")
    done = await run_to_end(make_worker(store, fixed), client, "evolving")
    assert (done.status, done.result, done.error) == ("completed", "v3", None)


@pytest.mark.asyncio(loop_scope="session")
async def test_an_activity_that_gives_up_fails_the_workflow_with_the_activity_named(store: Store) -> None:
    failing = Registry()

    @failing.activity(retry=RetryPolicy(max_attempts=1))
    async def boom() -> None:
        raise RuntimeError("kaput")

    @failing.workflow("explodes")
    async def explodes(ctx: WorkflowContext, _: None) -> None:
        await ctx.step(boom)

    client = Client(store, failing)
    await client.start("explodes", None, workflow_id="explodes")
    done = await run_to_end(make_worker(store, failing), client, "explodes")

    assert done.status == "failed"
    assert done.error["type"] == ActivityError.__name__
    assert (done.error["activity"], done.error["activity_error"]) == ("boom", "RuntimeError")


@pytest.mark.asyncio(loop_scope="session")
async def test_an_idle_worker_is_woken_by_notify_not_by_its_poll(store: Store, client: Client) -> None:
    stop = asyncio.Event()
    worker = make_worker(store, registry, idle_wait_max=timedelta(seconds=30))
    running = asyncio.create_task(worker.run(stop))
    await asyncio.sleep(0.3)  # idle, waiting up to 30 seconds

    started = time.perf_counter()
    await client.start("sums", 2, workflow_id="notified")
    done = await client.wait("notified", timeout=timedelta(seconds=5))
    elapsed = time.perf_counter() - started

    stop.set()
    await running
    assert done.status == "completed"
    assert elapsed < 2, f"took {elapsed:.2f}s; the notify did not wake the worker"


@pytest.mark.asyncio(loop_scope="session")
async def test_a_stopping_worker_hands_back_what_it_could_not_finish(store: Store, client: Client) -> None:
    hold = Registry()
    release = asyncio.Event()

    @hold.activity()
    async def forever() -> None:
        await release.wait()

    @hold.workflow("held")
    async def held(ctx: WorkflowContext, _: None) -> None:
        await ctx.step(forever)

    await Client(store, hold).start("held", None, workflow_id="held")
    stop = asyncio.Event()
    worker = make_worker(store, hold, shutdown_grace=timedelta(milliseconds=100))
    running = asyncio.create_task(worker.run(stop))
    await asyncio.sleep(0.3)
    assert (await client.get("held")).status == "running"

    stop.set()
    await running
    released = await client.get("held")
    assert released.status == "ready", "handed back now, not after the 30 second lease"
    release.set()


@pytest.mark.asyncio(loop_scope="session")
async def test_losing_the_database_mid_step_leaves_the_workflow_for_another_run_instead_of_failing_it(
    dsn: str, pool: asyncpg.Pool, store: Store
) -> None:
    doomed_pool = await create_pool(dsn, max_size=3)
    outage = Registry()

    @outage.activity()
    async def cut_the_cable() -> str:
        doomed_pool.terminate()  # the worker's own connections are gone by the time it records this step
        return "done"

    @outage.workflow("outage")
    async def survives(ctx: WorkflowContext, _: None) -> str:
        return await ctx.step(cut_the_cable)

    client = Client(store, outage)
    await client.start("outage", None, workflow_id="outage")
    await make_worker(Store(doomed_pool), outage, "unlucky").run_once()

    stranded = await client.get("outage")
    assert (stranded.status, stranded.error) == ("running", None), "an outage is not the workflow's failure"

    await expire_lease(pool, "outage")
    done = await run_to_end(make_worker(store, outage, "next"), client, "outage")
    assert (done.status, done.result) == ("completed", "done")
