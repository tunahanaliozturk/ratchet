"""Cases an outside review found. Each test failed against the code before its fix."""

import asyncio
from datetime import timedelta

import asyncpg
import pytest

from ratchet import Registry, RetryPolicy, Saga, WorkflowContext
from ratchet.client import Client
from ratchet.store import Store
from tests.integration.conftest import make_worker, run_to_end

registry = Registry()
calls: list[str] = []


@registry.activity()
async def unordered() -> dict[str, int]:
    return {"zebra": 1, "apple": 2}


@registry.activity()
async def echo(value: str) -> str:
    calls.append(f"echo {value}")
    return value


@registry.activity(retry=RetryPolicy(max_attempts=3))
async def nul() -> str:
    calls.append("nul")
    return "a\x00b"


@registry.activity()
async def slow(label: str) -> str:
    calls.append(f"{label} start")
    await asyncio.sleep(0.3)
    calls.append(f"{label} end")
    return label


@registry.activity()
async def undo(label: str) -> None:
    calls.append(f"undo {label}")


@registry.workflow()
async def key_order(ctx: WorkflowContext, _: None) -> list[str]:
    found = await ctx.step(unordered)
    keys = list(found)
    first = await ctx.step(echo, keys[0])
    await ctx.sleep(timedelta(milliseconds=50))
    second = await ctx.step(echo, keys[1])
    return [first, second]


@registry.workflow()
async def stores_nul(ctx: WorkflowContext, _: None) -> str:
    return await ctx.step(nul)


@registry.workflow()
async def parallel_wait(ctx: WorkflowContext, _: None) -> list[object]:
    async with asyncio.TaskGroup() as group:
        work = group.create_task(ctx.step(slow, "work"))
        signal = group.create_task(ctx.wait_for_signal("go", payload_type=str))
    return [work.result(), signal.result()]


@registry.workflow()
async def cancel_beside_timer(ctx: WorkflowContext, _: None) -> None:
    async with Saga(ctx) as saga:
        await ctx.step(echo, "booked")
        saga.on_failure(undo, "booked")
        async with asyncio.TaskGroup() as group:
            group.create_task(ctx.sleep(timedelta(hours=1)))
            group.create_task(ctx.wait_for_signal("never"))


@registry.workflow()
async def typed_signal(ctx: WorkflowContext, _: None) -> dict[str, bool]:
    return await ctx.wait_for_signal("decision", payload_type=dict)


@pytest.fixture(autouse=True)
def _reset() -> None:
    calls.clear()


@pytest.fixture
def client(store: Store) -> Client:
    return Client(store, registry)


@pytest.mark.asyncio(loop_scope="session")
async def test_a_dict_result_has_the_same_key_order_live_and_on_replay(store: Store, client: Client) -> None:
    await client.start("key_order", None, workflow_id="key-order")

    done = await run_to_end(make_worker(store, registry), client, "key-order")

    # Live and replay must agree. Before the fix the live run saw zebra first and the replay apple first, so the
    # workflow echoed "zebra" twice and never processed "apple".
    assert sorted(done.result) == ["apple", "zebra"]
    assert len(set(done.result)) == 2


@pytest.mark.asyncio(loop_scope="session")
async def test_a_value_postgres_refuses_fails_the_workflow_once_instead_of_looping(
    store: Store, client: Client
) -> None:
    await client.start("stores_nul", None, workflow_id="nul")

    done = await run_to_end(make_worker(store, registry), client, "nul")

    assert done.status == "failed"
    assert done.error["type"] == "UnstorableValue"
    assert calls == ["nul"], "ran once; not again on every lease"


@pytest.mark.asyncio(loop_scope="session")
async def test_a_parked_branch_does_not_cut_off_a_sibling_activity(store: Store, client: Client) -> None:
    await client.start("parallel_wait", None, workflow_id="parallel-wait")
    worker = make_worker(store, registry)
    await worker.run_once()

    assert calls == ["work start", "work end"], "the activity finished before the run parked"
    parked = await client.get("parallel-wait")
    assert parked.waiting_signals == ["go"]

    await client.signal("parallel-wait", "go", "now")
    done = await run_to_end(worker, client, "parallel-wait")
    assert done.result == ["work", "now"]
    assert calls == ["work start", "work end"], "and it never ran again"


@pytest.mark.asyncio(loop_scope="session")
async def test_a_cancel_next_to_a_long_timer_lands_at_once(store: Store, client: Client) -> None:
    await client.start("cancel_beside_timer", None, workflow_id="cancel-timer")
    worker = make_worker(store, registry)
    await worker.run_once()
    assert (await client.get("cancel-timer")).status == "sleeping"

    await client.cancel("cancel-timer")
    done = await run_to_end(worker, client, "cancel-timer", timeout=5)

    assert done.status == "cancelled"
    assert calls == ["echo booked", "undo booked"]


@pytest.mark.asyncio(loop_scope="session")
async def test_a_malformed_signal_is_set_aside_and_the_wait_goes_on(
    pool: asyncpg.Pool, store: Store, client: Client
) -> None:
    await client.start("typed_signal", None, workflow_id="typed")
    worker = make_worker(store, registry)
    await worker.run_once()

    await client.signal("typed", "decision", ["not", "a", "dict"])
    await worker.run_once()
    assert (await client.get("typed")).status == "sleeping", "still waiting, not failed"

    await client.signal("typed", "decision", {"approved": True})
    done = await run_to_end(worker, client, "typed")
    assert done.result == {"approved": True}
    async with pool.acquire() as conn:
        rejected = await conn.fetchval(
            "select count(*) from ratchet_signals where workflow_id = 'typed' and rejected_at is not null"
        )
    assert rejected == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_a_result_that_cannot_be_serialised_fails_the_workflow(store: Store) -> None:
    odd = Registry()

    @odd.workflow("unserialisable")
    async def unserialisable(ctx: WorkflowContext, _: None) -> object:
        return object()

    client = Client(store, odd)
    await client.start("unserialisable", None, workflow_id="odd")
    done = await run_to_end(make_worker(store, odd), client, "odd")

    assert done.status == "failed"
    assert done.error["type"] == "PydanticSerializationError"


@pytest.mark.asyncio(loop_scope="session")
async def test_changing_a_workflow_input_type_stalls_running_instances(store: Store) -> None:
    before, after = Registry(), Registry()

    @before.workflow("retyped")
    async def v1(ctx: WorkflowContext, count: int) -> int:
        await ctx.sleep(timedelta(milliseconds=30))
        return count

    @after.workflow("retyped")
    async def v2(ctx: WorkflowContext, names: list[str]) -> int:
        await ctx.sleep(timedelta(milliseconds=30))
        return len(names)

    client = Client(store, before)
    await client.start("retyped", 3, workflow_id="retyped")
    await make_worker(store, before).run_once()
    await asyncio.sleep(0.05)
    await make_worker(store, after).run_once()

    stalled = await client.get("retyped")
    assert (stalled.status, stalled.error["type"]) == ("sleeping", "ValidationError")
