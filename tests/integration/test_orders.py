"""The example the README walks through, end to end against Postgres."""

from typing import Any

import pytest

from examples.orders import registry
from ratchet.client import Client
from ratchet.store import Store
from tests.integration.conftest import make_worker, run_to_end


def order(order_id: str, amount: str = "120.00", card: str = "ok") -> dict[str, Any]:
    return {"order_id": order_id, "sku": "KB-01", "quantity": 1, "amount": amount, "card": card}


@pytest.fixture
def client(store: Store) -> Client:
    return Client(store, registry)


async def steps_of(client: Client, workflow_id: str) -> list[str]:
    return [e.name for e, _ in await client.history(workflow_id) if e.kind == "step"]


@pytest.mark.asyncio(loop_scope="session")
async def test_a_small_order_is_reserved_charged_and_shipped(store: Store, client: Client) -> None:
    await client.start("fulfil_order", order("o-1"), workflow_id="o-1")

    done = await run_to_end(make_worker(store, registry), client, "o-1")

    assert done.status == "completed"
    assert done.result == {"order_id": "o-1", "status": "shipped", "charge_id": "ch-o-1:1", "tracking": "TRK-O-1"}
    assert await steps_of(client, "o-1") == ["reserve_stock", "charge_card", "ship"]


@pytest.mark.asyncio(loop_scope="session")
async def test_a_flaky_payment_provider_is_retried_with_the_same_idempotency_key(store: Store, client: Client) -> None:
    await client.start("fulfil_order", order("o-2", card="flaky"), workflow_id="o-2")

    done = await run_to_end(make_worker(store, registry), client, "o-2")

    assert done.status == "completed"
    assert done.result["charge_id"] == "ch-o-2:1", "three attempts, one key"


@pytest.mark.asyncio(loop_scope="session")
async def test_a_declined_card_releases_the_stock_and_fails_the_order(store: Store, client: Client) -> None:
    await client.start("fulfil_order", order("o-3", card="declined"), workflow_id="o-3")

    done = await run_to_end(make_worker(store, registry), client, "o-3")

    assert done.status == "failed"
    assert done.error["activity_error"] == "CardDeclined"
    assert await steps_of(client, "o-3") == ["reserve_stock", "charge_card", "release_stock"]


@pytest.mark.asyncio(loop_scope="session")
async def test_a_large_order_waits_for_approval_then_ships(store: Store, client: Client) -> None:
    await client.start("fulfil_order", order("o-4", amount="2500.00"), workflow_id="o-4")
    worker = make_worker(store, registry)
    await worker.run_once()
    assert (await client.get("o-4")).waiting_signals == ["approval"]

    await client.signal("o-4", "approval", {"approved": True, "by": "finance"})
    done = await run_to_end(worker, client, "o-4")

    assert done.result["status"] == "shipped"


@pytest.mark.asyncio(loop_scope="session")
async def test_a_rejected_large_order_is_refunded_and_released_newest_first(store: Store, client: Client) -> None:
    await client.start("fulfil_order", order("o-5", amount="2500.00"), workflow_id="o-5")
    worker = make_worker(store, registry)
    await worker.run_once()

    await client.signal("o-5", "approval", {"approved": False})
    done = await run_to_end(worker, client, "o-5")

    assert (done.status, done.error["type"]) == ("failed", "OrderRejected")
    assert await steps_of(client, "o-5") == ["reserve_stock", "charge_card", "refund", "release_stock"]
