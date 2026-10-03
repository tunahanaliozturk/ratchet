"""The HTTP surface: auth, status codes, problem bodies, pagination and the body limit."""

from collections.abc import AsyncIterator

import asyncpg
import httpx
import pytest
import pytest_asyncio

from examples.orders import registry
from ratchet.api import build_app
from ratchet.client import Client
from ratchet.store import Store
from tests.integration.conftest import make_worker

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


def order(order_id: str, amount: str = "10.00") -> dict[str, object]:
    return {"order_id": order_id, "sku": "KB-01", "quantity": 1, "amount": amount}


@pytest_asyncio.fixture(loop_scope="session")
async def http(pool: asyncpg.Pool) -> AsyncIterator[httpx.AsyncClient]:
    app = build_app(TOKEN, max_payload_bytes=4096)
    app.state.client = Client(Store(pool), registry)

    async def ready() -> None:
        async with pool.acquire() as conn:
            await conn.fetchval("select 1")

    app.state.ready = ready
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://ratchet.test") as client:
        yield client


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer wrong"}, {"Authorization": "Basic abc"}])
async def test_every_workflow_route_refuses_a_missing_or_wrong_token(
    http: httpx.AsyncClient, headers: dict[str, str]
) -> None:
    for method, path in [("GET", "/v1/workflows"), ("GET", "/v1/workflows/x"), ("POST", "/v1/workflows/x/cancel")]:
        response = await http.request(method, path, headers=headers)
        assert response.status_code == 401, path
        assert response.headers["www-authenticate"] == "Bearer"
        assert response.headers["content-type"] == "application/problem+json"


@pytest.mark.asyncio(loop_scope="session")
async def test_start_is_201_then_200_for_the_same_request_and_409_for_a_different_one(
    http: httpx.AsyncClient,
) -> None:
    body = {"name": "fulfil_order", "id": "api-1", "input": order("api-1")}

    first = await http.post("/v1/workflows", json=body, headers=AUTH)
    again = await http.post("/v1/workflows", json=body, headers=AUTH)
    clash = await http.post("/v1/workflows", json=body | {"input": order("api-1", "99.00")}, headers=AUTH)

    assert (first.status_code, again.status_code, clash.status_code) == (201, 200, 409)
    assert first.headers["location"] == "/v1/workflows/api-1"
    assert first.json()["status"] == "ready"


@pytest.mark.asyncio(loop_scope="session")
async def test_an_invalid_input_is_refused_before_anything_is_stored(http: httpx.AsyncClient) -> None:
    bad = await http.post(
        "/v1/workflows", json={"name": "fulfil_order", "id": "bad", "input": {"order_id": "x"}}, headers=AUTH
    )
    unknown = await http.post("/v1/workflows", json={"name": "nope", "input": None}, headers=AUTH)

    assert (bad.status_code, unknown.status_code) == (422, 422)
    assert (await http.get("/v1/workflows/bad", headers=AUTH)).status_code == 404


@pytest.mark.asyncio(loop_scope="session")
async def test_a_signal_and_a_cancel_reach_the_workflow_and_a_finished_one_refuses_both(
    http: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    await http.post(
        "/v1/workflows", json={"name": "fulfil_order", "id": "big", "input": order("big", "5000.00")}, headers=AUTH
    )
    sent = await http.post("/v1/workflows/big/signals/approval", json={"payload": {"approved": True}}, headers=AUTH)
    assert (sent.status_code, sent.json()) == (202, {"delivered": True})

    worker = make_worker(Store(pool), registry)
    await worker.run_once()
    assert (await http.get("/v1/workflows/big", headers=AUTH)).json()["status"] == "completed"
    history = (await http.get("/v1/workflows/big/history", headers=AUTH)).json()
    assert [e["kind"] for e in history] == ["step", "step", "signal_wait", "signal", "step"]

    late = await http.post("/v1/workflows/big/signals/approval", json={}, headers=AUTH)
    cancel = await http.post("/v1/workflows/big/cancel", headers=AUTH)
    assert (late.status_code, cancel.status_code) == (409, 409)


@pytest.mark.asyncio(loop_scope="session")
async def test_listing_pages_by_cursor_without_gaps_or_repeats(http: httpx.AsyncClient) -> None:
    for i in range(5):
        body = {"name": "fulfil_order", "id": f"page-{i}", "input": order(f"page-{i}")}
        await http.post("/v1/workflows", json=body, headers=AUTH)

    seen: list[str] = []
    after: str | None = None
    while True:
        params = {"limit": 2} | ({"after": after} if after else {})
        page = (await http.get("/v1/workflows", params=params, headers=AUTH)).json()
        seen += [w["id"] for w in page["items"]]
        if (after := page["next"]) is None:
            break

    assert seen == [f"page-{i}" for i in range(5)]
    bad = await http.get("/v1/workflows", params={"after": "!!"}, headers=AUTH)
    assert bad.status_code == 400


@pytest.mark.asyncio(loop_scope="session")
async def test_a_body_over_the_limit_is_refused_with_or_without_a_content_length(http: httpx.AsyncClient) -> None:
    huge = {"name": "fulfil_order", "input": {"blob": "x" * 5000}}
    declared = await http.post("/v1/workflows", json=huge, headers=AUTH)

    async def chunks() -> AsyncIterator[bytes]:
        for _ in range(10):
            yield b"x" * 1000

    streamed = await http.post("/v1/workflows", content=chunks(), headers=AUTH | {"content-type": "application/json"})

    assert (declared.status_code, streamed.status_code) == (413, 413)


@pytest.mark.asyncio(loop_scope="session")
async def test_health_and_readiness_need_no_token(http: httpx.AsyncClient) -> None:
    assert (await http.get("/healthz")).status_code == 200
    assert (await http.get("/readyz")).json() == {"status": "ready"}
