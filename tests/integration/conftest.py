import asyncio
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.community.postgres import PostgresContainer

from ratchet.client import Client
from ratchet.definitions import Registry
from ratchet.migrate import migrate
from ratchet.store import FINAL, Store, WorkflowRecord, create_pool
from ratchet.worker import Worker

IMAGE = "postgres:18.6-alpine"


@pytest.fixture(scope="session")
def dsn() -> Iterator[str]:
    with PostgresContainer(IMAGE, driver=None) as container:
        yield container.get_connection_url()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def pool(dsn: str) -> AsyncIterator[asyncpg.Pool]:
    created = await create_pool(dsn, max_size=40)
    async with created.acquire() as conn:
        await migrate(conn)
    yield created
    await created.close()


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clean(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("truncate ratchet_workflows cascade")


@pytest.fixture
def store(pool: asyncpg.Pool) -> Store:
    return Store(pool)


def make_worker(store: Store, registry: Registry, name: str = "w1", **options: object) -> Worker:
    defaults: dict[str, object] = {"concurrency": 16, "lease": timedelta(seconds=30)}
    return Worker(store, registry, worker_id=name, **(defaults | options))  # type: ignore[arg-type]


async def run_to_end(worker: Worker, client: Client, workflow_id: str, timeout: float = 10.0) -> WorkflowRecord:
    """Drive a worker one pass at a time until the workflow is final. Sleeps between passes so timers can come due."""
    async with asyncio.timeout(timeout):
        while True:
            await worker.run_once()
            record = await client.get(workflow_id)
            if record.status in FINAL:
                return record
            await asyncio.sleep(0.02)


async def expire_lease(pool: asyncpg.Pool, workflow_id: str) -> None:
    """What waiting out the lease looks like, without the wait."""
    async with pool.acquire() as conn:
        await conn.execute("update ratchet_workflows set due_at = now() - interval '1 ms' where id = $1", workflow_id)
