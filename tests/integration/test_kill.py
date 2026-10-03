"""A real worker process, killed without warning in the middle of an activity, and a second one finishing the job."""

import asyncio
import os
import subprocess
import sys
import time
from datetime import timedelta

import asyncpg
import pytest

from ratchet.client import Client
from ratchet.store import Store
from tests.integration.kill_app import registry

LEASE_SECONDS = 2


def spawn_worker(dsn: str, *, slow: bool) -> subprocess.Popen[bytes]:
    env = os.environ | {
        "RATCHET_DATABASE_URL": dsn,
        "RATCHET_APP": "tests.integration.kill_app:registry",
        "RATCHET_LEASE_SECONDS": str(LEASE_SECONDS),
        "RATCHET_IDLE_WAIT_MAX_SECONDS": "1",
        "RATCHET_LOG_LEVEL": "WARNING",
        "KILL_TEST_DSN": dsn,
        "KILL_TEST_SLOW": "1" if slow else "0",
    }
    return subprocess.Popen([sys.executable, "-m", "ratchet", "worker"], env=env)  # our own interpreter


async def effects(pool: asyncpg.Pool) -> list[tuple[int, str, int]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("select seq, label, pid from kill_effects where workflow_id = 'killed' order by id")
    return [(r["seq"], r["label"], r["pid"]) for r in rows]


@pytest.mark.asyncio(loop_scope="session")
async def test_a_killed_worker_loses_no_recorded_work_and_repeats_only_the_interrupted_step(
    dsn: str, pool: asyncpg.Pool
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "create table if not exists kill_effects (id bigint generated always as identity,"
            " workflow_id text, seq int, label text, pid int)"
        )
        await conn.execute("truncate kill_effects")
    client = Client(Store(pool), registry)
    await client.start("killable", 42, workflow_id="killed")

    doomed = spawn_worker(dsn, slow=True)
    try:
        async with asyncio.timeout(30):
            while not any(label == "shipping started" for _, label, _ in await effects(pool)):  # noqa: ASYNC110
                await asyncio.sleep(0.05)
    finally:
        doomed.kill()  # SIGKILL on Linux, TerminateProcess on Windows: no cleanup code runs
        doomed.wait()
    killed_at = time.monotonic()

    survivor = spawn_worker(dsn, slow=False)
    try:
        done = await client.wait("killed", timeout=timedelta(seconds=30))
    finally:
        survivor.terminate()
        survivor.wait()

    assert (done.status, done.result) == ("completed", ["charge-42", "shipped"])
    assert time.monotonic() - killed_at >= LEASE_SECONDS * 0.9, "the takeover waited for the lease to run out"
    seen = await effects(pool)
    assert [(seq, label) for seq, label, _ in seen] == [
        (0, "charged"),  # once: it was recorded before the kill
        (1, "shipping started"),  # the killed attempt
        (1, "shipping started"),  # the step that was in flight runs again
        (1, "shipped"),
    ]
    # Process ids, not Popen.pid: on Windows a venv's python.exe is a launcher with the interpreter as its child.
    killed_pid, survivor_pid = seen[1][2], seen[2][2]
    assert killed_pid != survivor_pid
    assert seen[3][2] == survivor_pid
