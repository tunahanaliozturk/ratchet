"""The workflow tests/integration/test_kill.py runs in a separate worker process, so that process can be killed."""

import asyncio
import os

import asyncpg

from ratchet import Registry, WorkflowContext, activity_info

registry = Registry()


async def _mark(label: str) -> None:
    info = activity_info()
    conn = await asyncpg.connect(os.environ["KILL_TEST_DSN"])
    try:
        await conn.execute(
            "insert into kill_effects (workflow_id, seq, label, pid) values ($1, $2, $3, $4)",
            info.workflow_id,
            info.seq,
            label,
            os.getpid(),
        )
    finally:
        await conn.close()


@registry.activity()
async def charge(amount: int) -> str:
    await _mark("charged")
    return f"charge-{amount}"


@registry.activity()
async def slow_ship() -> str:
    await _mark("shipping started")
    if os.environ.get("KILL_TEST_SLOW") == "1":
        await asyncio.sleep(60)  # the test kills the process in here
    await _mark("shipped")
    return "shipped"


@registry.workflow("killable")
async def killable(ctx: WorkflowContext, amount: int) -> list[str]:
    return [await ctx.step(charge, amount), await ctx.step(slow_ship)]
