"""End-to-end numbers for docs/benchmark-results/. Three measurements:

drain    Start N workflows of S no-op steps, then start K worker processes and time how long the backlog takes.
latency  Keep K workers running, start workflows at a fixed rate R for T seconds, and report start-to-finish latency.
replay   The CPU cost of replaying a history of H recorded steps, which every wake of a long workflow pays.

Workers are separate processes (`python -m ratchet worker`), the same way they run in production. Latency is measured
with the database clock (finished_at minus created_at), so worker clocks cannot skew it.

    uv run python -m benchmarks.throughput --dsn postgresql://postgres:postgres@127.0.0.1:55434/ratchet
"""

import argparse
import asyncio
import os
import platform
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime

import asyncpg

from ratchet import Registry, WorkflowContext
from ratchet.context import Event, RunState
from ratchet.migrate import migrate
from ratchet.store import Store, create_pool

registry = Registry()


@registry.activity()
async def noop(i: int) -> int:
    return i


@registry.workflow("bench")
async def bench(ctx: WorkflowContext, steps: int) -> int:
    total = 0
    for i in range(steps):
        total += await ctx.step(noop, i)
    return total


def spawn(dsn: str, workers: int, concurrency: int) -> list[subprocess.Popen[bytes]]:
    env = os.environ | {
        "RATCHET_DATABASE_URL": dsn,
        "RATCHET_APP": "benchmarks.throughput:registry",
        "RATCHET_CONCURRENCY": str(concurrency),
        "RATCHET_LOG_LEVEL": "WARNING",
    }
    command = [sys.executable, "-m", "ratchet", "worker"]
    return [subprocess.Popen(command, env=env) for _ in range(workers)]  # noqa: S603  # our own interpreter


def stop(processes: list[subprocess.Popen[bytes]]) -> None:
    for p in processes:
        p.terminate()
    for p in processes:
        p.wait()


async def reset(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await migrate(conn)
        await conn.execute("truncate ratchet_workflows cascade")


async def start_many(pool: asyncpg.Pool, count: int, steps: int, prefix: str) -> None:
    store = Store(pool)
    batch = 200
    for first in range(0, count, batch):
        await asyncio.gather(
            *(store.start(f"{prefix}-{i}", "bench", steps) for i in range(first, min(first + batch, count)))
        )


async def latencies(pool: asyncpg.Pool) -> list[float]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "select extract(epoch from finished_at - created_at)::float8 as s from ratchet_workflows"
            " where status = 'completed'"
        )
    return sorted(r["s"] for r in rows)


async def wait_all_done(pool: asyncpg.Pool, count: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        async with pool.acquire() as conn:
            done = await conn.fetchval("select count(*) from ratchet_workflows where status = 'completed'")
            failed = await conn.fetchval("select count(*) from ratchet_workflows where status = 'failed'")
        if failed:
            raise RuntimeError(f"{failed} workflows failed")
        if done >= count:
            return
        if time.monotonic() > deadline:
            raise TimeoutError(f"{done}/{count} done")
        await asyncio.sleep(0.05)


def pct(values: list[float], p: float) -> float:
    return values[min(len(values) - 1, round(p / 100 * (len(values) - 1)))]


async def drain(pool: asyncpg.Pool, dsn: str, *, count: int, steps: int, workers: int, concurrency: int) -> str:
    await reset(pool)
    await start_many(pool, count, steps, "drain")
    began = time.perf_counter()
    processes = spawn(dsn, workers, concurrency)
    try:
        await wait_all_done(pool, count, timeout=600)
    finally:
        elapsed = time.perf_counter() - began
        stop(processes)
    rate = count / elapsed
    return (
        f"| drain | {count} workflows x {steps} steps | {workers} x {concurrency} | {elapsed:.1f} s |"
        f" {rate:,.0f} workflows/s, {rate * steps:,.0f} steps/s |"
    )


async def latency(
    pool: asyncpg.Pool, dsn: str, *, rate: int, seconds: int, steps: int, workers: int, concurrency: int
) -> str:
    await reset(pool)
    processes = spawn(dsn, workers, concurrency)
    try:
        await asyncio.sleep(3)  # let the workers connect and start listening
        store = Store(pool)
        count = rate * seconds
        began = time.perf_counter()
        pending: set[asyncio.Task[object]] = set()
        for i in range(count):
            # Open loop: arrivals follow the clock, not the previous request, so a slow system cannot hide its queue.
            target = began + i / rate
            await asyncio.sleep(max(0.0, target - time.perf_counter()))
            task = asyncio.create_task(store.start(f"lat-{i}", "bench", steps))
            pending.add(task)
            task.add_done_callback(pending.discard)
        await asyncio.gather(*pending)
        await wait_all_done(pool, count, timeout=120)
    finally:
        stop(processes)
    values = await latencies(pool)
    ms = [v * 1000 for v in values]
    return (
        f"| latency | {rate}/s for {seconds} s, {steps} steps | {workers} x {concurrency} |"
        f" p50 {pct(ms, 50):.0f} ms, p95 {pct(ms, 95):.0f} ms, p99 {pct(ms, 99):.0f} ms, max {ms[-1]:.0f} ms |"
    )


class _Unused:
    """Replay never writes; anything reaching the journal means the history did not cover the run."""

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"replay touched the journal: {name}")


async def replay(history_size: int, rounds: int) -> str:
    now = datetime.now(UTC)
    history = {i: Event(i, "step", "noop", {"result": i}) for i in range(history_size)}
    samples = []
    for _ in range(rounds):
        ctx = WorkflowContext(RunState("r", history, {}, now, cancel_requested=False), _Unused())  # type: ignore[arg-type]
        started = time.perf_counter()
        await bench.fn(ctx, history_size)
        samples.append(time.perf_counter() - started)
    median = statistics.median(samples)
    per_step = median / history_size * 1e6
    return (
        f"| replay | {history_size} recorded steps | 1 | {median * 1000:.2f} ms per wake | {per_step:.1f} us per step |"
    )


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument("--steps", type=int, default=3)
    parser.add_argument("--rate", type=int, default=200)
    parser.add_argument("--seconds", type=int, default=20)
    args = parser.parse_args()

    pool = await create_pool(args.dsn, max_size=20)
    try:
        rows = [
            await drain(
                pool, args.dsn, count=args.count, steps=args.steps, workers=args.workers, concurrency=args.concurrency
            ),
            await latency(
                pool,
                args.dsn,
                rate=args.rate,
                seconds=args.seconds,
                steps=args.steps,
                workers=args.workers,
                concurrency=args.concurrency,
            ),
            await replay(100, 50),
            await replay(1000, 20),
            await replay(10_000, 5),
        ]
    finally:
        await pool.close()
    print(f"{platform.platform()}, Python {platform.python_version()}, {os.cpu_count()} logical CPUs")
    print(f"run at {datetime.now(UTC):%Y-%m-%d %H:%M} UTC\n")
    print("| run | load | workers x concurrency | result | |")
    print("|---|---|---|---|---|")
    for row in rows:
        print(row)


if __name__ == "__main__":
    asyncio.run(main())
