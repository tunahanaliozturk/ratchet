"""``ratchet migrate | worker | api``. Configuration comes from the environment; see docs/operations.md."""

import argparse
import asyncio
import contextlib
import importlib
import signal

import structlog
import uvicorn

from ratchet import telemetry
from ratchet.api import create_app
from ratchet.definitions import Registry
from ratchet.migrate import migrate
from ratchet.settings import Settings
from ratchet.store import Store, create_pool
from ratchet.worker import Worker

log = structlog.get_logger()


def load_registry(target: str) -> Registry:
    module_name, _, attribute = target.partition(":")
    registry = getattr(importlib.import_module(module_name), attribute or "registry")
    if not isinstance(registry, Registry):
        raise TypeError(f"{target} is a {type(registry).__name__}, not a ratchet Registry")
    return registry


async def _migrate(settings: Settings) -> None:
    pool = await create_pool(settings.database_url.get_secret_value(), min_size=1, max_size=1)
    try:
        async with pool.acquire() as conn:
            applied = await migrate(conn)
        log.info("schema up to date", applied=applied)
    finally:
        await pool.close()


async def _work(settings: Settings) -> None:
    registry = load_registry(settings.app)
    # A run holds a connection only while it writes, so the pool can be much smaller than the concurrency. One
    # connection is the listener's for good, and the claim loop needs one now and then.
    pool = await create_pool(settings.database_url.get_secret_value(), min_size=3, max_size=settings.pool_size)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # Windows: Ctrl+C still arrives as KeyboardInterrupt
            loop.add_signal_handler(sig, stop.set)
    worker = Worker(
        Store(pool),
        registry,
        worker_id=settings.worker_id,
        concurrency=settings.concurrency,
        lease=settings.lease,
        idle_wait_max=settings.idle_wait_max,
        max_history=settings.max_history,
        shutdown_grace=settings.shutdown_grace,
    )
    try:
        await worker.run(stop)
    finally:
        await pool.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="ratchet", description="Durable workflows on Postgres")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate", help="bring the database schema up to date")
    sub.add_parser("worker", help="run workflows from RATCHET_APP")
    api = sub.add_parser("api", help="serve the HTTP API for RATCHET_APP")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    args = parser.parse_args(argv)

    settings = Settings()  # filled from the environment
    telemetry.configure(settings.log_level, settings.otlp_endpoint)
    match args.command:
        case "migrate":
            asyncio.run(_migrate(settings))
        case "worker":
            asyncio.run(_work(settings))
        case "api":
            app = create_app(settings, load_registry(settings.app))
            uvicorn.run(app, host=args.host, port=args.port, log_config=None, proxy_headers=False)
