"""Applies the SQL files in ``migrations/`` in order, once each, under an advisory lock.

Plain SQL rather than Alembic: there is no ORM here to generate from, and a reader can review a schema change as the
exact statements that will run. Every file runs in its own transaction together with its bookkeeping row.
"""

from importlib.resources import files

import asyncpg
import structlog

log = structlog.get_logger()

# Any constant works, as long as nothing else in the database uses it. "ratchet" in ASCII.
_LOCK = 0x72617463686574


def _migrations() -> list[tuple[str, str]]:
    folder = files("ratchet") / "migrations"
    return sorted((f.name, f.read_text(encoding="utf-8")) for f in folder.iterdir() if f.name.endswith(".sql"))


async def migrate(conn: asyncpg.Connection) -> list[str]:
    """Bring the schema up to date. Safe to run from every process at start-up; the lock makes them take turns."""
    applied_now: list[str] = []
    await conn.execute("select pg_advisory_lock($1)", _LOCK)
    try:
        await conn.execute(
            "create table if not exists ratchet_schema_migrations ("
            " name text primary key, applied_at timestamptz not null default now())"
        )
        done = {r["name"] for r in await conn.fetch("select name from ratchet_schema_migrations")}
        for name, sql in _migrations():
            if name in done:
                continue
            async with conn.transaction():
                await conn.execute(sql)
                await conn.execute("insert into ratchet_schema_migrations (name) values ($1)", name)
            log.info("migration applied", migration=name)
            applied_now.append(name)
    finally:
        await conn.execute("select pg_advisory_unlock($1)", _LOCK)
    return applied_now
