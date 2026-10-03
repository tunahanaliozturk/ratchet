# 8. asyncpg and plain SQL, no ORM

## Context

The house standard for Python services in these repositories is SQLAlchemy with Alembic. This project is a different
kind of thing: a library whose value sits in about fifteen SQL statements, each written for a particular locking and
round-trip behaviour (`FOR SHARE` in a CTE in front of an insert, `SKIP LOCKED` claims, one-statement loads).

## Decision

asyncpg directly, with every statement parameterised and written out in `store.py`. Migrations are numbered `.sql`
files applied by `ratchet migrate` under an advisory lock, each in a transaction with its bookkeeping row.

psycopg 3 was the other candidate. It is LGPL-3.0, which the licence policy for these repositories does not allow.

## Consequences

- A reviewer reads the exact SQL that runs, next to the comment explaining its lock.
- There is no ORM session to share by mistake across tasks.
- JSON parameters are wrapped explicitly (`Jsonb(value)`), because asyncpg sends a bare `None` as SQL NULL without
  consulting the codec, and `None` is a valid workflow input. The integration tests caught this on their first run.
- asyncpg resets every connection it takes back into the pool, which costs a round trip. Nothing here leaves session
  state on a pooled connection, so the reset is switched off. Together with folding the history load and the final
  write into one statement each, that took the laptop drain benchmark from 331 to 896 workflows a second
  (docs/benchmark-results/2026-10-03-laptop.md).
- Schema changes are written by hand. There is one small set of tables and it changes rarely; autogenerate would not
  earn its keep.
