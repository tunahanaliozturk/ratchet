# Contributing

Thanks for looking. A few things make a change easy to accept.

## Before you start

Open an issue first for anything bigger than a fix, so we can agree on the shape before you spend time on it. The
engine is small on purpose; features that need a second moving part (a broker, a cache) are unlikely to land.

## Setting up

```sh
uv sync
docker info   # the integration tests start Postgres in a container
```

## What a change needs

- `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy src tests examples benchmarks` and
  `uv run lint-imports` all clean. CI runs the same.
- A test that fails without the change. For anything touching `store.py`, `worker.py` or `context.py`, that test runs
  against Postgres in `tests/integration`, not against a mock.
- Test names that state the promise: `a_signal_sent_before_the_wait_is_kept`, not `test_signal_2`.
- If the change is a decision someone could reasonably have made differently, an ADR in `docs/adr/`.
- If it changes a number in the README, the benchmark run that produced the new number, committed under
  `docs/benchmark-results/` with the machine named.
- New dependencies must be permissively licensed at every depth. `uv run python tools/license_audit.py` checks.

## Commits

Conventional commit subjects (`fix(store): ...`, `feat(api): ...`), one logical change per commit.
