"""Claims due workflows, replays them, and records where each one ended up.

One worker process runs many workflows at once on one event loop. Any number of worker processes can share a database;
they never coordinate with each other directly, only through row locks and fences.
"""

import asyncio
import contextlib
import time
import traceback
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import structlog
from pydantic import ValidationError

from ratchet import telemetry
from ratchet.context import RunState, WorkflowContext, clean
from ratchet.definitions import Json, Registry
from ratchet.errors import (
    ActivityError,
    JournalUnavailable,
    LeaseLost,
    NonDeterminismError,
    Suspend,
    UnknownWorkflow,
    UnstorableValue,
    Wakeup,
    WorkflowCancelled,
)
from ratchet.store import UNREACHABLE, Claim, RunJournal, Store

log = structlog.get_logger()

# A workflow that cannot be replayed (unknown name, or code that no longer matches its history) is parked, not failed.
# A rolling deploy or a fix brings it back; failing it would throw the work away. It is retried this often meanwhile.
STALLED_RETRY = timedelta(minutes=1)
_MAX_TRACEBACK = 4000


@dataclass(frozen=True)
class _Completed:
    value: Any


@dataclass(frozen=True)
class _Failed:
    error: dict[str, Json]


@dataclass(frozen=True)
class _Cancelled:
    pass


@dataclass(frozen=True)
class _Stalled:
    error: dict[str, Json]


type _Outcome = _Completed | _Failed | _Cancelled | _Stalled | Wakeup


def _describe(exc: BaseException) -> dict[str, Json]:
    error: dict[str, Json] = {"type": type(exc).__qualname__, "message": clean(str(exc))}
    if isinstance(exc, ActivityError):
        error |= {"activity": exc.activity, "activity_error": exc.error_type, "attempts": exc.attempts}
    error["traceback"] = clean("".join(traceback.format_exception(exc))[-_MAX_TRACEBACK:])
    return error


def _leaves(group: BaseExceptionGroup[BaseException]) -> list[BaseException]:
    found: list[BaseException] = []
    for exc in group.exceptions:
        found.extend(_leaves(exc) if isinstance(exc, BaseExceptionGroup) else [exc])
    return found


class Worker:
    def __init__(
        self,
        store: Store,
        registry: Registry,
        *,
        worker_id: str,
        concurrency: int = 32,
        lease: timedelta = timedelta(seconds=30),
        idle_wait_max: timedelta = timedelta(seconds=5),
        max_history: int = 10_000,
        shutdown_grace: timedelta = timedelta(seconds=20),
    ) -> None:
        if lease < timedelta(seconds=1):
            raise ValueError("lease must be at least a second; heartbeats run at a third of it")
        self._store = store
        self._registry = registry
        self.worker_id = worker_id
        self._concurrency = concurrency
        self._lease = lease
        self._idle_wait_max = idle_wait_max.total_seconds()
        self._max_history = max_history
        self._grace = shutdown_grace.total_seconds()
        self._running: dict[asyncio.Task[None], RunJournal] = {}
        self._due = asyncio.Event()

    # Driving the loop

    async def run(self, stop: asyncio.Event) -> None:
        """Work until ``stop`` is set, then drain within the grace period and hand back whatever is left."""
        log.info("worker started", worker=self.worker_id, concurrency=self._concurrency)
        async with self._store.listen(self._due.set):
            while not stop.is_set():
                free = self._concurrency - len(self._running)
                try:
                    claimed = await self._store.claim(self.worker_id, self._lease, free) if free else []
                except UNREACHABLE as exc:
                    log.error("cannot claim work; retrying", error=f"{type(exc).__name__}: {exc}")
                    await self._idle(stop, busy=True)
                    continue
                for claim in claimed:
                    self._spawn(claim)
                if claimed and len(claimed) == free:
                    continue  # there may be more; the next pass waits only if every slot is busy
                await self._idle(stop, busy=free == 0)
        await self._drain()
        log.info("worker stopped", worker=self.worker_id)

    async def run_once(self) -> int:
        """Claim whatever is due right now, run it to its next resting point, and return how many ran. For tests."""
        claimed = await self._store.claim(self.worker_id, self._lease, self._concurrency)
        await asyncio.gather(*(self.execute(c) for c in claimed))
        return len(claimed)

    def _spawn(self, claim: Claim) -> None:
        journal = self._store.journal(claim, self._lease)
        task = asyncio.create_task(self._execute(journal), name=f"ratchet:{claim.workflow_id}")
        self._running[task] = journal
        task.add_done_callback(self._finished)

    def _finished(self, task: asyncio.Task[None]) -> None:
        self._running.pop(task, None)
        self._due.set()  # a slot is free

    async def _idle(self, stop: asyncio.Event, *, busy: bool) -> None:
        timeout = self._idle_wait_max
        # Clear first: a NOTIFY that lands while we ask the database when work is due must still wake us.
        self._due.clear()
        if not busy:
            try:
                until = await self._store.seconds_until_due()
            except UNREACHABLE:
                until = None
            if until is not None:
                timeout = min(timeout, max(until, 0.0))
        waiters = {asyncio.ensure_future(self._due.wait()), asyncio.ensure_future(stop.wait())}
        try:
            await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in waiters:
                waiter.cancel()

    async def _drain(self) -> None:
        if not self._running:
            return
        log.info("draining", worker=self.worker_id, in_flight=len(self._running))
        _, pending = await asyncio.wait(list(self._running), timeout=self._grace)
        journals = [self._running[t] for t in pending if t in self._running]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        # Whatever was interrupted goes back now rather than after its lease. Its current step runs again elsewhere;
        # activities are at-least-once, which is why they get an idempotency key.
        await asyncio.gather(*(j.release() for j in journals), return_exceptions=True)

    # One workflow

    async def execute(self, claim: Claim) -> None:
        await self._execute(self._store.journal(claim, self._lease))

    async def _execute(self, journal: RunJournal) -> None:
        run = asyncio.current_task()
        assert run is not None  # noqa: S101
        lost = asyncio.Event()
        heartbeat = asyncio.create_task(self._heartbeat(journal, run, lost))
        claim = journal.claim
        structlog.contextvars.bind_contextvars(workflow_id=claim.workflow_id, workflow=claim.name, fence=claim.fence)
        try:
            with telemetry.tracer.start_as_current_span(
                "ratchet.run", attributes={"workflow.id": claim.workflow_id, "workflow.name": claim.name}
            ):
                await self._drive(journal)
        except LeaseLost:
            telemetry.leases_lost.add(1)
            log.warning("lease lost, abandoning run")
        except JournalUnavailable as exc:
            # Nothing can be written, so nothing is. The lease runs out and another run starts from the last record.
            telemetry.runs.add(1, {"outcome": "journal_unavailable"})
            log.error("database unavailable during a run; it will be retried after the lease", error=str(exc))
        except asyncio.CancelledError:
            if not lost.is_set():
                raise
            telemetry.leases_lost.add(1)
            log.warning("lease lost during an activity, run cancelled")
        except Exception:
            # A bug in the engine or in the database schema, not in the workflow. Recording anything could make it
            # worse, so record nothing: the lease runs out, another run tries again, and this log says why.
            telemetry.runs.add(1, {"outcome": "crashed"})
            log.exception("run crashed; the workflow will be retried after its lease")
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
            structlog.contextvars.unbind_contextvars("workflow_id", "workflow", "fence")

    async def _heartbeat(self, journal: RunJournal, run: asyncio.Task[None], lost: asyncio.Event) -> None:
        interval = self._lease.total_seconds() / 3
        while True:
            await asyncio.sleep(interval)
            try:
                alive = await journal.heartbeat()
            except Exception, JournalUnavailable:  # a blip; the lease has two more intervals before it runs out
                log.warning("heartbeat failed", exc_info=True)
                continue
            if not alive:
                lost.set()
                run.cancel()
                return

    async def _drive(self, journal: RunJournal) -> None:
        claim = journal.claim
        state = journal.first_state() or await journal.load()
        while True:
            outcome, cancel_delivered = await self._replay(claim, state, journal)
            match outcome:
                case Wakeup(wake_at=wake_at, signals=signals):
                    # A run that recorded a cancel but was unwound by a sibling's suspension before raising it must
                    # not park: the cancel would then wait out the sibling's timer. Re-reading delivers it.
                    if not cancel_delivered and await journal.suspend(wake_at, signals):
                        telemetry.runs.add(1, {"outcome": "suspended"})
                        log.debug("suspended", wake_at=wake_at, signals=sorted(signals))
                        return
                    # A signal or a cancel came in while we ran. Read it and go again under the same lease.
                    state = await journal.load()
                    claim = journal.claim
                case _Stalled(error=error):
                    retry_at = state.now + STALLED_RETRY
                    if not await journal.suspend(retry_at, frozenset(), error):
                        state = await journal.load()  # a signal raced in; it may change nothing, but look
                        claim = journal.claim
                        continue
                    telemetry.runs.add(1, {"outcome": "stalled"})
                    log.error("workflow stalled", error=error["message"], retry_at=retry_at)
                    return
                case _Completed(value=value):
                    try:
                        await journal.finish("completed", value, None)
                    except UnstorableValue as exc:
                        await journal.finish("failed", None, _describe(exc))
                        self._final("failed", error=str(exc))
                        return
                    self._final("completed")
                    return
                case _Failed(error=error):
                    await journal.finish("failed", None, error)
                    self._final("failed", error=error["message"])
                    return
                case _Cancelled():
                    await journal.finish("cancelled", None, {"type": "WorkflowCancelled", "message": "cancelled"})
                    self._final("cancelled")
                    return

    @staticmethod
    def _final(status: str, **fields: Any) -> None:
        telemetry.runs.add(1, {"outcome": status})
        telemetry.workflows_finished.add(1, {"status": status})
        log.info(f"workflow {status}", **fields)

    async def _replay(self, claim: Claim, state: RunState, journal: RunJournal) -> tuple[_Outcome, bool]:
        """Run the workflow function once. Returns where it ended up, and whether this run delivered a cancel."""
        try:
            definition = self._registry.get_workflow(claim.name)
        except UnknownWorkflow as exc:
            return _Stalled({"type": "UnknownWorkflow", "message": f"this worker does not know workflow {exc}"}), False
        try:
            workflow_input = definition.input.validate_python(claim.input)
        except ValidationError as exc:
            # The input was valid when the workflow started, so the code's input type changed under it: a deploy
            # problem, like non-determinism, and handled the same way.
            return _Stalled(_describe(exc)), False

        ctx = WorkflowContext(state, journal, max_history=self._max_history)
        started = time.perf_counter()
        outcome: _Outcome
        try:
            value = await definition.fn(ctx, workflow_input)
            outcome = self._returned(ctx, definition.result.dump_python(value, mode="json"))
        except Suspend as suspension:
            outcome = Wakeup.merge([suspension])
        except WorkflowCancelled:
            outcome = _Cancelled()
        except NonDeterminismError as exc:
            outcome = _Stalled(_describe(exc))
        except Exception as exc:  # whatever the workflow raised, or a result that will not serialise, is its result
            outcome = self._from_group(exc) if isinstance(exc, ExceptionGroup) else _Failed(_describe(exc))
        except BaseExceptionGroup as group:
            outcome = self._from_group(group)
        finally:
            log.debug("replayed", seconds=round(time.perf_counter() - started, 4), history=len(state.history))
        return outcome, ctx.cancel_delivered

    @staticmethod
    def _returned(ctx: WorkflowContext, value: Json) -> _Outcome:
        unreached = ctx.unreached_history()
        if unreached:
            return _Stalled(_describe(NonDeterminismError(unreached[0], "an event", "nothing (the workflow returned)")))
        return _Completed(value)

    @staticmethod
    def _from_group(group: BaseExceptionGroup[BaseException]) -> _Outcome:
        """Branches of a TaskGroup. Lease loss wins, then a real failure, then cancellation, then suspension."""
        leaves = _leaves(group)
        for interrupt in (LeaseLost, JournalUnavailable):
            if found := next((e for e in leaves if isinstance(e, interrupt)), None):
                raise found
        if any(not isinstance(e, Exception | Suspend | asyncio.CancelledError) for e in leaves):
            raise group  # the process is going down (or a test says it is); record nothing
        if any(isinstance(e, NonDeterminismError) for e in leaves):
            return _Stalled(_describe(next(e for e in leaves if isinstance(e, NonDeterminismError))))
        suspensions = [e for e in leaves if isinstance(e, Suspend)]
        others = [e for e in leaves if not isinstance(e, Suspend | asyncio.CancelledError)]
        if not others and suspensions:
            return Wakeup.merge(suspensions)
        if all(isinstance(e, WorkflowCancelled) for e in others):
            return _Cancelled()
        return _Failed(_describe(group))
