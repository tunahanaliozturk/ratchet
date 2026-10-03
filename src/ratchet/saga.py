"""Compensation in reverse order, written as a context manager around the steps it protects."""

from collections.abc import Callable
from types import TracebackType
from typing import Any, Self

from ratchet.context import WorkflowContext
from ratchet.definitions import Activity
from ratchet.errors import ActivityError


class CompensationFailed(ActivityError):
    """One or more compensations failed too. The workflow needs a person; the history says which ones."""


class Saga:
    """Register an undo after each step that succeeded. If the block raises, the undos run newest first, durably.

    .. code-block:: python

        async with Saga(ctx) as saga:
            hold = await ctx.step(reserve_stock, order)
            saga.on_failure(release_stock, hold)
            await ctx.step(charge_card, order)

    Compensations run as ordinary steps, so they are retried, recorded and replayed like any other. A compensation
    that still fails after its retries does not stop the ones before it from running.
    """

    def __init__(self, ctx: WorkflowContext) -> None:
        self._ctx = ctx
        self._undo: list[Callable[[], Any]] = []

    def on_failure[**P, R](self, activity: Activity[P, R], /, *args: P.args, **kwargs: P.kwargs) -> None:
        # The position is taken when the compensation runs, not now, so registering is free and deterministic.
        self._undo.append(lambda: self._ctx.step(activity, *args, **kwargs))

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        if exc is None or not isinstance(exc, Exception):
            return  # success, or a suspension unwinding the coroutine: nothing to undo
        failures: list[ActivityError] = []
        for undo in reversed(self._undo):
            try:
                await undo()
            except ActivityError as failed:
                failures.append(failed)
        if failures:
            names = ", ".join(f.activity for f in failures)
            raise CompensationFailed(names, "CompensationFailed", str(failures[0]), len(failures)) from exc
