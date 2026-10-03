"""Registering workflows and activities, and the retry policy an activity runs under."""

from __future__ import annotations

import annotationlib
import asyncio
import inspect
import typing
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, overload

from pydantic import TypeAdapter

from ratchet.errors import UnknownWorkflow

if TYPE_CHECKING:
    from ratchet.context import WorkflowContext

type Json = Any
"""What survives a round trip through ``jsonb``: dicts, lists, strings, numbers, booleans and None."""


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with jitter. ``max_attempts`` counts the first try, so 1 means no retries."""

    max_attempts: int = 3
    initial_delay: timedelta = timedelta(seconds=1)
    multiplier: float = 2.0
    max_delay: timedelta = timedelta(minutes=5)
    jitter: float = 0.2

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")
        if not 0 <= self.jitter < 1:
            raise ValueError("jitter must be in [0, 1)")

    def delay_after(self, attempt: int, sample: float) -> timedelta:
        """The wait before attempt ``attempt + 1``. ``sample`` is a uniform draw from [0, 1), passed in to stay pure.

        Jitter only ever shortens the wait, so ``max_delay`` stays a real ceiling.
        """
        base = min(self.initial_delay * self.multiplier ** (attempt - 1), self.max_delay)
        return base * (1 - self.jitter * sample)


@dataclass(frozen=True)
class ActivityInfo:
    """What a running activity can learn about itself, through :func:`activity_info`."""

    workflow_id: str
    seq: int
    attempt: int

    @property
    def idempotency_key(self) -> str:
        """Stable across retries and across a crash and replay of the same step. Hand it to the system you call."""
        return f"{self.workflow_id}:{self.seq}"


_current_activity: ContextVar[ActivityInfo] = ContextVar("ratchet_activity")


def activity_info() -> ActivityInfo:
    """Inside an activity, the identity of this execution. Raises ``LookupError`` anywhere else."""
    return _current_activity.get()


@dataclass(frozen=True)
class Activity[**P, R]:
    """A side-effecting function a workflow calls through ``ctx.step``. Its result is recorded; its arguments not."""

    name: str
    fn: Callable[P, Awaitable[R]] | Callable[P, R]
    is_async: bool
    retry: RetryPolicy
    timeout: timedelta | None
    result: TypeAdapter[R] = field(repr=False)

    async def invoke(self, info: ActivityInfo, *args: P.args, **kwargs: P.kwargs) -> R:
        token = _current_activity.set(info)
        try:
            if self.is_async:
                return await typing.cast("Awaitable[R]", self.fn(*args, **kwargs))
            # Context variables are copied into the thread, so activity_info() works in sync activities too.
            return await asyncio.to_thread(typing.cast("Callable[P, R]", self.fn), *args, **kwargs)
        finally:
            _current_activity.reset(token)


@dataclass(frozen=True)
class Workflow[I, R]:
    """An async function of ``(ctx, input)`` that the engine may run many times and must always run the same way."""

    name: str
    fn: Callable[[WorkflowContext, I], Awaitable[R]]
    input: TypeAdapter[I] = field(repr=False)
    result: TypeAdapter[R] = field(repr=False)


def _hint(fn: Callable[..., Any], key: str) -> Any:
    try:
        return typing.get_type_hints(fn).get(key, Any)
    except NameError:
        # Some other annotation cannot be resolved yet. Take this one if it can, and treat it as Any if it cannot,
        # rather than guess.
        value = annotationlib.get_annotations(fn, format=annotationlib.Format.FORWARDREF).get(key, Any)
        return Any if isinstance(value, str | annotationlib.ForwardRef) else value


class _ActivityDecorator:
    def __init__(self, registry: Registry, name: str | None, retry: RetryPolicy, timeout: timedelta | None) -> None:
        self._registry = registry
        self._name = name
        self._retry = retry
        self._timeout = timeout

    @overload
    def __call__[**P, R](self, fn: Callable[P, Awaitable[R]]) -> Activity[P, R]: ...
    @overload
    def __call__[**P, R](self, fn: Callable[P, R]) -> Activity[P, R]: ...
    def __call__(self, fn: Callable[..., Any]) -> Activity[..., Any]:
        defined = Activity(
            name=self._name or fn.__name__,
            fn=fn,
            is_async=inspect.iscoroutinefunction(fn),
            retry=self._retry,
            timeout=self._timeout,
            result=TypeAdapter(_hint(fn, "return")),
        )
        self._registry.add_activity(defined)
        return defined


class Registry:
    """The workflows and activities a process knows about. Workers and the API load the same one."""

    def __init__(self) -> None:
        self._workflows: dict[str, Workflow[Any, Any]] = {}
        self._activities: dict[str, Activity[..., Any]] = {}

    def activity(
        self, name: str | None = None, *, retry: RetryPolicy | None = None, timeout: timedelta | None = None
    ) -> _ActivityDecorator:
        """Register an activity. The name goes into the history, so renaming the function later is safe."""
        return _ActivityDecorator(self, name, retry or RetryPolicy(), timeout)

    def workflow[I, R](
        self, name: str | None = None
    ) -> Callable[[Callable[[WorkflowContext, I], Awaitable[R]]], Workflow[I, R]]:
        """Register a workflow. The input type, if annotated, is validated before anything is stored."""

        def register(fn: Callable[[WorkflowContext, I], Awaitable[R]]) -> Workflow[I, R]:
            params = list(inspect.signature(fn).parameters)
            if len(params) != 2:  # noqa: PLR2004
                raise TypeError(f"workflow {fn.__name__} must take exactly (ctx, input)")
            defined: Workflow[I, R] = Workflow(
                name=name or fn.__name__,
                fn=fn,
                input=TypeAdapter(_hint(fn, params[1])),
                result=TypeAdapter(_hint(fn, "return")),
            )
            if defined.name in self._workflows:
                raise ValueError(f"workflow {defined.name!r} is registered twice")
            self._workflows[defined.name] = defined
            return defined

        return register

    def add_activity(self, defined: Activity[..., Any]) -> None:
        if defined.name in self._activities:
            raise ValueError(f"activity {defined.name!r} is registered twice")
        self._activities[defined.name] = defined

    def get_workflow(self, name: str) -> Workflow[Any, Any]:
        try:
            return self._workflows[name]
        except KeyError:
            raise UnknownWorkflow(name) from None

    @property
    def workflow_names(self) -> frozenset[str]:
        return frozenset(self._workflows)
