"""Exceptions a workflow author can see, plus the two control-flow signals the engine uses internally."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


class RatchetError(Exception):
    """Base class for every error this library raises on purpose."""


class ActivityError(RatchetError):
    """An activity failed for good: retries are exhausted, or it raised a non-retryable error.

    The original exception object does not survive a replay, so the workflow sees this instead, on the live run and
    on every replay alike. Branch on ``error_type``, never on the exception class of the original.
    """

    def __init__(self, activity: str, error_type: str, message: str, attempts: int) -> None:
        super().__init__(f"activity {activity!r} failed after {attempts} attempt(s): {error_type}: {message}")
        self.activity = activity
        self.error_type = error_type
        self.message = message
        self.attempts = attempts


class NonRetryableError(RatchetError):
    """Raise this, or a subclass, from an activity to fail it immediately without spending the retry budget."""


class WorkflowCancelled(RatchetError):  # it is an outcome a workflow handles, not a programming error
    """Delivered once, at the first durable call after a cancel was requested. Catch it to compensate."""


class SignalTimeout(RatchetError):  # same reason as above
    """``wait_for_signal`` reached its deadline before the signal arrived."""

    def __init__(self, signal: str) -> None:
        super().__init__(f"no {signal!r} signal before the deadline")
        self.signal = signal


class NonDeterminismError(RatchetError):
    """The workflow code no longer makes the calls its history recorded, so replaying it would be a lie."""

    def __init__(self, seq: int, recorded: str, attempted: str) -> None:
        super().__init__(f"history position {seq} recorded {recorded}, but the code now calls {attempted}")
        self.seq = seq
        self.recorded = recorded
        self.attempted = attempted


class EngineInterrupt(BaseException):
    """Stops a run for a reason that is none of the workflow's business.

    These derive from ``BaseException`` on purpose. Workflow code that catches ``Exception`` must not be able to swallow
    them, and a :class:`ratchet.Saga` must not compensate for them: the workflow did not fail, the run did, and another
    run will pick up from the last recorded position.
    """


class LeaseLost(EngineInterrupt):
    """Another worker owns this workflow now. Whatever this run was about to write must be thrown away."""


class JournalUnavailable(EngineInterrupt):
    """The database could not record an outcome. The run stops, the lease runs out, and another run retries."""


class UnknownWorkflow(RatchetError):
    """No workflow with that name is registered."""


class WorkflowIdConflict(RatchetError):
    """A workflow with this id exists already, started with a different name or input."""


class WorkflowNotFound(RatchetError):
    """No workflow has that id."""


class WorkflowFinished(RatchetError):
    """The workflow has completed, failed or been cancelled, so it cannot take a signal or a cancel any more."""


class Suspend(EngineInterrupt):  # control flow, not an error
    """Unwinds the workflow coroutine because it cannot make progress until a time or a signal.

    Like every :class:`EngineInterrupt`, ``except Exception`` in workflow code does not swallow it.
    ``wake_at`` of ``None`` means "only a signal or a cancel can wake this".
    """

    def __init__(self, wake_at: datetime | None, signals: frozenset[str] = frozenset()) -> None:
        super().__init__(wake_at, signals)
        self.wake_at = wake_at
        self.signals = signals


@dataclass(frozen=True)
class Wakeup:
    """Where a set of suspended branches leaves the workflow: the earliest time, and every signal any branch awaits."""

    wake_at: datetime | None
    signals: frozenset[str]

    @staticmethod
    def merge(suspensions: list[Suspend]) -> Wakeup:
        times = [s.wake_at for s in suspensions if s.wake_at is not None]
        return Wakeup(min(times) if times else None, frozenset().union(*(s.signals for s in suspensions)))
