from datetime import timedelta

import pytest
from pydantic import BaseModel, ValidationError

from ratchet import Registry, RetryPolicy, WorkflowContext
from ratchet.errors import UnknownWorkflow


def test_retry_delays_grow_by_the_multiplier_and_stop_at_the_ceiling() -> None:
    policy = RetryPolicy(initial_delay=timedelta(seconds=1), multiplier=3, max_delay=timedelta(seconds=10), jitter=0)

    assert [policy.delay_after(n, 0.0).total_seconds() for n in (1, 2, 3, 4)] == [1, 3, 9, 10]


def test_jitter_only_ever_shortens_the_wait() -> None:
    policy = RetryPolicy(initial_delay=timedelta(seconds=10), max_delay=timedelta(seconds=10), jitter=0.2)

    assert policy.delay_after(1, 0.0) == timedelta(seconds=10)
    assert policy.delay_after(1, 0.999) > timedelta(seconds=8)
    assert policy.delay_after(5, 0.999) <= timedelta(seconds=10)


@pytest.mark.parametrize("kwargs", [{"max_attempts": 0}, {"multiplier": 0.5}, {"jitter": 1.0}])
def test_a_retry_policy_that_cannot_work_is_refused(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="must be"):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]


class Payment(BaseModel):
    amount: int


def test_a_workflow_input_is_validated_against_its_annotation() -> None:
    registry = Registry()

    @registry.workflow()
    async def pay(ctx: WorkflowContext, payment: Payment) -> int:
        return payment.amount

    definition = registry.get_workflow("pay")
    assert definition.input.validate_python({"amount": 5}) == Payment(amount=5)
    with pytest.raises(ValidationError):
        definition.input.validate_python({"amount": "lots"})


def test_names_are_unique_and_unknown_names_are_refused() -> None:
    registry = Registry()

    @registry.activity("same")
    async def first() -> None: ...

    with pytest.raises(ValueError, match="registered twice"):

        @registry.activity("same")
        async def second() -> None: ...

    with pytest.raises(UnknownWorkflow):
        registry.get_workflow("missing")


def test_a_workflow_must_take_ctx_and_one_input() -> None:
    registry = Registry()
    with pytest.raises(TypeError, match=r"exactly \(ctx, input\)"):

        @registry.workflow()  # type: ignore[arg-type]  # the mistake under test
        async def wrong(ctx: WorkflowContext) -> None: ...
