"""An order fulfilment workflow: the example the README and the compose stack run.

Stock is reserved, the card is charged, large orders wait for a person to approve them (for up to a day), then the
order ships. Anything failing after the reservation undoes what came before, newest first.

The activities pretend to call other systems. They log, and they fail on purpose for the inputs that say so, so that
retries, compensation and approval timeouts can be tried from requests.http without editing code.
"""

from datetime import timedelta
from decimal import Decimal

import structlog
from pydantic import BaseModel, Field

from ratchet import NonRetryableError, Registry, RetryPolicy, Saga, SignalTimeout, WorkflowContext, activity_info

registry = Registry()
log = structlog.get_logger()

APPROVAL_THRESHOLD = Decimal(1000)
FLAKY_FAILURES = 2


class Order(BaseModel):
    order_id: str = Field(min_length=1, max_length=64)
    sku: str = Field(min_length=1, max_length=64)
    quantity: int = Field(gt=0, le=1000)
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    card: str = Field(default="ok", description="'ok', 'flaky' (fails twice, then works) or 'declined'")


class Receipt(BaseModel):
    order_id: str
    status: str
    charge_id: str | None = None
    tracking: str | None = None


class CardDeclined(NonRetryableError):
    pass


@registry.activity(retry=RetryPolicy(max_attempts=3, initial_delay=timedelta(milliseconds=200)))
async def reserve_stock(order: Order) -> str:
    log.info("stock reserved", order_id=order.order_id, sku=order.sku, quantity=order.quantity)
    return f"hold-{order.order_id}"


@registry.activity()
async def release_stock(hold: str) -> None:
    log.info("stock released", hold=hold)


@registry.activity(
    retry=RetryPolicy(max_attempts=5, initial_delay=timedelta(milliseconds=500)), timeout=timedelta(seconds=10)
)
async def charge_card(order: Order) -> str:
    # The key the payment provider would use to make a retried or replayed charge a no-op.
    key = activity_info().idempotency_key
    if order.card == "declined":
        raise CardDeclined(f"card declined for {order.order_id}")
    if order.card == "flaky" and activity_info().attempt <= FLAKY_FAILURES:
        raise ConnectionError("payment provider timed out")
    log.info("card charged", order_id=order.order_id, amount=str(order.amount), idempotency_key=key)
    return f"ch-{key}"


@registry.activity()
async def refund(charge_id: str) -> None:
    log.info("refunded", charge_id=charge_id)


@registry.activity()
async def ship(order: Order) -> str:
    log.info("shipped", order_id=order.order_id)
    return f"TRK-{order.order_id.upper()}"


@registry.workflow("fulfil_order")
async def fulfil_order(ctx: WorkflowContext, order: Order) -> Receipt:
    async with Saga(ctx) as saga:
        hold = await ctx.step(reserve_stock, order)
        saga.on_failure(release_stock, hold)

        charge_id = await ctx.step(charge_card, order)
        saga.on_failure(refund, charge_id)

        if order.amount >= APPROVAL_THRESHOLD:
            try:
                decision = await ctx.wait_for_signal("approval", timeout=timedelta(hours=24), payload_type=dict)
            except SignalTimeout:
                decision = {"approved": False}
            if not decision.get("approved"):
                # Raising inside the saga runs the refund, then the release, then fails the workflow with this.
                raise OrderRejected(order.order_id)

        tracking = await ctx.step(ship, order)
    return Receipt(order_id=order.order_id, status="shipped", charge_id=charge_id, tracking=tracking)


class OrderRejected(Exception):
    pass
