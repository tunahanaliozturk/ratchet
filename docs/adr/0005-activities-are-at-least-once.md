# 5. Activities are at-least-once, with a key for the other side

## Context

An activity does something to the outside world, and then its outcome is recorded. A crash between the two is always
possible. No engine can make that exactly-once by itself, because the effect and the record live in different systems.

## Decision

Say so plainly, and give activities what they need to make the effect idempotent on the other side. Inside an activity,
`activity_info().idempotency_key` is `"{workflow_id}:{seq}"`. It is the same on every retry of that step and on every
replay after a crash, and different for every other step. Pass it to the payment provider, use it as a unique key in
your own table, put it in the message id.

The engine guarantees what it can: exactly one recorded outcome per step, whatever crashes happen.
`tests/integration/test_crash_property.py` checks this for random crash schedules, and `tests/integration/test_kill.py`
checks it with a real process killed in the middle of an activity.

## Consequences

- The README and the docstrings say "at-least-once" wherever a reader might assume otherwise.
- The property test measures the window precisely: a sequential step runs exactly once more for each crash that hit
  after its effect and before its record, and never more than that.
- Parallel branches can repeat a little more. When one branch dies its siblings are cancelled, and a sibling cancelled
  after its effect but before its record runs again. The test bounds this instead of pretending it away.
