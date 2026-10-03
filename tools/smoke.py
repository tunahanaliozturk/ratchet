"""Walks the README quick start against a running `docker compose up` stack and fails loudly if any step is wrong.

Standard library only, so it runs anywhere Python does. CI runs it after bringing the stack up.
"""

import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from typing import Any

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8200"
TOKEN = "local-dev-token"


def call(method: str, path: str, body: Any = None, *, token: str | None = TOKEN) -> tuple[int, Any]:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(f"{BASE}{path}", data=data, method=method)  # noqa: S310  # http(s) base only
    request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
            raw = response.read()
            return response.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"null")


def start(workflow_id: str, amount: str, card: str = "ok") -> None:
    order = {"order_id": workflow_id, "sku": "KB-01", "quantity": 1, "amount": amount, "card": card}
    status, body = call("POST", "/v1/workflows", {"name": "fulfil_order", "id": workflow_id, "input": order})
    check(status == 201, f"start {workflow_id}: {status} {body}")


def until(workflow_id: str, wanted: str, timeout: float = 30) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        _, body = call("GET", f"/v1/workflows/{workflow_id}")
        if body["status"] == wanted:
            return dict(body)
        if time.monotonic() > deadline:
            check(False, f"{workflow_id} is {body['status']}, expected {wanted}: {body}")
        time.sleep(0.2)


def check(condition: bool, message: str) -> None:
    if not condition:
        print(f"FAIL {message}")
        sys.exit(1)
    print(f"ok   {message.split(':', maxsplit=1)[0]}")


def main() -> None:
    run = uuid.uuid4().hex[:8]

    start(f"small-{run}", "120.00")
    done = until(f"small-{run}", "completed")
    check(done["result"]["tracking"] == f"TRK-SMALL-{run.upper()}", f"small order shipped: {done['result']}")

    start(f"flaky-{run}", "80.00", card="flaky")
    until(f"flaky-{run}", "completed")

    start(f"declined-{run}", "80.00", card="declined")
    failed = until(f"declined-{run}", "failed")
    check(failed["error"]["activity_error"] == "CardDeclined", f"declined card failed the order: {failed['error']}")

    start(f"large-{run}", "4200.00")
    parked = until(f"large-{run}", "sleeping")
    check(parked["waiting_signals"] == ["approval"], f"large order waits for approval: {parked}")
    status, body = call("POST", f"/v1/workflows/large-{run}/signals/approval", {"payload": {"approved": True}})
    check(status == 202 and body == {"delivered": True}, f"approval sent: {status} {body}")
    until(f"large-{run}", "completed")

    start(f"cancelled-{run}", "4200.00")
    until(f"cancelled-{run}", "sleeping")
    status, _ = call("POST", f"/v1/workflows/cancelled-{run}/cancel")
    check(status == 202, f"cancel accepted: {status}")
    until(f"cancelled-{run}", "cancelled")
    _, history = call("GET", f"/v1/workflows/cancelled-{run}/history")
    steps = [e["name"] for e in history if e["kind"] == "step"]
    check(steps[-2:] == ["refund", "release_stock"], f"cancel compensated newest first: {steps}")

    status, _ = call("GET", "/v1/workflows", token=None)
    check(status == 401, f"no token is refused: {status}")
    print("smoke passed")


if __name__ == "__main__":
    main()
