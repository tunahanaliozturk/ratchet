"""Starting, signalling, cancelling and reading workflows from application code. The HTTP API wraps this."""

import asyncio
import time
import uuid
from datetime import datetime, timedelta
from typing import Any

from ratchet.context import Event
from ratchet.definitions import Json, Registry, Workflow
from ratchet.store import FINAL, Status, Store, WorkflowRecord


class Client:
    def __init__(self, store: Store, registry: Registry) -> None:
        self._store = store
        self._registry = registry

    async def start(
        self, workflow: Workflow[Any, Any] | str, input_: Any, *, workflow_id: str | None = None
    ) -> tuple[WorkflowRecord, bool]:
        """Start a workflow. With a ``workflow_id`` this is idempotent: the same id, name and input return the original.

        The input is validated against the workflow's annotation here, so a bad input is refused at the door instead
        of being stored and failing on the worker later. Returns the record and whether this call created it.
        """
        definition = self._registry.get_workflow(workflow if isinstance(workflow, str) else workflow.name)
        payload = definition.input.dump_python(definition.input.validate_python(input_), mode="json")
        return await self._store.start(workflow_id or str(uuid.uuid7()), definition.name, payload)

    async def signal(self, workflow_id: str, name: str, payload: Json = None, *, dedupe_key: str | None = None) -> bool:
        """Send a signal. Returns False when ``dedupe_key`` was seen before for this workflow and nothing was sent."""
        return await self._store.signal(workflow_id, name, payload, dedupe_key)

    async def cancel(self, workflow_id: str) -> None:
        await self._store.cancel(workflow_id)

    async def get(self, workflow_id: str) -> WorkflowRecord:
        return await self._store.get(workflow_id)

    async def history(self, workflow_id: str) -> list[tuple[Event, datetime]]:
        return await self._store.history(workflow_id)

    async def list(
        self, *, status: Status | None = None, after: tuple[datetime, str] | None = None, limit: int = 50
    ) -> list[WorkflowRecord]:
        return await self._store.list(status, after, limit)

    async def wait(
        self, workflow_id: str, *, timeout: timedelta, poll: timedelta = timedelta(milliseconds=50)
    ) -> WorkflowRecord:
        """Poll until the workflow reaches a final status. Raises ``TimeoutError`` if it does not in time."""
        deadline = time.monotonic() + timeout.total_seconds()
        while True:
            record = await self._store.get(workflow_id)
            if record.status in FINAL:
                return record
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{workflow_id} is still {record.status}")
            await asyncio.sleep(poll.total_seconds())
