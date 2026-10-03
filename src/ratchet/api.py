"""HTTP API over the client. Bearer token auth, problem+json errors, keyset pagination, a hard body-size limit."""

import base64
import contextlib
import hmac
from collections.abc import AsyncIterator, Callable
from datetime import datetime
from typing import Annotated

from fastapi import Depends, FastAPI, Path, Query, Request, Response, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, ValidationError
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from ratchet.client import Client
from ratchet.definitions import Json, Registry
from ratchet.errors import UnknownWorkflow, WorkflowFinished, WorkflowIdConflict, WorkflowNotFound
from ratchet.settings import Settings
from ratchet.store import Status, Store, WorkflowRecord, create_pool

_ID_PATTERN = r"^[A-Za-z0-9._:\-]{1,200}$"
MIN_TOKEN_LENGTH = 16
_NAME_PATTERN = r"^[A-Za-z0-9._\-]{1,100}$"

type Lifespan = Callable[[FastAPI], contextlib.AbstractAsyncContextManager[None]]


class StartRequest(BaseModel):
    name: str = Field(pattern=_NAME_PATTERN)
    input: Json = None
    id: str | None = Field(default=None, pattern=_ID_PATTERN, description="Makes the start idempotent")


class SignalRequest(BaseModel):
    payload: Json = None
    dedupe_key: str | None = Field(default=None, max_length=200)


class WorkflowView(BaseModel):
    id: str
    name: str
    status: Status
    input: Json
    result: Json
    error: Json
    waiting_signals: list[str]
    due_at: datetime | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None

    @classmethod
    def of(cls, record: WorkflowRecord) -> WorkflowView:
        return cls.model_validate(record, from_attributes=True)


class WorkflowPage(BaseModel):
    items: list[WorkflowView]
    next: str | None = Field(description="Pass as `after` for the next page; null on the last one")


class EventView(BaseModel):
    seq: int
    kind: str
    name: str
    payload: Json
    recorded_at: datetime


class SignalAccepted(BaseModel):
    delivered: bool = Field(description="False when the dedupe key was used before and nothing was sent")


def _problem(status_code: int, title: str, detail: str) -> JSONResponse:
    return JSONResponse(
        {"type": "about:blank", "title": title, "status": status_code, "detail": detail},
        status_code=status_code,
        media_type="application/problem+json",
    )


def _cursor(record: WorkflowRecord) -> str:
    return base64.urlsafe_b64encode(f"{record.created_at.isoformat()}|{record.id}".encode()).decode()


def _parse_cursor(value: str) -> tuple[datetime, str]:
    try:
        at, _, workflow_id = base64.urlsafe_b64decode(value.encode()).decode().partition("|")
        return datetime.fromisoformat(at), workflow_id
    except ValueError as exc:
        raise _BadCursor from exc


class _BadCursor(Exception):
    pass


class _Unauthorised(Exception):
    pass


class BodyLimit:
    """Refuses bodies over a size. It reads the body itself, so leaving out Content-Length does not get past it.

    The body is buffered before the app sees it, which is fine at the sizes this API accepts (256 KiB by default).
    """

    def __init__(self, app: ASGIApp, max_bytes: int) -> None:
        self._app = app
        self._max = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        too_large = _problem(413, "Payload too large", f"bodies are limited to {self._max} bytes")
        declared = dict(scope["headers"]).get(b"content-length")
        if declared is not None and int(declared) > self._max:
            await too_large(scope, receive, send)
            return
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] != "http.request":
                break  # the client went away; the app finds out on its own
            body += message.get("body", b"")
            if len(body) > self._max:
                await too_large(scope, receive, send)
                return
            if not message.get("more_body", False):
                break
        replayed = False

        async def replay() -> Message:
            nonlocal replayed
            if replayed:
                return await receive()
            replayed = True
            return {"type": "http.request", "body": bytes(body), "more_body": False}

        await self._app(scope, replay, send)


def _client(request: Request) -> Client:
    client: Client = request.app.state.client
    return client


type _Client = Annotated[Client, Depends(_client)]


def build_app(token: str, max_payload_bytes: int, lifespan: Lifespan | None = None) -> FastAPI:
    """The app without its resources. Whoever runs it puts a ``Client`` and a ``ready`` probe on ``app.state``."""
    app = FastAPI(title="ratchet", version="0.1.0", docs_url="/docs", redoc_url=None, lifespan=lifespan)
    if len(token) < MIN_TOKEN_LENGTH:
        # An empty token would match a request with no Authorization header at all.
        raise ValueError(f"the API token must be at least {MIN_TOKEN_LENGTH} characters")
    bearer = HTTPBearer(auto_error=False)
    expected = token.encode()

    def authorised(credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)]) -> None:
        presented = credentials.credentials.encode() if credentials is not None else b""
        if not hmac.compare_digest(presented, expected):
            raise _Unauthorised

    @app.exception_handler(_Unauthorised)
    async def unauthorised(_: Request, __: Exception) -> Response:
        response = _problem(401, "Unauthorised", "a valid bearer token is required")
        response.headers["WWW-Authenticate"] = "Bearer"
        return response

    @app.exception_handler(WorkflowNotFound)
    async def not_found(_: Request, exc: Exception) -> Response:
        return _problem(404, "Workflow not found", f"no workflow with id {exc}")

    @app.exception_handler(UnknownWorkflow)
    async def unknown(_: Request, exc: Exception) -> Response:
        return _problem(422, "Unknown workflow", f"no workflow named {exc} is registered")

    @app.exception_handler(WorkflowIdConflict)
    async def conflict(_: Request, exc: Exception) -> Response:
        return _problem(409, "Id in use", f"{exc} was started with a different name or input")

    @app.exception_handler(WorkflowFinished)
    async def finished(_: Request, exc: Exception) -> Response:
        return _problem(409, "Workflow finished", f"{exc} has already reached a final status")

    @app.exception_handler(ValidationError)
    async def invalid_input(_: Request, exc: Exception) -> Response:
        assert isinstance(exc, ValidationError)  # noqa: S101
        return _problem(422, "Invalid input", exc.json(include_url=False, include_input=False))

    @app.exception_handler(_BadCursor)
    async def bad_cursor(_: Request, __: Exception) -> Response:
        return _problem(400, "Bad cursor", "`after` must be a value returned as `next`")

    guarded = [Depends(authorised)]

    @app.post("/v1/workflows", status_code=201, response_model=WorkflowView, dependencies=guarded)
    async def start(body: StartRequest, response: Response, client: _Client) -> WorkflowView:
        record, created = await client.start(body.name, body.input, workflow_id=body.id)
        if not created:
            response.status_code = status.HTTP_200_OK
        response.headers["Location"] = f"/v1/workflows/{record.id}"
        return WorkflowView.of(record)

    @app.get("/v1/workflows", response_model=WorkflowPage, dependencies=guarded)
    async def list_workflows(
        client: _Client,
        status_: Annotated[Status | None, Query(alias="status")] = None,
        after: Annotated[str | None, Query(max_length=500)] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> WorkflowPage:
        records = await client.list(status=status_, after=_parse_cursor(after) if after else None, limit=limit)
        next_page = _cursor(records[-1]) if len(records) == limit else None
        return WorkflowPage(items=[WorkflowView.of(r) for r in records], next=next_page)

    @app.get("/v1/workflows/{workflow_id}", response_model=WorkflowView, dependencies=guarded)
    async def get(workflow_id: str, client: _Client) -> WorkflowView:
        return WorkflowView.of(await client.get(workflow_id))

    @app.get("/v1/workflows/{workflow_id}/history", response_model=list[EventView], dependencies=guarded)
    async def history(workflow_id: str, client: _Client) -> list[EventView]:
        return [
            EventView(seq=e.seq, kind=e.kind, name=e.name, payload=e.payload, recorded_at=at)
            for e, at in await client.history(workflow_id)
        ]

    @app.post(
        "/v1/workflows/{workflow_id}/signals/{name}",
        status_code=202,
        response_model=SignalAccepted,
        dependencies=guarded,
    )
    async def signal(
        workflow_id: str, name: Annotated[str, Path(pattern=_NAME_PATTERN)], body: SignalRequest, client: _Client
    ) -> SignalAccepted:
        delivered = await client.signal(workflow_id, name, body.payload, dedupe_key=body.dedupe_key)
        return SignalAccepted(delivered=delivered)

    @app.post("/v1/workflows/{workflow_id}/cancel", status_code=202, dependencies=guarded)
    async def cancel(workflow_id: str, client: _Client) -> Response:
        await client.cancel(workflow_id)
        return Response(status_code=202)

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz(request: Request) -> Response:
        try:
            await request.app.state.ready()
        except Exception:  # any failure to reach the database means not ready
            return _problem(503, "Not ready", "the database is unreachable")
        return JSONResponse({"status": "ready"})

    app.add_middleware(BodyLimit, max_bytes=max_payload_bytes)
    return app


def create_app(settings: Settings, registry: Registry) -> FastAPI:
    """The production app. It owns a connection pool for the lifetime of the process."""
    if settings.api_token is None:
        raise ValueError("RATCHET_API_TOKEN must be set; the API has no anonymous mode")

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = await create_pool(settings.database_url.get_secret_value(), max_size=settings.pool_size)

        async def ready() -> None:
            async with pool.acquire() as conn:
                await conn.fetchval("select 1")

        app.state.client = Client(Store(pool), registry)
        app.state.ready = ready
        try:
            yield
        finally:
            await pool.close()

    return build_app(settings.api_token.get_secret_value(), settings.max_payload_bytes, lifespan)
