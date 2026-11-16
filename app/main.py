"""FastAPI application: dependency-alert API."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .database import close_connection, get_connection, init_db, write_tx
from .errors import GraphError, NoGraph, ReportConflict, UnknownService
from .graph_service import classify_alerts, get_graph, replace_graph
from .report_service import submit_report
from .schemas import (
    AlertsResponse,
    GraphPayload,
    GraphResponse,
    ReplaceResponse,
    ReportPayload,
    ReportResponse,
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Dependency Alert API",
    version="1.0.0",
    description=(
        "Service dependency graph + fault/recovery reports with monotonic "
        "sequence numbers and upstream-root-cause alert suppression."
    ),
    lifespan=lifespan,
)


@app.middleware("http")
async def _db_session(request: Request, call_next):
    # One SQLite connection per request (thread-local); always released.
    get_connection()
    try:
        return await call_next(request)
    finally:
        close_connection()


@app.exception_handler(GraphError)
async def _graph_error(_: Request, exc: GraphError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "detail": str(exc)},
    )


@app.exception_handler(ReportConflict)
async def _conflict(_: Request, exc: ReportConflict):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "detail": str(exc)},
    )


@app.exception_handler(UnknownService)
async def _unknown(_: Request, exc: UnknownService):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "detail": str(exc)},
    )


@app.exception_handler(NoGraph)
async def _no_graph(_: Request, exc: NoGraph):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "detail": str(exc)},
    )


@app.post("/graph", response_model=ReplaceResponse, status_code=200)
def put_graph(payload: GraphPayload) -> ReplaceResponse:
    """Replace the **whole** dependency graph.

    Invalid payloads (cycles, unknown references, duplicate names/edges) are
    rejected and the previously committed graph is retained.  States of
    same-named services survive the replacement.
    """
    return ReplaceResponse(**replace_graph(payload))


@app.get("/graph", response_model=GraphResponse)
def read_graph() -> GraphResponse:
    """Return the currently committed graph."""
    return get_graph()


@app.post("/reports", response_model=ReportResponse, status_code=200)
def post_report(payload: ReportPayload) -> ReportResponse:
    """Report a service fault or recovery with a per-service increasing seq."""
    return ReportResponse(**submit_report(payload))


@app.get("/alerts", response_model=AlertsResponse)
def read_alerts() -> AlertsResponse:
    """Return actionable faults, suppressed faults and their upstream root causes."""
    return AlertsResponse(**classify_alerts())


@app.post("/admin/reset", status_code=200)
def admin_reset() -> dict:
    """Clear all data (test/operations helper)."""
    with write_tx() as conn:
        conn.execute("DELETE FROM events")
        conn.execute("DELETE FROM service_state")
        conn.execute("DELETE FROM edges")
        conn.execute("DELETE FROM nodes")
        conn.execute("DELETE FROM graph_meta")
    return {"reset": True}


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
