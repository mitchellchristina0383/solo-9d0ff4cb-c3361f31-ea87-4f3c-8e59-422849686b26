"""Domain errors mapped to HTTP responses in main.py."""
from __future__ import annotations


class GraphError(Exception):
    """Graph payload is structurally invalid (unknown reference, cycle, ...)."""

    status_code = 422
    code = "invalid_graph"


class ReportConflict(Exception):
    """Same seq with different content, or stale (lower) seq."""

    status_code = 409
    code = "conflict"


class UnknownService(Exception):
    """Report references a service that is not in the current graph."""

    status_code = 404
    code = "unknown_service"


class NoGraph(Exception):
    """No dependency graph has been committed yet."""

    status_code = 409
    code = "no_graph"
