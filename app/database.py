"""SQLite connection management and schema.

The database is the single source of truth.  Every state-changing request runs
inside one serialized ``BEGIN IMMEDIATE`` transaction so that concurrent
reports, graph replacements and reads can never observe or produce a mix of
two committed states.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Iterator

DB_PATH = os.environ.get("ALERT_DB_PATH", "/data/alerts.db")

# SQLite only allows one writer at a time; when two writers race, the loser
# gets SQLITE_BUSY.  We retry short-lived write transactions a few times
# instead of surfacing that internal contention to clients.
BUSY_RETRIES = 50
BUSY_RETRY_DELAY = 0.05

_SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS graph_meta (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    revision        INTEGER NOT NULL,
    version         TEXT,
    committed_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS nodes (
    service         TEXT PRIMARY KEY,
    revision        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS edges (
    revision        INTEGER NOT NULL,
    upstream        TEXT NOT NULL,
    downstream      TEXT NOT NULL,
    PRIMARY KEY (revision, upstream, downstream)
);

CREATE TABLE IF NOT EXISTS service_state (
    service         TEXT PRIMARY KEY,
    status          TEXT NOT NULL CHECK (status IN ('up', 'down')),
    last_seq        INTEGER NOT NULL CHECK (last_seq > 0)
);

CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    service         TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('up', 'down')),
    applied         INTEGER NOT NULL,
    recorded_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""

_local = threading.local()


def get_connection() -> sqlite3.Connection:
    """Return the request-scoped connection (one per thread/request)."""
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(
            DB_PATH,
            timeout=30.0,
            isolation_level=None,  # autocommit; transactions are explicit
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA busy_timeout = 30000")
        _local.conn = conn
    return conn


def init_db() -> None:
    conn = get_connection()
    try:
        conn.executescript(_SCHEMA)
    finally:
        close_connection()


@contextmanager
def write_tx() -> Iterator[sqlite3.Connection]:
    """Serialized write transaction with retry on ``SQLITE_BUSY``.

    ``BEGIN IMMEDIATE`` acquires the reserved lock up front, which prevents
    deadlocks between competing writers.
    """
    conn = get_connection()
    attempt = 0
    while True:
        try:
            conn.execute("BEGIN IMMEDIATE")
            break
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc) or attempt >= BUSY_RETRIES:
                raise
            attempt += 1
            time.sleep(BUSY_RETRY_DELAY)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


@contextmanager
def read_tx() -> Iterator[sqlite3.Connection]:
    """Read transaction holding a consistent snapshot of one committed state."""
    conn = get_connection()
    conn.execute("BEGIN")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def close_connection() -> None:
    conn = getattr(_local, "conn", None)
    if conn is not None:
        conn.close()
        _local.conn = None
