"""Fault/recovery report processing with per-service monotonic sequence numbers."""
from __future__ import annotations

from .database import write_tx
from .errors import ReportConflict, UnknownService
from .schemas import ReportPayload

# Report status -> stored state.
_STATUS_MAP = {"fault": "down", "recover": "up"}


def submit_report(payload: ReportPayload) -> dict:
    """Process one fault/recovery report.

    Sequence semantics (per service, strictly increasing positive integers):

    * ``seq > last_seq``           -> apply the new state.
    * ``seq == last_seq``, same    -> idempotent retry, no state change.
    * ``seq == last_seq``, differ  -> 409 conflict, no state change.
    * ``seq <  last_seq``          -> stale/late report, 409, never rolls back.

    The service must exist in the currently committed graph.
    """
    wanted = _STATUS_MAP[payload.status]

    with write_tx() as conn:
        exists = conn.execute(
            "SELECT 1 FROM nodes WHERE service = ?", (payload.service,)
        ).fetchone()
        if exists is None:
            raise UnknownService(
                f"service {payload.service!r} is not part of the current graph"
            )

        row = conn.execute(
            "SELECT status, last_seq FROM service_state WHERE service = ?",
            (payload.service,),
        ).fetchone()

        if row is not None and payload.seq < row["last_seq"]:
            raise ReportConflict(
                f"stale report: seq {payload.seq} < last_seq {row['last_seq']} "
                f"for service {payload.service!r}; state is not rolled back"
            )

        if row is not None and payload.seq == row["last_seq"]:
            if row["status"] != wanted:
                raise ReportConflict(
                    f"conflicting report for seq {payload.seq} of service "
                    f"{payload.service!r}: already recorded {row['status']!r}, "
                    f"cannot change to {wanted!r}"
                )
            applied = False
            duplicate = True
            current_status = row["status"]
        else:
            conn.execute(
                "INSERT INTO service_state (service, status, last_seq) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT(service) DO UPDATE SET status = excluded.status, "
                "last_seq = excluded.last_seq",
                (payload.service, wanted, payload.seq),
            )
            applied = True
            duplicate = False
            current_status = wanted

        conn.execute(
            "INSERT INTO events (service, seq, status, applied) VALUES (?, ?, ?, ?)",
            (payload.service, payload.seq, wanted, int(applied)),
        )

    return {
        "applied": applied,
        "duplicate": duplicate,
        "service": payload.service,
        "seq": payload.seq,
        "status": payload.status,
        "current_status": current_status,
        "last_seq": payload.seq,
    }
