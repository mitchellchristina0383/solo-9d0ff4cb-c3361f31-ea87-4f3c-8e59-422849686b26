"""Dependency-graph validation, replacement and fault classification."""
from __future__ import annotations

import sqlite3
from collections import deque

from .database import read_tx, write_tx
from .errors import GraphError, NoGraph
from .schemas import GraphPayload, GraphResponse, ServiceIn

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_graph(payload: GraphPayload) -> tuple[list[str], dict[str, list[str]]]:
    """Validate a full-graph payload.

    Returns ``(nodes, upstream)`` where ``upstream[s]`` is the sorted list of
    services ``s`` depends on.  Raises :class:`GraphError` on any structural
    problem (duplicate names, unknown references or cycles); the caller keeps
    the previously committed graph untouched in that case.
    """
    nodes: list[str] = []
    seen: set[str] = set()
    duplicates: list[str] = []
    for svc in payload.services:
        if svc.name in seen:
            duplicates.append(svc.name)
        seen.add(svc.name)
        nodes.append(svc.name)
    if duplicates:
        raise GraphError(f"duplicate service names: {sorted(set(duplicates))}")

    node_set = set(nodes)
    upstream: dict[str, list[str]] = {}
    unknown: set[tuple[str, str]] = set()
    dup_edges: set[tuple[str, str]] = set()

    for svc in payload.services:
        deps: list[str] = []
        edge_seen: set[str] = set()
        for dep in svc.dependencies:
            if dep == svc.name:
                raise GraphError(f"service {svc.name!r} cannot depend on itself")
            if dep not in node_set:
                unknown.add((svc.name, dep))
            if dep in edge_seen:
                dup_edges.add((svc.name, dep))
            edge_seen.add(dep)
            deps.append(dep)
        upstream[svc.name] = deps

    if unknown:
        detail = ", ".join(f"{a}->{b}" for a, b in sorted(unknown))
        raise GraphError(f"unknown service references: {detail}")
    if dup_edges:
        detail = ", ".join(f"{a}->{b}" for a, b in sorted(dup_edges))
        raise GraphError(f"duplicate dependency edges: {detail}")

    _ensure_acyclic(nodes, upstream)
    return nodes, upstream


def _ensure_acyclic(nodes: list[str], upstream: dict[str, list[str]]) -> None:
    """Kahn's algorithm; anything left over when the queue drains is on a cycle."""
    indegree = {n: 0 for n in nodes}
    dependents: dict[str, list[str]] = {n: [] for n in nodes}
    for downstream, deps in upstream.items():
        for up in deps:
            indegree[downstream] += 1
            dependents[up].append(downstream)

    queue = deque(sorted(n for n, d in indegree.items() if d == 0))
    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        for child in sorted(dependents[node]):
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if visited != len(nodes):
        cyclic = sorted(n for n, d in indegree.items() if d > 0)
        raise GraphError(f"dependency cycle detected involving: {cyclic}")


# ---------------------------------------------------------------------------
# Replacement
# ---------------------------------------------------------------------------


def replace_graph(payload: GraphPayload) -> dict:
    """Atomically replace the whole graph.

    Service-state rows (up/down + last seq) are keyed by service name and live
    outside the graph tables, so states of same-named services survive a
    replacement.  The swap of nodes/edges/meta happens in one write
    transaction and is therefore all-or-nothing.
    """
    nodes, upstream = validate_graph(payload)

    with write_tx() as conn:
        row = conn.execute("SELECT revision FROM graph_meta WHERE id = 1").fetchone()
        new_revision = (row["revision"] + 1) if row else 1

        conn.execute("DELETE FROM edges")
        conn.execute("DELETE FROM nodes")
        conn.executemany(
            "INSERT INTO nodes (service, revision) VALUES (?, ?)",
            [(n, new_revision) for n in nodes],
        )
        edge_rows = [
            (new_revision, up, downstream)
            for downstream, deps in upstream.items()
            for up in deps
        ]
        conn.executemany(
            "INSERT INTO edges (revision, upstream, downstream) VALUES (?, ?, ?)",
            edge_rows,
        )
        conn.execute(
            "INSERT INTO graph_meta (id, revision, version) VALUES (1, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET revision = excluded.revision, "
            "version = excluded.version, "
            "committed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
            (new_revision, payload.version),
        )
        preserved = [
            r["service"]
            for r in conn.execute(
                "SELECT service FROM service_state WHERE service IN (%s) ORDER BY service"
                % ",".join("?" * len(nodes)),
                nodes,
            )
        ]
    return {
        "accepted": True,
        "revision": new_revision,
        "version": payload.version,
        "service_count": len(nodes),
        "edge_count": len(edge_rows),
        "preserved_state": preserved,
    }


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


def _load_graph(conn: sqlite3.Connection) -> tuple[int, str | None, dict[str, list[str]]]:
    meta = conn.execute("SELECT revision, version FROM graph_meta WHERE id = 1").fetchone()
    if meta is None:
        raise NoGraph("no dependency graph has been committed yet")
    upstream: dict[str, list[str]] = {
        r["service"]: [] for r in conn.execute("SELECT service FROM nodes")
    }
    for r in conn.execute(
        "SELECT edges.upstream AS upstream, edges.downstream AS downstream "
        "FROM edges ORDER BY edges.upstream, edges.downstream"
    ):
        upstream[r["downstream"]].append(r["upstream"])
    return meta["revision"], meta["version"], upstream


def get_graph() -> GraphResponse:
    with read_tx() as conn:
        revision, version, upstream = _load_graph(conn)
        meta = conn.execute(
            "SELECT committed_at FROM graph_meta WHERE id = 1"
        ).fetchone()
        services = [
            ServiceIn(name=name, dependencies=sorted(deps))
            for name, deps in sorted(upstream.items())
        ]
    return GraphResponse(
        revision=revision,
        version=version,
        committed_at=meta["committed_at"],
        services=services,
    )


# ---------------------------------------------------------------------------
# Fault classification
# ---------------------------------------------------------------------------


def classify_alerts() -> dict:
    """Compute actionable vs suppressed faults from one committed snapshot.

    A faulty service is *suppressed* only when there exists a dependency path
    on which **every** node is faulty leading to a root cause (a faulty
    service with no faulty upstream).  Healthy or never-reported nodes cut
    paths, so a fault behind such a node stays actionable.
    """
    with read_tx() as conn:
        revision, version, upstream = _load_graph(conn)
        down: set[str] = {
            r["service"]
            for r in conn.execute("SELECT service FROM service_state WHERE status = 'down'")
        }
        seq_rows = {
            r["service"]: r["last_seq"]
            for r in conn.execute("SELECT service, last_seq FROM service_state")
        }

    down_here = {s for s in down if s in upstream}
    roots = sorted(s for s in down_here if not any(u in down_here for u in upstream[s]))

    # Memoized DFS over the DAG: roots reachable from a node through down nodes only.
    memo: dict[str, list[str]] = {}

    def reachable_roots(node: str) -> list[str]:
        if node in memo:
            return memo[node]
        if node in roots:
            memo[node] = [node]
            return memo[node]
        found: set[str] = set()
        for up in upstream.get(node, []):
            if up in down_here:
                found.update(reachable_roots(up))
        memo[node] = sorted(found)
        return memo[node]

    # All-all-down paths from a node up to a given root (for transparency).
    def all_down_paths(node: str, root: str) -> list[list[str]]:
        if node == root:
            return [[node]]
        paths: list[list[str]] = []
        for up in upstream.get(node, []):
            if up in down_here:
                for tail in all_down_paths(up, root):
                    paths.append([node] + tail)
        return paths

    actionable: list[dict] = []
    suppressed: list[dict] = []
    for svc in sorted(down_here):
        # A root cause (faulty node with no faulty upstream) is always
        # actionable; only non-root nodes can be suppressed.
        if svc in roots:
            actionable.append({"service": svc, "last_seq": seq_rows[svc]})
            continue
        svc_roots = reachable_roots(svc)
        if svc_roots:
            paths: list[list[str]] = []
            for root in svc_roots:
                paths.extend(all_down_paths(svc, root))
            suppressed.append(
                {
                    "service": svc,
                    "last_seq": seq_rows[svc],
                    "root_causes": svc_roots,
                    "paths": sorted(paths),
                }
            )
        else:
            actionable.append({"service": svc, "last_seq": seq_rows[svc]})

    all_nodes = set(upstream)
    unreported = sorted(all_nodes - set(seq_rows))
    healthy = sorted(s for s in seq_rows if s in all_nodes and s not in down_here)

    return {
        "graph_revision": revision,
        "graph_version": version,
        "services": sorted(all_nodes),
        "actionable_faults": actionable,
        "suppressed_faults": suppressed,
        "down_services": sorted(down_here),
        "unreported_services": unreported,
        "healthy_services": healthy,
    }
