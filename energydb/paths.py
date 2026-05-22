"""Path-based addressing primitives for the fluent CLI.

After the UUID identity rewrite, ``node.uuid`` is the row PK and the value
held by every :class:`energydatamodel.Reference`. Paths are still a
user-friendly addressing form though — the fluent CLI lets you write
``client.get_node("Europe", "Sweden", "Lillgrund")`` and resolve the path
chain to a UUID with one indexed recursive CTE on ``(parent_uuid, name)``.

A node is identified at the storage layer by its ``uuid``; an edge by its
``uuid`` (or by the ``(from_node_uuid, to_node_uuid, edge_type)`` triple,
which is the upsert key on :class:`energydb.models.Edge`).
"""

from __future__ import annotations

import json
from typing import Any, NamedTuple
from uuid import UUID

import polars as pl


class ResolveSummary(NamedTuple):
    """Set-level signals derived during :func:`resolve_manifest`.

    Lifts ``OVERLAPPING`` detection out of the per-row resolved frame — it
    is a property of the *series set*, not the data rows, so paying
    linear-in-rows for it was wasted work. Powers the knowledge_time-
    required check in :func:`energydb._io.write_manifest`.
    """

    has_overlapping: bool


# A path is a tuple of names from the tree root.
Path = tuple[str, ...]


# ---------------------------------------------------------------------------
# Node resolution: path -> uuid
# ---------------------------------------------------------------------------


# Shared recursive tail for ``resolve_node_uuid`` — the walk and final
# SELECT are identical in both seed cases; only the seed row differs.
_RESOLVE_NODE_UUID_TAIL = """
        UNION ALL
        SELECT n.uuid, w.depth + 1
        FROM walk w
        JOIN energydb.node n
             ON n.parent_uuid = w.uuid
            AND n.name = (%s::text[])[w.depth + 1]
        WHERE w.depth < array_length(%s::text[], 1)
    )
    SELECT uuid FROM walk WHERE depth = array_length(%s::text[], 1)
"""


def resolve_node_uuid(conn, path: Path, *, start_uuid: UUID | None = None) -> UUID:
    """Resolve a path tuple like ``("Europe", "Sweden", "Lillgrund")`` to a uuid.

    One recursive CTE walking down the chain — single round-trip regardless
    of depth. With ``UNIQUE (parent_uuid, name)`` and the partial unique
    index on roots, the path either resolves to exactly one row or raises.

    When ``start_uuid`` is given the walk begins as a child of that node
    rather than at the root; this supports
    ``client.get_node(uuid=...).get_node("Child")``-style relative navigation.
    """
    if not path:
        if start_uuid is not None:
            return start_uuid
        raise ValueError("Empty path cannot be resolved.")

    p = list(path)
    if start_uuid is None:
        seed_sql = "SELECT uuid, 1 AS depth FROM energydb.node WHERE name = (%s::text[])[1] AND parent_uuid IS NULL"
        seed_params: tuple = (p,)
    else:
        seed_sql = "SELECT %s::uuid AS uuid, 0 AS depth"
        seed_params = (start_uuid,)

    sql = f"WITH RECURSIVE walk AS (\n        {seed_sql}{_RESOLVE_NODE_UUID_TAIL}"
    rows = conn.execute(sql, (*seed_params, p, p, p)).fetchall()

    if len(rows) == 0:
        raise ValueError(f"Node not found: {'/'.join(path)}")
    if len(rows) > 1:
        # Defensive: schema constraints should make this impossible.
        ids = [r[0] for r in rows]
        raise RuntimeError(f"Path {path!r} resolved to multiple uuids {ids}")
    return rows[0][0]


def resolve_paths_to_uuids(conn, paths: list[Path]) -> dict[Path, UUID]:
    """Bulk-resolve a list of path tuples to uuids in one round-trip.

    Input may contain duplicates; the returned dict is keyed by deduplicated
    path tuples. Raises ``ValueError`` if any path fails to resolve.
    """
    if not paths:
        return {}

    unique: list[Path] = []
    seen: set[Path] = set()
    for p in paths:
        t = tuple(p)
        if t not in seen:
            seen.add(t)
            unique.append(t)

    payload = json.dumps([list(p) for p in unique])

    rows = conn.execute(
        """
        WITH RECURSIVE inputs AS (
            SELECT
                idx::int AS idx,
                ARRAY(SELECT jsonb_array_elements_text(p)) AS path
            FROM jsonb_array_elements(%s::jsonb) WITH ORDINALITY AS t(p, idx)
        ),
        walk AS (
            SELECT i.idx, i.path, n.uuid, 1 AS depth
            FROM inputs i
            JOIN energydb.node n
                ON n.name = i.path[1] AND n.parent_uuid IS NULL
            WHERE array_length(i.path, 1) > 0
            UNION ALL
            SELECT w.idx, w.path, n.uuid, w.depth + 1
            FROM walk w
            JOIN energydb.node n
                ON n.parent_uuid = w.uuid
               AND n.name = w.path[w.depth + 1]
            WHERE w.depth < array_length(w.path, 1)
        )
        SELECT idx, uuid FROM walk WHERE depth = array_length(path, 1)
        """,
        (payload,),
    ).fetchall()

    out: dict[Path, UUID] = {}
    for idx, node_uuid in rows:
        out[unique[idx - 1]] = node_uuid

    missing = [p for p in unique if p not in out]
    if missing:
        rendered = ", ".join("/".join(p) for p in missing)
        raise ValueError(f"Could not resolve path(s): {rendered}")
    return out


# ---------------------------------------------------------------------------
# Node filter predicates (shared between Client.query_nodes and
# NodeScope._resolve_target_node_uuids)
# ---------------------------------------------------------------------------


def build_filter_conditions(
    where_filters: dict[str, Any],
    *,
    type_col: str,
    table_alias: str = "",
) -> tuple[list[str], list[Any]]:
    """Translate ``where_filters`` into SQL fragments + bind params.

    Recognized structural keys: ``type_col`` (typically ``"node_type"`` or
    ``"edge_type"``) and ``"name"`` — both compared against their own
    columns. Everything else is treated as a JSONB ``data->>`` predicate.
    Fragments must be joined with ``AND`` by the caller. ``table_alias``
    (e.g. ``"n"``) is prefixed to each column reference when the filter
    is composed against an aliased table in a join.
    """
    prefix = f"{table_alias}." if table_alias else ""
    conditions: list[str] = []
    params: list[Any] = []
    if type_col in where_filters:
        conditions.append(f"{prefix}{type_col} = %s")
        params.append(where_filters[type_col])
    if "name" in where_filters:
        conditions.append(f"{prefix}name = %s")
        params.append(where_filters["name"])
    for key, value in where_filters.items():
        if key in (type_col, "name"):
            continue
        conditions.append(f"{prefix}data->>%s = %s")
        params.append(key)
        params.append(str(value))
    return conditions, params


# ---------------------------------------------------------------------------
# Subtree
# ---------------------------------------------------------------------------


def resolve_subtree_uuids(conn, node_uuid: UUID) -> list[UUID]:
    rows = conn.execute(
        """
        WITH RECURSIVE subtree AS (
            SELECT uuid FROM energydb.node WHERE uuid = %s
            UNION ALL
            SELECT n.uuid FROM energydb.node n
            JOIN subtree s ON n.parent_uuid = s.uuid
        ) CYCLE uuid SET _is_cycle USING _cycle_path
        SELECT uuid FROM subtree WHERE NOT _is_cycle
        """,
        (node_uuid,),
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# uuid -> path
# ---------------------------------------------------------------------------


def resolve_path(conn, node_uuid: UUID) -> Path:
    """Return the full path tuple from root → node."""
    rows = conn.execute(
        """
        WITH RECURSIVE ancestors AS (
            SELECT uuid, name, parent_uuid, 0 AS depth
            FROM energydb.node WHERE uuid = %s
            UNION ALL
            SELECT n.uuid, n.name, n.parent_uuid, a.depth + 1
            FROM energydb.node n
            JOIN ancestors a ON n.uuid = a.parent_uuid
        ) CYCLE uuid SET _is_cycle USING _cycle_path
        SELECT name FROM ancestors WHERE NOT _is_cycle ORDER BY depth DESC
        """,
        (node_uuid,),
    ).fetchall()
    return tuple(r[0] for r in rows)


def fetch_node_hierarchy_bulk(conn, node_uuid_strs: list[str]) -> dict[str, str]:
    """Bulk: ``uuid → joined_path`` for many nodes in one round-trip.

    One recursive CTE walks each target's parent chain; the result is
    ``/``-joined at the Python side. Used by the read-pipeline hierarchy
    attach (:func:`energydb._join.attach_node_hierarchy` and friends).

    Input uuids are passed as strings (the form the manifest carries) and
    cast to ``uuid[]`` on the PG side.
    """
    if not node_uuid_strs:
        return {}
    rows = conn.execute(
        """
        WITH RECURSIVE ancestors AS (
            SELECT uuid AS target_uuid, uuid, name, parent_uuid, 0 AS depth
            FROM energydb.node WHERE uuid = ANY(%s::uuid[])
            UNION ALL
            SELECT a.target_uuid, n.uuid, n.name, n.parent_uuid, a.depth + 1
            FROM energydb.node n
            JOIN ancestors a ON n.uuid = a.parent_uuid
        ) CYCLE uuid SET _is_cycle USING _cycle_path
        SELECT target_uuid, name, depth FROM ancestors
        WHERE NOT _is_cycle
        ORDER BY target_uuid, depth DESC
        """,
        (node_uuid_strs,),
    ).fetchall()

    # depth iterated DESC so root segments arrive first, leaf last.
    parts: dict[str, list[str]] = {}
    for target_uuid, name, _depth in rows:
        parts.setdefault(str(target_uuid), []).append(name)
    return {uid: "/".join(segments) for uid, segments in parts.items()}


def fetch_edge_hierarchy_bulk(conn, edge_uuid_strs: list[str]) -> dict[str, tuple[str, str, str]]:
    """Bulk: ``uuid → (edge_type, from_node_uuid_str, to_node_uuid_str)``.

    Endpoint *paths* aren't fetched here — the caller resolves them by
    chaining through :func:`fetch_node_hierarchy_bulk` on the endpoint uuids.
    """
    if not edge_uuid_strs:
        return {}
    rows = conn.execute(
        "SELECT uuid, edge_type, from_node_uuid, to_node_uuid FROM energydb.edge WHERE uuid = ANY(%s::uuid[])",
        (edge_uuid_strs,),
    ).fetchall()
    return {str(uuid_): (edge_type, str(from_uuid), str(to_uuid)) for uuid_, edge_type, from_uuid, to_uuid in rows}


# ---------------------------------------------------------------------------
# Edge resolution: (from_path, to_path, edge_type) -> uuid
# ---------------------------------------------------------------------------


def resolve_edge_uuid(conn, from_path: Path, to_path: Path, edge_type: str) -> UUID:
    """Resolve an edge by its triple identity to its uuid."""
    paths = resolve_paths_to_uuids(conn, [from_path, to_path])
    from_uuid = paths[tuple(from_path)]
    to_uuid = paths[tuple(to_path)]
    rows = conn.execute(
        "SELECT uuid FROM energydb.edge WHERE edge_type = %s AND from_node_uuid = %s AND to_node_uuid = %s",
        (edge_type, from_uuid, to_uuid),
    ).fetchall()
    if not rows:
        raise ValueError(f"Edge not found: type={edge_type!r} from={'/'.join(from_path)!r} to={'/'.join(to_path)!r}")
    return rows[0][0]


# ---------------------------------------------------------------------------
# Manifest resolution
# ---------------------------------------------------------------------------


_MANIFEST_REQUIRED = ("data_type", "name")
_MANIFEST_ROUTES = ("node_uuid", "path", "edge_uuid")


def resolve_manifest(conn, manifest: pl.DataFrame) -> tuple[pl.DataFrame, ResolveSummary]:
    """Resolve a routing manifest to series metadata.

    Detects routing mode from the columns present:

    * ``node_uuid`` — direct lookup against ``energydb.series.node_uuid``.
    * ``path``      — ``Utf8`` path joined with ``/`` (e.g. ``"a/b/c"``);
      resolved to ``node_uuid`` first.
    * ``edge_uuid`` — direct lookup against ``energydb.series.edge_uuid``.

    The manifest must also carry ``data_type`` and ``name`` columns. Returns
    the original frame plus per-row ``series_id``, ``retention``, and
    ``canonical_unit``, alongside a :class:`ResolveSummary` carrying
    set-level signals (``has_overlapping``) that callers used to derive
    themselves from the resolved frame.
    """
    present_routes = [c for c in _MANIFEST_ROUTES if c in manifest.columns]
    if len(present_routes) == 0:
        raise ValueError(f"Manifest must include one of {list(_MANIFEST_ROUTES)} as a routing column.")
    if len(present_routes) > 1:
        raise ValueError(f"Manifest has ambiguous routing columns {present_routes}; provide exactly one.")
    route = present_routes[0]

    missing_required = [c for c in _MANIFEST_REQUIRED if c not in manifest.columns]
    if missing_required:
        raise ValueError(f"Manifest is missing required columns: {sorted(missing_required)}")

    manifest = manifest.with_columns(pl.col("data_type").cast(pl.Utf8).str.to_lowercase())

    if route == "edge_uuid":
        return _resolve_manifest_by_owner(conn, manifest, owner_col="edge_uuid")
    if route == "path":
        manifest = _attach_node_uuid_from_path(conn, manifest)
    return _resolve_manifest_by_owner(conn, manifest, owner_col="node_uuid")


def _attach_node_uuid_from_path(conn, manifest: pl.DataFrame) -> pl.DataFrame:
    """Resolve every ``path`` (Utf8, joined with ``/``) to a ``node_uuid`` and attach it.

    The manifest ``path`` column must be ``Utf8`` — strings like ``"a/b/c"``.
    List-shaped paths from earlier API versions are rejected explicitly so
    callers see a clear migration message rather than an opaque type error.
    """
    dtype = manifest["path"].dtype
    if dtype == pl.List(pl.Utf8):
        raise ValueError(
            "Manifest 'path' column must be Utf8 joined with '/'. "
            "Got List(Utf8) — pass 'a/b/c' instead of ['a','b','c']."
        )
    if dtype != pl.Utf8:
        raise ValueError(f"Manifest 'path' column must be Utf8 joined with '/'; got {dtype}.")

    path_strs = manifest["path"].to_list()
    unique_paths_str = list({p for p in path_strs if p is not None})
    if not unique_paths_str:
        return manifest.with_columns(pl.lit(None, dtype=pl.Utf8).cast(pl.Utf8).alias("node_uuid"))

    miss_tuples = [tuple(p.split("/")) for p in unique_paths_str]
    pg_map = resolve_paths_to_uuids(conn, miss_tuples)
    path_to_uuid_str: dict[str, str] = {
        "/".join(path_tuple): str(node_uuid) for path_tuple, node_uuid in pg_map.items()
    }

    node_uuids = [path_to_uuid_str[p] if p is not None else None for p in path_strs]
    return manifest.with_columns(pl.Series("node_uuid", node_uuids, dtype=pl.Utf8))


def _coerce_uuid_col(manifest: pl.DataFrame, col: str) -> pl.DataFrame:
    """Normalize a uuid routing column to ``str`` form for joining.

    Manifests may carry uuids as ``UUID`` objects, strings, or polars Utf8.
    Joins against PG-returned uuids work cleanly when both sides are str.
    Fast-path the common case where the column is already ``Utf8``.
    """
    if manifest[col].dtype == pl.Utf8:
        return manifest
    values = [str(v) if v is not None else None for v in manifest[col].to_list()]
    return manifest.with_columns(pl.Series(col, values, dtype=pl.Utf8))


def _resolve_manifest_by_owner(
    conn,
    manifest: pl.DataFrame,
    *,
    owner_col: str,
) -> tuple[pl.DataFrame, ResolveSummary]:
    """Resolve a manifest routed by ``owner_col`` (``node_uuid`` or ``edge_uuid``)
    against the series table.

    Workflow:

    1. Build a single ``_triple_k = hash_rows(owner, dt, name)`` column on
       the manifest. ``hash_rows`` walks the three Arrow buffers once
       without materializing a concatenated string — much cheaper than a
       multi-key composite join over Utf8 columns.
    2. ``manifest['_triple_k'].unique()`` — single-column dedupe, ~2× cheaper
       than the equivalent 4-column ``unique(subset=['_triple_k'])`` because
       it doesn't carry the string columns through the dedupe.
    3. Materialize the unique triples via a 4-column filtered unique, then
       bulk-fetch every series owned by the affected owners in one indexed
       scan on ``ix_series_{owner_col}``.
    4. Attach ``series_id`` + ``retention`` + ``canonical_unit`` via a single
       left-join over a small ``_triple_k``-keyed lookup frame — faster than
       N independent ``replace_strict`` calls because polars hash-joins
       once and copies all N values in a single pass.

    ``timeseries_type`` is *not* attached per-row; the OVERLAPPING check
    moves up into :class:`ResolveSummary`.
    """
    manifest = _coerce_uuid_col(manifest, owner_col)

    # Empty-or-all-null check first so the message matches the historical
    # contract; per-row null check second.
    non_null = manifest.height - manifest[owner_col].null_count()
    if non_null == 0:
        raise ValueError(f"No {owner_col} values to resolve in manifest.")
    if manifest[owner_col].null_count() > 0:
        null_row = manifest.filter(pl.col(owner_col).is_null()).row(0, named=True)
        raise ValueError(
            f"Series not registered for {owner_col}=None, "
            f"data_type={null_row['data_type']!r}, name={null_row['name']!r}."
        )

    # 1. Pre-hash the triple on the per-row manifest.
    manifest = manifest.with_columns(manifest.select([owner_col, "data_type", "name"]).hash_rows().alias("_triple_k"))

    # 2. Recover the unique (owner, data_type, name) triples for this manifest.
    triples_df = manifest.select([owner_col, "data_type", "name", "_triple_k"]).unique(subset=["_triple_k"])
    miss_keys = triples_df["_triple_k"].to_list()
    miss_owners = triples_df[owner_col].to_list()
    miss_dts = triples_df["data_type"].to_list()
    miss_names = triples_df["name"].to_list()
    miss_triples: list[tuple[str, str, str]] = list(zip(miss_owners, miss_dts, miss_names, strict=True))

    # 3. Bulk-fetch every series owned by the affected owners — one indexed
    # scan on ``ix_series_{owner_col}`` rather than per-triple lookups.
    unique_owners = list({t[0] for t in miss_triples})
    rows = conn.execute(
        f"""
        SELECT s.{owner_col}, s.data_type, s.name, s.series_id,
               s.canonical_unit, s.timeseries_type, s.retention
        FROM energydb.series s
        WHERE s.{owner_col} = ANY(%s::uuid[])
        """,
        (unique_owners,),
    ).fetchall()
    # Meta tuple layout: (series_id, canonical_unit, timeseries_type, retention).
    triple_to_meta: dict[tuple[str, str, str], tuple[int, str, str, str]] = {}
    for owner, dt, name, sid, unit, ts_type, retention in rows:
        triple_to_meta[(str(owner), dt, name)] = (sid, unit, ts_type, retention)

    for owner, dt, name in miss_triples:
        if (owner, dt, name) not in triple_to_meta:
            raise ValueError(f"Series not registered for {owner_col}={owner!r}, data_type={dt!r}, name={name!r}.")

    hash_to_meta: dict[int, tuple[int, str, str, str]] = {
        hash_val: triple_to_meta[triple] for hash_val, triple in zip(miss_keys, miss_triples, strict=True)
    }

    # 4. Build the per-hash lookup frame and attach via a single left-join.
    ks: list[int] = list(hash_to_meta.keys())
    metas: list[tuple[int, str, str, str]] = list(hash_to_meta.values())
    lookup_df = pl.DataFrame(
        {
            "_triple_k": ks,
            "series_id": [m[0] for m in metas],
            "retention": [m[3] for m in metas],
            "canonical_unit": [m[1] for m in metas],
        },
        schema={
            "_triple_k": pl.UInt64,
            "series_id": pl.Int64,
            "retention": pl.Utf8,
            "canonical_unit": pl.Utf8,
        },
    )
    resolved = manifest.join(lookup_df, on="_triple_k", how="left").drop("_triple_k")

    summary = ResolveSummary(
        has_overlapping=any(m[2] == "OVERLAPPING" for m in hash_to_meta.values()),
    )
    return resolved, summary


__all__ = [
    "Path",
    "ResolveSummary",
    "fetch_edge_hierarchy_bulk",
    "fetch_node_hierarchy_bulk",
    "resolve_edge_uuid",
    "resolve_manifest",
    "resolve_node_uuid",
    "resolve_path",
    "resolve_paths_to_uuids",
    "resolve_subtree_uuids",
]
