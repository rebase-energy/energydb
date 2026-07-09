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

from typing import Any, NamedTuple
from uuid import UUID

import polars as pl

from energydb.runs import RunRow, run_upsert_cte


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


# LIKE-escape helper for bind-param prefixes: escape PG LIKE metacharacters so
# a literal path prefix is matched literally with ``... LIKE %s ESCAPE '\\'``.
# Subtree queries fetch the prefix into Python and pass it as a bind param.
_LIKE_TRANS = str.maketrans({"\\": r"\\", "%": r"\%", "_": r"\_"})


def _like_escape(s: str) -> str:
    """Escape PG LIKE metacharacters in a literal prefix.

    Use with ``... LIKE %s ESCAPE '\\'`` when the prefix is a bind parameter.
    """
    return s.translate(_LIKE_TRANS)


def derived_prefix_like(expr: str) -> str:
    r"""SQL fragment: the LIKE pattern ``<expr-escaped>/%`` for a server-side path.

    The single-round-trip hierarchy queries derive the subtree prefix inside
    the statement (e.g. from a joined root row), so it can't be escaped
    Python-side with :func:`_like_escape` — the LIKE metacharacter escape is
    inlined in SQL instead. Combine as
    ``path LIKE {derived_prefix_like('r.path')} ESCAPE '\'`` and pair with an
    explicit ``path = r.path`` term when the root row itself should match.
    """
    return rf"replace(replace(replace({expr}, E'\\', E'\\\\'), '%%', E'\\%%'), '_', E'\\_') || '/%%'"


# ---------------------------------------------------------------------------
# Node resolution: path -> uuid
# ---------------------------------------------------------------------------


async def resolve_node_uuid(conn, path: Path, *, start_uuid: UUID | None = None) -> UUID:
    """Resolve a path tuple like ``("Europe", "Sweden", "Lillgrund")`` to a uuid.

    Single indexed equality on the materialized ``node.path`` column
    (``node_path_uniq`` btree). When ``start_uuid`` is supplied the lookup
    keys off ``<start.path>/<joined-path>`` instead — one query with a
    self-join on the same column, still one round-trip.
    """
    if not path:
        if start_uuid is not None:
            return start_uuid
        raise ValueError("Empty path cannot be resolved.")

    joined = "/".join(path)
    if start_uuid is None:
        row = await (
            await conn.execute(
                "SELECT uuid FROM node WHERE path = %s",
                (joined,),
            )
        ).fetchone()
    else:
        # One round-trip: derive the start node's path inline and compose.
        row = await (
            await conn.execute(
                """
            SELECT n.uuid FROM node n, node s
            WHERE s.uuid = %s AND n.path = s.path || '/' || %s
            """,
                (start_uuid, joined),
            )
        ).fetchone()

    if row is None:
        if start_uuid is None:
            raise ValueError(f"Node not found: {joined}")
        raise ValueError(f"Node not found: {joined} (relative to {start_uuid})")
    return row[0]


async def resolve_paths_to_uuids(conn, paths: list[Path]) -> dict[Path, UUID]:
    """Bulk-resolve a list of path tuples to uuids in one round-trip.

    Joins each tuple with ``/`` and hits the unique ``node.path`` index in
    one ``ANY()`` scan. Input may contain duplicates; the returned dict is
    keyed by deduplicated path tuples. Raises ``ValueError`` if any path
    fails to resolve.
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

    joined_keys = ["/".join(p) for p in unique]
    rows = await (
        await conn.execute(
            "SELECT path, uuid FROM node WHERE path = ANY(%s)",
            (joined_keys,),
        )
    ).fetchall()

    out: dict[Path, UUID] = {}
    by_joined = {row[0]: row[1] for row in rows}
    for tup, joined in zip(unique, joined_keys, strict=True):
        node_uuid = by_joined.get(joined)
        if node_uuid is not None:
            out[tup] = node_uuid

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


async def resolve_subtree_uuids(conn, node_uuid: UUID) -> list[UUID]:
    """Return self + every descendant uuid for ``node_uuid``.

    Two PG round-trips: the root path lookup, then a prefix-LIKE on the
    materialized path. The escaped prefix is passed as a bind parameter so
    PG can extract the literal prefix at plan time and pick an Index Scan
    on ``ix_node_path_prefix`` (``text_pattern_ops``) — a column-source
    LIKE would force a Seq Scan. Net 2.4× faster than the single-query
    self-join on bench-scale data (44 ms → 18 ms at C=200).
    """
    row = await (
        await conn.execute(
            "SELECT path FROM node WHERE uuid = %s",
            (node_uuid,),
        )
    ).fetchone()
    if row is None:
        return []
    prefix = row[0]
    rows = await (
        await conn.execute(
            r"""
        SELECT uuid FROM node
        WHERE path = %s OR path LIKE %s || '/%%' ESCAPE '\'
        """,
            (prefix, _like_escape(prefix)),
        )
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# uuid -> path
# ---------------------------------------------------------------------------


async def resolve_path(conn, node_uuid: UUID) -> Path:
    """Return the full path tuple from root → node."""
    row = await (
        await conn.execute(
            "SELECT path FROM node WHERE uuid = %s",
            (node_uuid,),
        )
    ).fetchone()
    if row is None:
        raise ValueError(f"Node not found: uuid={node_uuid}")
    return tuple(row[0].split("/"))


# ---------------------------------------------------------------------------
# Edge resolution: (from_path, to_path, edge_type) -> uuid
# ---------------------------------------------------------------------------


async def resolve_edge_uuid(conn, from_path: Path, to_path: Path, edge_type: str) -> UUID:
    """Resolve an edge by its triple identity to its uuid."""
    paths = await resolve_paths_to_uuids(conn, [from_path, to_path])
    from_uuid = paths[tuple(from_path)]
    to_uuid = paths[tuple(to_path)]
    rows = await (
        await conn.execute(
            "SELECT uuid FROM edge WHERE edge_type = %s AND from_node_uuid = %s AND to_node_uuid = %s",
            (edge_type, from_uuid, to_uuid),
        )
    ).fetchall()
    if not rows:
        raise ValueError(f"Edge not found: type={edge_type!r} from={'/'.join(from_path)!r} to={'/'.join(to_path)!r}")
    return rows[0][0]


# ---------------------------------------------------------------------------
# Manifest resolution
# ---------------------------------------------------------------------------


_MANIFEST_REQUIRED = ("data_type", "name")
_MANIFEST_ROUTES = ("node_uuid", "path", "edge_uuid")


async def resolve_manifest(
    conn, manifest: pl.DataFrame, *, attach_path: bool = True, run: RunRow | None = None
) -> tuple[pl.DataFrame, ResolveSummary]:
    """Resolve a routing manifest to series metadata.

    Detects routing mode from the columns present:

    * ``node_uuid`` — direct lookup against ``series.node_uuid``.
    * ``path``      — ``Utf8`` path joined with ``/`` (e.g. ``"a/b/c"``);
      resolved to ``node_uuid`` first.
    * ``edge_uuid`` — direct lookup against ``series.edge_uuid``.

    The manifest must also carry ``data_type`` and ``name`` columns. Returns
    the original frame plus per-row ``series_id``, ``retention``, and
    ``canonical_unit``, alongside a :class:`ResolveSummary` carrying
    set-level signals (``has_overlapping``) that callers used to derive
    themselves from the resolved frame.

    ``attach_path=True`` (default, for read pipelines) also surfaces the
    DB-derived hierarchy paths on the resolved frame (``path`` for nodes;
    ``edge_type`` / ``from_path`` / ``to_path`` for edges) so post-read
    attach steps don't need another PG round-trip. ``attach_path=False``
    skips that JOIN entirely — write pipelines drop those columns before
    the CH insert and don't need them.
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

    # ``run`` (write pipelines only) upserts the energydb.runs row in the same
    # call, folded into the resolve statement as a data-modifying CTE — every
    # route resolves in ONE round-trip.
    if route == "edge_uuid":
        return await _resolve_manifest_by_owner(conn, manifest, owner_col="edge_uuid", attach_path=attach_path, run=run)
    if route == "path":
        # A single ``node ⋈ series`` query keyed by the materialized path.
        # ``attach_path=True`` (reads) also surfaces ``node_uuid`` on the
        # resolved frame for the post-read hierarchy attach.
        return await _resolve_manifest_by_path(conn, manifest, run=run, attach_path=attach_path)
    return await _resolve_manifest_by_owner(conn, manifest, owner_col="node_uuid", attach_path=attach_path, run=run)


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


# SQL fragments per (owner_col, attach_path) combination. Splitting the
# JOIN-to-owner-row from the core series scan lets writes opt out of the
# hierarchy attach entirely — saves ~20ms PG-side at scale=200.
_OWNER_PATH_SELECT = {
    ("node_uuid", True): (", n.path AS path", " LEFT JOIN node n ON n.uuid = s.node_uuid"),
    ("node_uuid", False): ("", ""),
    ("edge_uuid", True): (
        ", e.edge_type AS edge_type, fn.path AS from_path, tn.path AS to_path",
        " LEFT JOIN edge e  ON e.uuid = s.edge_uuid"
        " LEFT JOIN node fn ON fn.uuid = e.from_node_uuid"
        " LEFT JOIN node tn ON tn.uuid = e.to_node_uuid",
    ),
    ("edge_uuid", False): ("", ""),
}


async def _resolve_manifest_by_owner(
    conn,
    manifest: pl.DataFrame,
    *,
    owner_col: str,
    attach_path: bool,
    run: RunRow | None = None,
) -> tuple[pl.DataFrame, ResolveSummary]:
    """Resolve a manifest routed by ``owner_col`` (``node_uuid`` or ``edge_uuid``)
    against the series table.

    Workflow:

    1. Pre-hash the (owner, data_type, name) triple per manifest row via
       ``hash_rows`` — walks the three Arrow buffers once without
       materializing a concatenated string. Much cheaper than a multi-key
       composite join over Utf8 columns once the manifest is large.
    2. Dedupe via single-column unique on ``_triple_k``.
    3. One PG round-trip on ``ix_series_{owner_col}`` filtering by
       ``= ANY(unique_owners)``. We measured this beats ``UNNEST``-driven
       triple JOINs at every scale that matters: at 36k unique triples PG
       picks a Hash Join + Seq Scan plan for UNNEST (~300ms) vs an Index
       Scan for ``= ANY()`` (~80ms). Owner uuid comes back as text
       (``::text`` cast) so psycopg skips the UUID-object parse step
       (~15ms cheaper at scale=200).
    4. Attach ``series_id`` + ``retention`` + ``canonical_unit`` via a
       single left-join over a ``_triple_k``-keyed lookup frame.

    ``attach_path`` controls the hierarchy attach side:
    * True (read pipelines): JOIN through ``node`` / ``edge`` and surface
      ``path`` (node) or ``edge_type``/``from_path``/``to_path`` (edge) on
      the resolved frame. Downstream attach steps consume these without
      another PG hop.
    * False (write pipelines): skip the JOIN entirely — writes drop path
      before the CH insert anyway.

    ``timeseries_type`` is *not* attached per-row; the OVERLAPPING check
    moves up into :class:`ResolveSummary`.
    """
    manifest = _coerce_uuid_col(manifest, owner_col)
    is_edge = owner_col == "edge_uuid"

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
    # scan on ``ix_series_{owner_col}``. ``::text`` cast skips psycopg's
    # per-row UUID-object parse. ``run`` (write pipelines) folds the runs
    # upsert into this same statement as a leading data-modifying CTE, same
    # as the path route; its params bind before the owner ANY().
    unique_owners = list({t[0] for t in miss_triples})
    extra_cols, join_sql = _OWNER_PATH_SELECT[(owner_col, attach_path)]
    cte_sql, cte_params = ("", ())
    if run is not None:
        cte_sql, cte_params = run_upsert_cte(run)
    rows = await (
        await conn.execute(
            cte_sql
            + f"""
        SELECT s.{owner_col}::text, s.data_type, s.name, s.series_id,
               s.canonical_unit, s.timeseries_type, s.retention{extra_cols}
        FROM series s
        {join_sql}
        WHERE s.{owner_col} = ANY(%s::uuid[])
        """,
            (*cte_params, unique_owners),
        )
    ).fetchall()
    # Meta tuple layout:
    #   attach_path=False:  (series_id, canonical_unit, timeseries_type, retention)
    #   node + attach_path: (series_id, canonical_unit, timeseries_type, retention, path)
    #   edge + attach_path: (series_id, canonical_unit, timeseries_type, retention,
    #                        edge_type, from_path, to_path)
    triple_to_meta: dict[tuple[str, str, str], tuple] = {}
    for row in rows:
        owner, dt, name, sid, unit, ts_type, retention, *paths = row
        triple_to_meta[(owner, dt, name)] = (sid, unit, ts_type, retention, *paths)

    for owner, dt, name in miss_triples:
        if (owner, dt, name) not in triple_to_meta:
            raise ValueError(f"Series not registered for {owner_col}={owner!r}, data_type={dt!r}, name={name!r}.")

    hash_to_meta: dict[int, tuple] = {
        hash_val: triple_to_meta[triple] for hash_val, triple in zip(miss_keys, miss_triples, strict=True)
    }

    # 4. Build the per-hash lookup frame and attach via a single left-join.
    ks: list[int] = list(hash_to_meta.keys())
    metas: list[tuple] = list(hash_to_meta.values())
    lookup_data: dict[str, list] = {
        "_triple_k": ks,
        "series_id": [m[0] for m in metas],
        "retention": [m[3] for m in metas],
        "canonical_unit": [m[1] for m in metas],
    }
    lookup_schema: dict[str, Any] = {
        "_triple_k": pl.UInt64,
        "series_id": pl.Int64,
        "retention": pl.Utf8,
        "canonical_unit": pl.Utf8,
    }
    if attach_path:
        if is_edge:
            lookup_data["edge_type"] = [m[4] for m in metas]
            lookup_data["from_path"] = [m[5] for m in metas]
            lookup_data["to_path"] = [m[6] for m in metas]
            lookup_schema["edge_type"] = pl.Utf8
            lookup_schema["from_path"] = pl.Utf8
            lookup_schema["to_path"] = pl.Utf8
        else:
            lookup_data["path"] = [m[4] for m in metas]
            lookup_schema["path"] = pl.Utf8

    lookup_df = pl.DataFrame(lookup_data, schema=lookup_schema)
    # Drop the manifest's user-supplied path/edge-meta cols before joining so
    # we end up with one canonical set of DB-derived values.
    overlap = [c for c in lookup_df.columns if c != "_triple_k" and c in manifest.columns]
    if overlap:
        manifest = manifest.drop(overlap)
    resolved = manifest.join(lookup_df, on="_triple_k", how="left").drop("_triple_k")

    summary = ResolveSummary(
        has_overlapping=any(m[2] == "OVERLAPPING" for m in hash_to_meta.values()),
    )
    return resolved, summary


async def _resolve_manifest_by_path(
    conn, manifest: pl.DataFrame, *, run: RunRow | None = None, attach_path: bool = False
) -> tuple[pl.DataFrame, ResolveSummary]:
    """Resolve a path-routed manifest to series in ONE round-trip.

    Collapses the former two-step path resolve (:func:`resolve_paths_to_uuids`,
    then a ``node_uuid``-keyed series scan, plus an intermediate per-row
    ``node_uuid`` attach) into a single ``node`` ⋈ ``series`` query keyed by the
    materialized ``path``. Mirrors :func:`_resolve_manifest_by_owner`: hash the
    ``(path, data_type, name)`` triple, dedupe, one indexed ``ANY()`` scan,
    attach ``series_id`` / ``retention`` / ``canonical_unit`` via a
    ``_triple_k`` left-join.

    ``attach_path=True`` (read pipelines) additionally surfaces ``node_uuid``
    on the resolved frame for the post-read hierarchy attach. The manifest's
    ``path`` column needs no re-attach: the resolve joins on ``n.path``
    equality, so the manifest value IS the DB value.
    """
    if manifest["path"].dtype == pl.List(pl.Utf8):
        raise ValueError(
            "Manifest 'path' column must be Utf8 joined with '/'. "
            "Got List(Utf8) — pass 'a/b/c' instead of ['a','b','c']."
        )
    if manifest["path"].dtype != pl.Utf8:
        raise ValueError(f"Manifest 'path' column must be Utf8 joined with '/'; got {manifest['path'].dtype}.")
    non_null = manifest.height - manifest["path"].null_count()
    if non_null == 0:
        raise ValueError("No path values to resolve in manifest.")
    if manifest["path"].null_count() > 0:
        null_row = manifest.filter(pl.col("path").is_null()).row(0, named=True)
        raise ValueError(
            f"Series not registered for path=None, data_type={null_row['data_type']!r}, name={null_row['name']!r}."
        )

    # Hash + dedupe the (path, data_type, name) triple, same as the owner path.
    manifest = manifest.with_columns(manifest.select(["path", "data_type", "name"]).hash_rows().alias("_triple_k"))
    triples_df = manifest.select(["path", "data_type", "name", "_triple_k"]).unique(subset=["_triple_k"])
    miss_keys = triples_df["_triple_k"].to_list()
    miss_triples: list[tuple[str, str, str]] = list(
        zip(triples_df["path"].to_list(), triples_df["data_type"].to_list(), triples_df["name"].to_list(), strict=True)
    )

    unique_paths = list({t[0] for t in miss_triples})
    # Optionally fold the runs upsert into this same statement (one round-trip)
    # as a leading data-modifying CTE; its params bind before the path ANY().
    cte_sql, cte_params = ("", ())
    if run is not None:
        cte_sql, cte_params = run_upsert_cte(run)
    rows = await (
        await conn.execute(
            cte_sql
            + """
        SELECT n.path, s.data_type, s.name, s.series_id, s.canonical_unit, s.timeseries_type, s.retention,
               s.node_uuid::text
        FROM node n
        JOIN series s ON s.node_uuid = n.uuid
        WHERE n.path = ANY(%s)
        """,
            (*cte_params, unique_paths),
        )
    ).fetchall()
    triple_to_meta: dict[tuple[str, str, str], tuple] = {}
    for path, dt, name, sid, unit, ts_type, retention, node_uuid in rows:
        triple_to_meta[(path, dt, name)] = (sid, unit, ts_type, retention, node_uuid)

    for path, dt, name in miss_triples:
        if (path, dt, name) not in triple_to_meta:
            raise ValueError(f"Series not registered for path={path!r}, data_type={dt!r}, name={name!r}.")

    hash_to_meta: dict[int, tuple] = {
        hash_val: triple_to_meta[triple] for hash_val, triple in zip(miss_keys, miss_triples, strict=True)
    }
    metas = list(hash_to_meta.values())
    lookup_data: dict[str, list] = {
        "_triple_k": list(hash_to_meta.keys()),
        "series_id": [m[0] for m in metas],
        "retention": [m[3] for m in metas],
        "canonical_unit": [m[1] for m in metas],
    }
    lookup_schema: dict[str, Any] = {
        "_triple_k": pl.UInt64,
        "series_id": pl.Int64,
        "retention": pl.Utf8,
        "canonical_unit": pl.Utf8,
    }
    if attach_path:
        lookup_data["node_uuid"] = [m[4] for m in metas]
        lookup_schema["node_uuid"] = pl.Utf8
    lookup_df = pl.DataFrame(lookup_data, schema=lookup_schema)
    overlap = [c for c in lookup_df.columns if c != "_triple_k" and c in manifest.columns]
    if overlap:
        manifest = manifest.drop(overlap)
    resolved = manifest.join(lookup_df, on="_triple_k", how="left").drop("_triple_k")
    summary = ResolveSummary(has_overlapping=any(m[2] == "OVERLAPPING" for m in hash_to_meta.values()))
    return resolved, summary


__all__ = [
    "Path",
    "ResolveSummary",
    "resolve_edge_uuid",
    "resolve_manifest",
    "resolve_node_uuid",
    "resolve_path",
    "resolve_paths_to_uuids",
    "resolve_subtree_uuids",
]
