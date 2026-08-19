"""Path-based addressing primitives for the fluent CLI.

After the UUID identity rewrite, ``node.uuid`` is the row PK and the value
held by every :class:`energydatamodel.Reference`. Paths are still a
user-friendly addressing form though — the fluent CLI lets you write
``client.get_node("Europe", "Sweden", "Lillgrund")`` and resolve the path
chain to a UUID with one indexed recursive CTE on ``(parent_uuid, name)``.

A node is identified at the storage layer by its ``uuid``; an edge by its
``uuid`` (or by the ``(from_node_uuid, to_node_uuid, edge_type, name)``
quadruple, which is the unique key on :class:`energydb.models.Edge`).
``edge`` is a multigraph — several parallel edges may share the triple and
differ only by ``name`` — so every triple-addressed lookup here resolves a
unique match or raises :class:`~energydb.errors.AmbiguousEdgeError`; none of
them ever picks an arbitrary edge.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, NamedTuple
from uuid import UUID

import polars as pl

from energydb.errors import (
    AmbiguousEdgeError,
    EdgeNotFoundError,
    ManifestError,
    NodeNotFoundError,
    SeriesNotFoundError,
    ValidationError,
)
from energydb.models import SQL_SCHEMA_PREFIX as P
from energydb.runs import RunRow, run_upsert_cte

# What to do with a manifest triple that has no registered series. ``"raise"``
# is the default everywhere; ``"skip"`` is opt-in on reads only (writes must
# never silently drop rows — that would be data loss).
OnMissing = Literal["raise", "skip"]
_ON_MISSING_VALUES = ("raise", "skip")


def _check_on_missing(on_missing: str) -> None:
    """Reject a typo before it silently becomes ``"skip"``-like behaviour."""
    if on_missing not in _ON_MISSING_VALUES:
        raise ValidationError(f"on_missing must be one of {list(_ON_MISSING_VALUES)}, got {on_missing!r}")


class ResolveSummary(NamedTuple):
    """Set-level signals derived during :func:`resolve_manifest`.

    Lifts ``OVERLAPPING`` detection out of the per-row resolved frame — it
    is a property of the *series set*, not the data rows, so paying
    linear-in-rows for it was wasted work. Powers the knowledge_time-
    required check and the per-series ``skip_unchanged`` comparison in
    :func:`energydb._io.write_manifest`.

    The ids (rather than a bare boolean) are what let one write call dedupe
    FLAT and OVERLAPPING series by different keys: they become timedb's
    ``knowledge_time_scoped_series``. Collected while walking the resolved
    metadata that is already in hand, so no extra DB work.

    ``missing`` carries the manifest triples that resolved to no series —
    always present, and zero-row (with the route's column schema) unless
    ``on_missing="skip"`` actually dropped something. Riding on the summary
    keeps the resolver's ``(resolved, summary)`` return arity stable.
    """

    overlapping_series_ids: frozenset[int]
    missing: pl.DataFrame

    @property
    def has_overlapping(self) -> bool:
        """True when any resolved series is ``OVERLAPPING``."""
        return bool(self.overlapping_series_ids)


def _resolvable_keys(hash_to_meta: dict[int, tuple]) -> pl.DataFrame:
    """A ``_triple_k`` frame to semi-join a manifest down to its resolved rows.

    A frame rather than ``is_in``: the key dtype stays explicit (so the empty
    everything-was-missing case needs no special-casing) and it reuses the
    ``_triple_k`` join the resolvers already run on.
    """
    return pl.DataFrame({"_triple_k": list(hash_to_meta)}, schema={"_triple_k": pl.UInt64})


def _missing_frame(cols: Sequence[str], rows: Sequence[tuple[str, ...]]) -> pl.DataFrame:
    """The unresolved routing triples as a ``Utf8`` frame keyed by ``cols``.

    ``rows`` come from the resolver's deduplicated triple set, so the frame is
    already unique. Zero-row (correct schema, never a schema-less empty frame)
    when everything resolved — callers can select/join on it unconditionally.
    """
    return pl.DataFrame(
        {c: [r[i] for r in rows] for i, c in enumerate(cols)},
        schema={c: pl.Utf8 for c in cols},
    )


# How many unresolved triples the error message spells out before truncating.
_MISSING_PREVIEW = 5


def _series_not_found_error(
    *, route: str, cols: Sequence[str], unresolved: Sequence[tuple[str, ...]]
) -> SeriesNotFoundError:
    """Build the all-triples :class:`SeriesNotFoundError` for a failed resolve.

    The resolvers used to raise on the *first* unresolvable triple, so a
    manifest of 1,500 series reported one gap per retry. The message now leads
    with that same first-triple sentence (callers match on the
    ``Series not registered for <route>=…`` prefix), then names the total and
    spells out up to :data:`_MISSING_PREVIEW` of them; ``.missing`` on the
    exception carries every one structurally.
    """
    rendered = [", ".join(f"{c}={v!r}" for c, v in zip(cols, t, strict=True)) for t in unresolved[:_MISSING_PREVIEW]]
    message = f"Series not registered for {rendered[0]}."
    if len(unresolved) > 1:
        rest, remaining = rendered[1:], len(unresolved) - 1
        truncated = f" ({len(rest)} of {remaining} shown)" if remaining > len(rest) else ""
        message += f" {len(unresolved)} triples in this manifest are unresolved; also {'; '.join(rest)}{truncated}."
    return SeriesNotFoundError(message, route=route, missing=unresolved)


def _overlapping_ids(hash_to_meta: dict[int, tuple]) -> frozenset[int]:
    """The ``series_id``s of the OVERLAPPING series in a resolved metadata map.

    Every resolver's meta tuple starts ``(series_id, canonical_unit,
    timeseries_type, retention, ...)``, so one shape serves all three routes.
    """
    return frozenset(m[0] for m in hash_to_meta.values() if m[2] == "OVERLAPPING")


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
        raise ValidationError("Empty path cannot be resolved.")

    joined = "/".join(path)
    if start_uuid is None:
        row = await (
            await conn.execute(
                f"SELECT uuid FROM {P}node WHERE path = %s",
                (joined,),
            )
        ).fetchone()
    else:
        # One round-trip: derive the start node's path inline and compose.
        row = await (
            await conn.execute(
                f"""
            SELECT n.uuid FROM {P}node n, {P}node s
            WHERE s.uuid = %s AND n.path = s.path || '/' || %s
            """,
                (start_uuid, joined),
            )
        ).fetchone()

    if row is None:
        if start_uuid is None:
            raise NodeNotFoundError(f"Node not found: {joined}", path=joined)
        raise NodeNotFoundError(f"Node not found: {joined} (relative to {start_uuid})", path=joined)
    return row[0]


async def resolve_paths_to_uuids(conn, paths: list[Path]) -> dict[Path, UUID]:
    """Bulk-resolve a list of path tuples to uuids in one round-trip.

    Joins each tuple with ``/`` and hits the unique ``node.path`` index in
    one ``ANY()`` scan. Input may contain duplicates; the returned dict is
    keyed by deduplicated path tuples. Raises
    :class:`~energydb.errors.NodeNotFoundError` if any path fails to resolve.
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
            f"SELECT path, uuid FROM {P}node WHERE path = ANY(%s)",
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
        # ``path`` only when a single path missed — with several, no one value
        # identifies the failure, so leave it unset rather than pick a winner.
        raise NodeNotFoundError(
            f"Could not resolve path(s): {rendered}",
            path="/".join(missing[0]) if len(missing) == 1 else None,
        )
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
            f"SELECT path FROM {P}node WHERE uuid = %s",
            (node_uuid,),
        )
    ).fetchone()
    if row is None:
        return []
    prefix = row[0]
    rows = await (
        await conn.execute(
            rf"""
        SELECT uuid FROM {P}node
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
            f"SELECT path FROM {P}node WHERE uuid = %s",
            (node_uuid,),
        )
    ).fetchone()
    if row is None:
        raise NodeNotFoundError(f"Node not found: uuid={node_uuid}", uuid=node_uuid)
    return tuple(row[0].split("/"))


# ---------------------------------------------------------------------------
# Edge resolution: (from_path, to_path, edge_type[, name]) -> uuid
# ---------------------------------------------------------------------------


def edge_address_repr(from_path: str, to_path: str, edge_type: str, name: str | None) -> str:
    """``type=… from=… to=…`` (plus ``name=…``) for edge error messages."""
    rendered = f"type={edge_type!r} from={from_path!r} to={to_path!r}"
    return rendered if name is None else f"{rendered} name={name!r}"


def ambiguous_edge_error(
    *,
    from_path: str,
    to_path: str,
    edge_type: str,
    matches: Sequence[tuple[Any, str | None]],
    fix: str = "pass name= to address one of them",
) -> AmbiguousEdgeError:
    """Build the :class:`AmbiguousEdgeError` for a triple matching several edges.

    ``matches`` are ``(uuid, name)`` pairs; they are sorted by name (unnamed
    first) so the message and the structured ``matches`` list are stable
    across calls rather than following PostgreSQL's row order.
    """
    ordered = sorted(matches, key=lambda m: (m[1] is not None, m[1] or ""))
    rendered = ", ".join(repr(name) for _uuid, name in ordered)
    return AmbiguousEdgeError(
        f"Edge address is ambiguous: {edge_address_repr(from_path, to_path, edge_type, None)} "
        f"matches {len(ordered)} parallel edges with names {rendered}. "
        f"These are distinct edges — {fix}, or address the edge by uuid.",
        from_path=from_path,
        to_path=to_path,
        edge_type=edge_type,
        matches=[{"uuid": uuid_, "name": name} for uuid_, name in ordered],
    )


async def resolve_edge_uuid(
    conn,
    from_path: Path,
    to_path: Path,
    edge_type: str,
    *,
    name: str | None = None,
) -> UUID:
    """Resolve an edge by its ``(from, to, type[, name])`` identity to its uuid.

    ``edge`` is a multigraph: the triple alone can match several parallel
    edges, which are told apart by ``name``. Pass ``name`` to address one of
    them directly. Without it a triple that matches exactly one edge resolves
    (the whole pre-multigraph corpus), and one that matches several raises
    :class:`~energydb.errors.AmbiguousEdgeError` rather than picking a winner.
    """
    paths = await resolve_paths_to_uuids(conn, [from_path, to_path])
    from_uuid = paths[tuple(from_path)]
    to_uuid = paths[tuple(to_path)]
    name_sql, name_params = ("", ()) if name is None else (" AND name = %s", (name,))
    rows = await (
        await conn.execute(
            f"SELECT uuid, name FROM {P}edge "
            f"WHERE edge_type = %s AND from_node_uuid = %s AND to_node_uuid = %s{name_sql}",
            (edge_type, from_uuid, to_uuid, *name_params),
        )
    ).fetchall()
    joined_from, joined_to = "/".join(from_path), "/".join(to_path)
    if not rows:
        raise EdgeNotFoundError(
            f"Edge not found: {edge_address_repr(joined_from, joined_to, edge_type, name)}",
            from_path=joined_from,
            to_path=joined_to,
            edge_type=edge_type,
            name=name,
        )
    if len(rows) > 1:
        # Unreachable when ``name`` was given — ``edge_uniq`` makes the
        # quadruple unique — so this only fires for a bare triple.
        raise ambiguous_edge_error(
            from_path=joined_from,
            to_path=joined_to,
            edge_type=edge_type,
            matches=rows,
        )
    return rows[0][0]


# ---------------------------------------------------------------------------
# Manifest resolution
# ---------------------------------------------------------------------------


_MANIFEST_REQUIRED = ("data_type", "name")
_MANIFEST_ROUTES = ("node_uuid", "path", "edge_uuid")
# Edge-triple routing: all three columns must be present together, resolved
# server-side the same way node ``path`` is.
_MANIFEST_EDGE_TRIPLE = ("from_path", "to_path", "edge_type")
# Optional fourth routing column, disambiguating parallel edges. Null means
# "the unnamed edge of this triple" — the manifest counterpart of passing
# ``name=None`` is simply leaving the column out.
_MANIFEST_EDGE_NAME = "edge_name"
# The edge-triple route's identity is the whole quintuple, so its unresolved-key
# reporting carries all five columns where the single-column routes carry three
# (six when ``edge_name`` is routing too).
_MISSING_EDGE_COLS = (*_MANIFEST_EDGE_TRIPLE, "data_type", "name")
_MISSING_EDGE_NAMED_COLS = (*_MANIFEST_EDGE_TRIPLE, _MANIFEST_EDGE_NAME, "data_type", "name")


async def resolve_manifest(
    conn,
    manifest: pl.DataFrame,
    *,
    attach_path: bool = True,
    run: RunRow | None = None,
    on_missing: OnMissing = "raise",
) -> tuple[pl.DataFrame, ResolveSummary]:
    """Resolve a routing manifest to series metadata.

    Detects routing mode from the columns present:

    * ``node_uuid`` — direct lookup against ``series.node_uuid``.
    * ``path``      — ``Utf8`` path joined with ``/`` (e.g. ``"a/b/c"``);
      resolved to ``node_uuid`` first.
    * ``edge_uuid`` — direct lookup against ``series.edge_uuid``.
    * ``from_path`` + ``to_path`` + ``edge_type`` — a ``Utf8`` edge identity
      (all three columns required together), resolved to ``edge_uuid`` via the
      edge's endpoint nodes the same way ``path`` is resolved for nodes. An
      optional fourth ``edge_name`` column (``Utf8``, null = the unnamed edge)
      picks one of several *parallel* edges sharing that triple; without it a
      triple matching more than one edge raises
      :class:`~energydb.errors.AmbiguousEdgeError`.

    The manifest must also carry ``data_type`` and ``name`` columns. Returns
    the original frame plus per-row ``series_id``, ``retention``, and
    ``canonical_unit``, alongside a :class:`ResolveSummary` carrying
    set-level signals (``has_overlapping``) that callers used to derive
    themselves from the resolved frame.

    ``attach_path=True`` (default, for read pipelines) also surfaces the
    DB-derived hierarchy paths on the resolved frame (``path`` for nodes;
    ``edge_type`` / ``edge_name`` / ``from_path`` / ``to_path`` for edges) so post-read
    attach steps don't need another PG round-trip. ``attach_path=False``
    skips that JOIN entirely — write pipelines drop those columns before
    the CH insert and don't need them.

    ``on_missing`` governs unregistered series only:

    * ``"raise"`` (default) — :class:`~energydb.errors.SeriesNotFoundError`
      naming *every* unresolved triple, not just the first one hit.
    * ``"skip"`` — unresolved rows are dropped from the resolved frame and
      reported on ``summary.missing``, so one unregistered series can't fail a
      1,500-series read. Read pipelines only; the write pipeline never passes
      it, because silently dropping writes is data loss.

    Structural problems (missing/ambiguous routing column, wrong dtype, null
    routing values, missing ``data_type``/``name``) raise either way — they are
    caller bugs, not catalog gaps.
    """
    _check_on_missing(on_missing)
    present_routes = [c for c in _MANIFEST_ROUTES if c in manifest.columns]
    # The edge triple is a single route spread across three columns. Treat it as
    # present only when all three are given; a strict subset is a usage error.
    triple_cols = [c for c in _MANIFEST_EDGE_TRIPLE if c in manifest.columns]
    if triple_cols:
        if len(triple_cols) != len(_MANIFEST_EDGE_TRIPLE):
            missing = [c for c in _MANIFEST_EDGE_TRIPLE if c not in manifest.columns]
            raise ManifestError(
                f"Edge-triple routing requires all of {list(_MANIFEST_EDGE_TRIPLE)}; missing {missing}."
            )
        present_routes.append("edge_triple")
    elif _MANIFEST_EDGE_NAME in manifest.columns:
        # ``edge_name`` narrows an edge triple; on its own it routes nothing.
        raise ManifestError(
            f"Manifest has {_MANIFEST_EDGE_NAME!r} without the edge triple "
            f"{list(_MANIFEST_EDGE_TRIPLE)}; {_MANIFEST_EDGE_NAME!r} only "
            f"disambiguates parallel edges of a triple-routed manifest."
        )

    if len(present_routes) == 0:
        raise ManifestError(
            f"Manifest must include one of {list(_MANIFEST_ROUTES)} "
            f"or {list(_MANIFEST_EDGE_TRIPLE)} as a routing column."
        )
    if len(present_routes) > 1:
        raise ManifestError(f"Manifest has ambiguous routing columns {present_routes}; provide exactly one.")
    route = present_routes[0]

    missing_required = [c for c in _MANIFEST_REQUIRED if c not in manifest.columns]
    if missing_required:
        raise ManifestError(f"Manifest is missing required columns: {sorted(missing_required)}")

    manifest = manifest.with_columns(pl.col("data_type").cast(pl.Utf8).str.to_lowercase())

    # ``run`` (write pipelines only) upserts the energydb.runs row in the same
    # call, folded into the resolve statement as a data-modifying CTE — every
    # route resolves in ONE round-trip.
    if route == "edge_uuid":
        return await _resolve_manifest_by_owner(
            conn, manifest, owner_col="edge_uuid", attach_path=attach_path, run=run, on_missing=on_missing
        )
    if route == "edge_triple":
        # A single ``edge ⋈ node ⋈ node ⋈ series`` query keyed by the endpoint
        # paths + edge_type. Always attaches ``edge_uuid`` so downstream edge
        # detection and projection work like the ``edge_uuid`` route.
        return await _resolve_manifest_by_edge_triple(
            conn, manifest, run=run, attach_path=attach_path, on_missing=on_missing
        )
    if route == "path":
        # A single ``node ⋈ series`` query keyed by the materialized path.
        # ``attach_path=True`` (reads) also surfaces ``node_uuid`` on the
        # resolved frame for the post-read hierarchy attach.
        return await _resolve_manifest_by_path(conn, manifest, run=run, attach_path=attach_path, on_missing=on_missing)
    return await _resolve_manifest_by_owner(
        conn, manifest, owner_col="node_uuid", attach_path=attach_path, run=run, on_missing=on_missing
    )


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
    ("node_uuid", True): (", n.path AS path", f" LEFT JOIN {P}node n ON n.uuid = s.node_uuid"),
    ("node_uuid", False): ("", ""),
    ("edge_uuid", True): (
        ", e.edge_type AS edge_type, e.name AS edge_name, fn.path AS from_path, tn.path AS to_path",
        f" LEFT JOIN {P}edge e  ON e.uuid = s.edge_uuid"
        f" LEFT JOIN {P}node fn ON fn.uuid = e.from_node_uuid"
        f" LEFT JOIN {P}node tn ON tn.uuid = e.to_node_uuid",
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
    on_missing: OnMissing = "raise",
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
      ``path`` (node) or ``edge_type``/``edge_name``/``from_path``/``to_path``
      (edge) on the resolved frame. Downstream attach steps consume these without
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
        raise ManifestError(f"No {owner_col} values to resolve in manifest.")
    if manifest[owner_col].null_count() > 0:
        null_row = manifest.filter(pl.col(owner_col).is_null()).row(0, named=True)
        raise ManifestError(
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
        FROM {P}series s
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
    #                        edge_type, edge_name, from_path, to_path)
    triple_to_meta: dict[tuple[str, str, str], tuple] = {}
    for row in rows:
        owner, dt, name, sid, unit, ts_type, retention, *paths = row
        triple_to_meta[(owner, dt, name)] = (sid, unit, ts_type, retention, *paths)

    unresolved = [t for t in miss_triples if t not in triple_to_meta]
    if unresolved and on_missing == "raise":
        raise _series_not_found_error(route=owner_col, cols=(owner_col, "data_type", "name"), unresolved=unresolved)

    hash_to_meta: dict[int, tuple] = {
        hash_val: triple_to_meta[triple]
        for hash_val, triple in zip(miss_keys, miss_triples, strict=True)
        if triple in triple_to_meta
    }
    if unresolved:
        # ``on_missing="skip"``: drop the unresolvable rows *before* the join.
        # The left-join would otherwise leave them carrying a null ``series_id``
        # and ``_project_meta``'s ``.unique()`` would emit a null-id series into
        # the read.
        manifest = manifest.join(_resolvable_keys(hash_to_meta), on="_triple_k", how="semi")

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
            lookup_data["edge_name"] = [m[5] for m in metas]
            lookup_data["from_path"] = [m[6] for m in metas]
            lookup_data["to_path"] = [m[7] for m in metas]
            lookup_schema["edge_type"] = pl.Utf8
            lookup_schema["edge_name"] = pl.Utf8
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
        overlapping_series_ids=_overlapping_ids(hash_to_meta),
        missing=_missing_frame((owner_col, "data_type", "name"), unresolved),
    )
    return resolved, summary


async def _resolve_manifest_by_path(
    conn,
    manifest: pl.DataFrame,
    *,
    run: RunRow | None = None,
    attach_path: bool = False,
    on_missing: OnMissing = "raise",
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
        raise ManifestError(
            "Manifest 'path' column must be Utf8 joined with '/'. "
            "Got List(Utf8) — pass 'a/b/c' instead of ['a','b','c']."
        )
    if manifest["path"].dtype != pl.Utf8:
        raise ManifestError(f"Manifest 'path' column must be Utf8 joined with '/'; got {manifest['path'].dtype}.")
    non_null = manifest.height - manifest["path"].null_count()
    if non_null == 0:
        raise ManifestError("No path values to resolve in manifest.")
    if manifest["path"].null_count() > 0:
        null_row = manifest.filter(pl.col("path").is_null()).row(0, named=True)
        raise ManifestError(
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
            + f"""
        SELECT n.path, s.data_type, s.name, s.series_id, s.canonical_unit, s.timeseries_type, s.retention,
               s.node_uuid::text
        FROM {P}node n
        JOIN {P}series s ON s.node_uuid = n.uuid
        WHERE n.path = ANY(%s)
        """,
            (*cte_params, unique_paths),
        )
    ).fetchall()
    triple_to_meta: dict[tuple[str, str, str], tuple] = {}
    for path, dt, name, sid, unit, ts_type, retention, node_uuid in rows:
        triple_to_meta[(path, dt, name)] = (sid, unit, ts_type, retention, node_uuid)

    unresolved = [t for t in miss_triples if t not in triple_to_meta]
    if unresolved and on_missing == "raise":
        raise _series_not_found_error(route="path", cols=("path", "data_type", "name"), unresolved=unresolved)

    hash_to_meta: dict[int, tuple] = {
        hash_val: triple_to_meta[triple]
        for hash_val, triple in zip(miss_keys, miss_triples, strict=True)
        if triple in triple_to_meta
    }
    if unresolved:
        # ``on_missing="skip"`` — see the note in ``_resolve_manifest_by_owner``.
        manifest = manifest.join(_resolvable_keys(hash_to_meta), on="_triple_k", how="semi")
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
    summary = ResolveSummary(
        overlapping_series_ids=_overlapping_ids(hash_to_meta),
        missing=_missing_frame(("path", "data_type", "name"), unresolved),
    )
    return resolved, summary


async def _resolve_manifest_by_edge_triple(
    conn,
    manifest: pl.DataFrame,
    *,
    run: RunRow | None = None,
    attach_path: bool = False,
    on_missing: OnMissing = "raise",
) -> tuple[pl.DataFrame, ResolveSummary]:
    """Resolve an edge-triple-routed manifest to series in ONE round-trip.

    The edge analogue of :func:`_resolve_manifest_by_path`: instead of routing
    by a single materialized ``node.path``, route by the ``(from_path, to_path,
    edge_type)`` identity — plus the optional ``edge_name`` fourth column —
    joining the edge to its endpoint nodes' materialized paths. Hashes the
    ``(from_path, to_path, edge_type[, edge_name], data_type, name)`` key,
    dedupes, and issues one ``edge ⋈ node ⋈ node ⋈ series`` query with a
    single-column ``ANY()`` per triple component (a cartesian superset,
    trimmed back to the exact keys by the ``_triple_k`` left-join).

    ``series`` is LEFT-JOINed so an edge with no matching series still shows
    up: that is what makes the multigraph ambiguity check see *every* edge on
    a triple, not just the ones that happen to carry the requested series. A
    triple matching several edges without an ``edge_name`` column raises
    :class:`~energydb.errors.AmbiguousEdgeError` listing the candidates.

    Always surfaces ``edge_uuid`` and ``edge_name`` on the resolved frame
    regardless of ``attach_path`` — downstream edge detection
    (:func:`_finish_read`) and projection (:func:`_project_meta`) key off
    them. ``from_path`` / ``to_path`` / ``edge_type`` need no re-attach: the
    join is on path/type equality, so the manifest values ARE the DB values.
    """
    for col in _MANIFEST_EDGE_TRIPLE:
        if manifest[col].dtype == pl.List(pl.Utf8):
            raise ManifestError(
                f"Manifest {col!r} column must be Utf8 joined with '/'. "
                f"Got List(Utf8) — pass 'a/b/c' instead of ['a','b','c']."
            )
        if manifest[col].dtype != pl.Utf8:
            raise ManifestError(f"Manifest {col!r} column must be Utf8; got {manifest[col].dtype}.")

    # ``edge_name`` is optional; when present it joins the routing key. Nulls
    # are meaningful there (the unnamed edge), so unlike the triple columns it
    # gets no null check — only a dtype one.
    route_by_name = _MANIFEST_EDGE_NAME in manifest.columns
    if route_by_name and manifest[_MANIFEST_EDGE_NAME].dtype != pl.Utf8:
        raise ManifestError(
            f"Manifest {_MANIFEST_EDGE_NAME!r} column must be Utf8 "
            f"(null for an unnamed edge); got {manifest[_MANIFEST_EDGE_NAME].dtype}."
        )

    non_null = (
        manifest.height - manifest.filter(pl.any_horizontal(pl.col(c).is_null() for c in _MANIFEST_EDGE_TRIPLE)).height
    )
    if non_null == 0:
        raise ManifestError("No edge-triple values to resolve in manifest.")
    null_mask = pl.any_horizontal(pl.col(c).is_null() for c in _MANIFEST_EDGE_TRIPLE)
    if manifest.filter(null_mask).height > 0:
        null_row = manifest.filter(null_mask).row(0, named=True)
        raise ManifestError(
            f"Series not registered for from_path={null_row['from_path']!r}, "
            f"to_path={null_row['to_path']!r}, edge_type={null_row['edge_type']!r}, "
            f"data_type={null_row['data_type']!r}, name={null_row['name']!r}."
        )

    routing_cols = [*_MANIFEST_EDGE_TRIPLE, _MANIFEST_EDGE_NAME] if route_by_name else list(_MANIFEST_EDGE_TRIPLE)
    key_cols = [*routing_cols, "data_type", "name"]
    missing_cols = _MISSING_EDGE_NAMED_COLS if route_by_name else _MISSING_EDGE_COLS
    manifest = manifest.with_columns(manifest.select(key_cols).hash_rows().alias("_triple_k"))
    keys_df = manifest.select([*key_cols, "_triple_k"]).unique(subset=["_triple_k"])
    miss_keys = keys_df["_triple_k"].to_list()
    miss_tuples: list[tuple] = list(zip(*(keys_df[c].to_list() for c in key_cols), strict=True))

    unique_from = list({t[0] for t in miss_tuples})
    unique_to = list({t[1] for t in miss_tuples})
    unique_etype = list({t[2] for t in miss_tuples})
    # Fold the runs upsert into this same statement (one round-trip) as a leading
    # data-modifying CTE; its params bind before the triple ANY() params.
    cte_sql, cte_params = ("", ())
    if run is not None:
        cte_sql, cte_params = run_upsert_cte(run)
    rows = await (
        await conn.execute(
            cte_sql
            + f"""
        SELECT fn.path, tn.path, e.edge_type, e.name, s.data_type, s.name,
               s.series_id, s.canonical_unit, s.timeseries_type, s.retention, e.uuid::text
        FROM {P}edge e
        JOIN {P}node fn ON fn.uuid = e.from_node_uuid
        JOIN {P}node tn ON tn.uuid = e.to_node_uuid
        LEFT JOIN {P}series s ON s.edge_uuid = e.uuid
        WHERE fn.path = ANY(%s) AND tn.path = ANY(%s) AND e.edge_type = ANY(%s)
        """,
            (*cte_params, unique_from, unique_to, unique_etype),
        )
    ).fetchall()

    # Every edge the (superset) triple scan saw, keyed by triple — the input to
    # the ambiguity check. Series-less edges are in here too, thanks to the LEFT
    # JOIN, so an ambiguous address is caught even when only one of the parallel
    # edges carries the requested series.
    edges_by_triple: dict[tuple[str, str, str], dict[str, str | None]] = {}
    key_to_meta: dict[tuple, tuple] = {}
    for fp, tp, et, edge_name, dt, sname, sid, unit, ts_type, retention, edge_uuid in rows:
        edges_by_triple.setdefault((fp, tp, et), {})[edge_uuid] = edge_name
        if sid is None:
            continue  # LEFT-JOIN row for a series-less edge
        routing_key = (fp, tp, et, edge_name) if route_by_name else (fp, tp, et)
        key_to_meta[(*routing_key, dt, sname)] = (sid, unit, ts_type, retention, edge_uuid, edge_name)

    if not route_by_name:
        # Ambiguity is an addressing error, not a catalog gap: it raises even
        # under ``on_missing="skip"``, and before the missing-series report, so
        # "which of these edges did you mean?" always wins over "no series".
        for tup in miss_tuples:
            candidates = edges_by_triple.get(tup[:3], {})
            if len(candidates) > 1:
                raise ambiguous_edge_error(
                    from_path=tup[0],
                    to_path=tup[1],
                    edge_type=tup[2],
                    matches=[(uuid_, name) for uuid_, name in candidates.items()],
                    fix=f"add an {_MANIFEST_EDGE_NAME!r} column to the manifest",
                )

    unresolved = [t for t in miss_tuples if t not in key_to_meta]
    if unresolved and on_missing == "raise":
        raise _series_not_found_error(route="edge_triple", cols=missing_cols, unresolved=unresolved)

    hash_to_meta: dict[int, tuple] = {
        hash_val: key_to_meta[key] for hash_val, key in zip(miss_keys, miss_tuples, strict=True) if key in key_to_meta
    }
    if unresolved:
        # ``on_missing="skip"`` — see the note in ``_resolve_manifest_by_owner``.
        manifest = manifest.join(_resolvable_keys(hash_to_meta), on="_triple_k", how="semi")
    metas = list(hash_to_meta.values())
    lookup_df = pl.DataFrame(
        {
            "_triple_k": list(hash_to_meta.keys()),
            "series_id": [m[0] for m in metas],
            "retention": [m[3] for m in metas],
            "canonical_unit": [m[1] for m in metas],
            "edge_uuid": [m[4] for m in metas],
            "edge_name": [m[5] for m in metas],
        },
        schema={
            "_triple_k": pl.UInt64,
            "series_id": pl.Int64,
            "retention": pl.Utf8,
            "canonical_unit": pl.Utf8,
            "edge_uuid": pl.Utf8,
            "edge_name": pl.Utf8,
        },
    )
    overlap = [c for c in lookup_df.columns if c != "_triple_k" and c in manifest.columns]
    if overlap:
        manifest = manifest.drop(overlap)
    resolved = manifest.join(lookup_df, on="_triple_k", how="left").drop("_triple_k")
    summary = ResolveSummary(
        overlapping_series_ids=_overlapping_ids(hash_to_meta),
        missing=_missing_frame(missing_cols, unresolved),
    )
    return resolved, summary


__all__ = [
    "OnMissing",
    "Path",
    "ResolveSummary",
    "ambiguous_edge_error",
    "edge_address_repr",
    "resolve_edge_uuid",
    "resolve_manifest",
    "resolve_node_uuid",
    "resolve_path",
    "resolve_paths_to_uuids",
    "resolve_subtree_uuids",
]
