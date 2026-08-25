"""Run metadata operations on the ``runs`` PostgreSQL table.

``run_id`` is client-generated (uuid7 truncated to UInt64). This removes the
round-trip to PG for id allocation: the write path mints an id, upserts the
row, and calls ``td.write`` with that id already stamped on the dataframe.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, NamedTuple

from psycopg.types.json import Jsonb
from uuid6 import uuid7

from energydb.errors import ValidationError
from energydb.models import SQL_SCHEMA_PREFIX as P


class RunRow(NamedTuple):
    """The columns of a ``runs`` upsert, carried as one value.

    Lets the write path fold the run upsert into the manifest-resolve query
    (one round-trip) via :func:`run_upsert_cte`, or run it standalone via
    :func:`upsert_run_row`."""

    run_id: int
    workflow_id: str | None = None
    model_name: str | None = None
    run_start_time: datetime | None = None
    run_finish_time: datetime | None = None
    run_params: dict | None = None


# Shared by the standalone upsert and the foldable CTE. Idempotent under retry;
# run identity is immutable and keyed by run_id.
_RUN_UPSERT_BODY = f"""INSERT INTO {P}runs
            (run_id, workflow_id, model_name, run_start_time, run_finish_time, run_params)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE SET
            workflow_id     = EXCLUDED.workflow_id,
            model_name      = EXCLUDED.model_name,
            run_start_time  = EXCLUDED.run_start_time,
            run_finish_time = EXCLUDED.run_finish_time,
            run_params      = EXCLUDED.run_params"""


def _run_params(run: RunRow) -> tuple:
    if run.run_start_time is not None and run.run_start_time.tzinfo is None:
        raise ValidationError("run_start_time must be timezone-aware")
    if run.run_finish_time is not None and run.run_finish_time.tzinfo is None:
        raise ValidationError("run_finish_time must be timezone-aware")
    return (
        run.run_id,
        run.workflow_id,
        run.model_name,
        run.run_start_time,
        run.run_finish_time,
        Jsonb(run.run_params or {}),
    )


def run_upsert_cte(run: RunRow) -> tuple[str, tuple]:
    """A data-modifying CTE that upserts ``run``, to prepend to another query.

    Postgres executes a ``WITH … AS (INSERT …)`` exactly once, even when the
    main query never references it, so the run is recorded regardless of what
    the folded SELECT returns (preserves "an all-skipped write still records a
    run"). Returns ``(cte_sql, params)``; the params bind before the rest."""
    return f"WITH run_ins AS (\n        {_RUN_UPSERT_BODY}\n    )\n", _run_params(run)


async def upsert_run_row(conn, run: RunRow) -> None:
    """Standalone run upsert (run-metadata updates outside a write's folded resolve)."""
    await conn.execute(_RUN_UPSERT_BODY, _run_params(run))


def generate_run_id() -> int:
    """Client-side BIGINT run id: top 63 bits of a uuid7.

    uuid7's 128-bit layout keeps the 48-bit ms timestamp in the high bits.
    Shifting right by 65 preserves timestamp sortability and keeps the result
    within a signed 63-bit range (fits ``BIGINT`` / polars ``Int64``).
    """
    return uuid7().int >> 65


async def upsert_run(
    conn,
    *,
    run_id: int,
    workflow_id: str | None = None,
    model_name: str | None = None,
    run_start_time: datetime | None = None,
    run_finish_time: datetime | None = None,
    run_params: dict | None = None,
) -> None:
    """Insert or update a run row. Idempotent under retry."""
    await upsert_run_row(
        conn,
        RunRow(run_id, workflow_id, model_name, run_start_time, run_finish_time, run_params),
    )


async def get_runs(conn, run_ids: list[int]) -> list[dict[str, Any]]:
    """Hydrate run metadata by id. Returns matched rows ordered by
    ``inserted_at`` descending (latest run first)."""
    if not run_ids:
        return []
    rows = await (
        await conn.execute(
            f"""
            SELECT run_id, workflow_id, model_name, run_start_time, run_finish_time,
                   run_params, inserted_at
            FROM {P}runs
            WHERE run_id = ANY(%s)
            ORDER BY inserted_at DESC
            """,
            (run_ids,),
        )
    ).fetchall()
    return [
        {
            "run_id": r[0],
            "workflow_id": r[1],
            "model_name": r[2],
            "run_start_time": r[3],
            "run_finish_time": r[4],
            "run_params": r[5],
            "inserted_at": r[6],
        }
        for r in rows
    ]
