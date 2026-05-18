"""Run metadata operations on the ``energydb.runs`` PostgreSQL table.

``run_id`` is client-generated (uuid7 truncated to UInt64). This removes the
round-trip to PG for id allocation: the write path mints an id, upserts the
row, and calls ``td.write`` with that id already stamped on the dataframe.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from psycopg.types.json import Jsonb
from uuid6 import uuid7


def generate_run_id() -> int:
    """Client-side BIGINT run id: top 63 bits of a uuid7.

    uuid7's 128-bit layout keeps the 48-bit ms timestamp in the high bits.
    Shifting right by 65 preserves timestamp sortability and keeps the result
    within a signed 63-bit range (fits ``BIGINT`` / polars ``Int64``).
    """
    return uuid7().int >> 65


def upsert_run(
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
    if run_start_time is not None and run_start_time.tzinfo is None:
        raise ValueError("run_start_time must be timezone-aware")
    if run_finish_time is not None and run_finish_time.tzinfo is None:
        raise ValueError("run_finish_time must be timezone-aware")

    conn.execute(
        """
        INSERT INTO energydb.runs
            (run_id, workflow_id, model_name, run_start_time, run_finish_time, run_params)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE SET
            workflow_id     = EXCLUDED.workflow_id,
            model_name      = EXCLUDED.model_name,
            run_start_time  = EXCLUDED.run_start_time,
            run_finish_time = EXCLUDED.run_finish_time,
            run_params      = EXCLUDED.run_params
        """,
        (
            run_id,
            workflow_id,
            model_name,
            run_start_time,
            run_finish_time,
            Jsonb(run_params or {}),
        ),
    )


def get_runs(conn, run_ids: list[int]) -> list[dict[str, Any]]:
    """Hydrate run metadata by id. Returns matched rows ordered by
    ``inserted_at`` descending (latest run first)."""
    if not run_ids:
        return []
    rows = conn.execute(
        """
        SELECT run_id, workflow_id, model_name, run_start_time, run_finish_time,
               run_params, inserted_at
        FROM energydb.runs
        WHERE run_id = ANY(%s)
        ORDER BY inserted_at DESC
        """,
        (run_ids,),
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
