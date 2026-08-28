"""Unit tests for the empty-read column schema (energydb#5).

Pure (no DB): pins the exact column names, order, and dtypes ``execute_read``
/ ``_finish_read`` produce when a read resolves zero series, or resolves at
least one series whose ClickHouse result has zero rows, across node and edge
routing and the four bitemporal option combos.
"""

from __future__ import annotations

import asyncio

import pandas as pd
import polars as pl
import pytest
from energydb._frames import to_backend
from energydb._io import _finish_read, execute_read
from energydb.errors import ValidationError
from energydb.scope import _strip_scope_identity

_TS = pl.Datetime("us", "UTC")

_DATA_COLS = {
    (False, False): [("valid_time", _TS), ("value", pl.Float64)],
    (False, True): [("knowledge_time", _TS), ("valid_time", _TS), ("value", pl.Float64)],
    (True, False): [
        ("valid_time", _TS),
        ("change_time", _TS),
        ("value", pl.Float64),
        ("changed_by", pl.Utf8),
        ("annotation", pl.Utf8),
    ],
    (True, True): [
        ("valid_time", _TS),
        ("knowledge_time", _TS),
        ("change_time", _TS),
        ("value", pl.Float64),
        ("changed_by", pl.Utf8),
        ("annotation", pl.Utf8),
    ],
}
NODE_IDENTITY = [("path", pl.Utf8), ("data_type", pl.Utf8), ("name", pl.Utf8)]
EDGE_IDENTITY = [
    ("from_path", pl.Utf8),
    ("to_path", pl.Utf8),
    ("edge_type", pl.Utf8),
    ("edge_name", pl.Utf8),
    ("data_type", pl.Utf8),
    ("name", pl.Utf8),
]


class _FakeClient:
    def __init__(self) -> None:
        self._engine_unavailable = True


async def _resolves_to_nothing() -> None:
    return None


# ---------------------------------------------------------------------------
# execute_read's zero-resolved-series path (`_empty()`)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("is_edge", "identity"), [(False, NODE_IDENTITY), (True, EDGE_IDENTITY)])
@pytest.mark.parametrize(("include_updates", "include_knowledge_time"), list(_DATA_COLS))
def test_execute_read_empty_keeps_stable_schema(is_edge, identity, include_updates, include_knowledge_time):
    expected = _DATA_COLS[(include_updates, include_knowledge_time)] + identity
    result, n_series, missing = asyncio.run(
        execute_read(
            None,
            None,
            _FakeClient(),
            resolve=_resolves_to_nothing,
            is_edge=is_edge,
            include_updates=include_updates,
            include_knowledge_time=include_knowledge_time,
        )
    )
    assert isinstance(result, pl.DataFrame)
    assert list(result.schema.items()) == expected
    assert result.height == 0
    assert n_series == 0
    assert missing.is_empty()
    # Regression: column access must not raise on an empty frame.
    list(zip(result["valid_time"], result["value"], strict=True))


def test_execute_read_empty_by_path_returns_empty_dict():
    result, n_series, _missing = asyncio.run(
        execute_read(
            None,
            None,
            _FakeClient(),
            resolve=_resolves_to_nothing,
            is_edge=False,
            output="by_path",
        )
    )
    assert result == {}
    assert n_series == 0


def test_execute_read_relative_empty_keeps_stable_schema():
    result, n_series, _missing = asyncio.run(
        execute_read(
            None,
            None,
            _FakeClient(),
            resolve=_resolves_to_nothing,
            is_edge=False,
            relative=True,
            td_kwargs={},
        )
    )
    assert isinstance(result, pl.DataFrame)
    assert list(result.schema.items()) == _DATA_COLS[(False, False)] + NODE_IDENTITY
    assert n_series == 0


def test_execute_read_requires_is_edge_with_resolve():
    with pytest.raises(ValidationError, match="is_edge"):
        asyncio.run(
            execute_read(
                None,
                None,
                _FakeClient(),
                resolve=_resolves_to_nothing,
            )
        )


# ---------------------------------------------------------------------------
# _finish_read's zero-CH-rows-but-resolved-meta path
# ---------------------------------------------------------------------------


def _empty_ch_result() -> pl.DataFrame:
    return pl.DataFrame(schema={"series_id": pl.UInt64, "valid_time": _TS, "value": pl.Float64})


def test_finish_read_empty_result_node_meta_keeps_stable_schema():
    meta = pl.DataFrame(
        {
            "series_id": [1],
            "retention": ["forever"],
            "canonical_unit": ["MW"],
            "data_type": ["actual"],
            "name": ["power"],
            "node_uuid": ["abc"],
            "path": ["P/T01"],
        }
    )
    out = _finish_read(None, _empty_ch_result(), meta, unit=None, output="frame")
    assert isinstance(out, pl.DataFrame)
    assert list(out.schema.items()) == _DATA_COLS[(False, False)] + NODE_IDENTITY
    assert out.height == 0
    list(zip(out["valid_time"], out["value"], strict=True))


def test_finish_read_empty_result_edge_meta_keeps_stable_schema():
    meta = pl.DataFrame(
        {
            "series_id": [2],
            "retention": ["forever"],
            "canonical_unit": ["MW"],
            "data_type": ["actual"],
            "name": ["flow"],
            "edge_uuid": ["xyz"],
            "edge_type": ["Line"],
            "edge_name": [None],
            "from_path": ["Grid/A"],
            "to_path": ["Grid/B"],
        },
        schema_overrides={"edge_name": pl.Utf8},
    )
    out = _finish_read(None, _empty_ch_result(), meta, unit=None, output="frame")
    assert isinstance(out, pl.DataFrame)
    assert list(out.schema.items()) == _DATA_COLS[(False, False)] + EDGE_IDENTITY
    assert out.height == 0


def test_finish_read_empty_result_scope_single_series_strips_to_data_columns():
    """The scope's post-read strip must still land on {valid_time, value} once
    the empty branch carries the full identity shape."""
    meta = pl.DataFrame(
        {
            "series_id": [1],
            "retention": ["forever"],
            "canonical_unit": ["MW"],
            "data_type": ["actual"],
            "name": ["power"],
            "node_uuid": ["abc"],
            "path": ["P/T01"],
        }
    )
    out = _finish_read(None, _empty_ch_result(), meta, unit=None, output="frame")
    assert isinstance(out, pl.DataFrame)
    stripped = _strip_scope_identity(out, is_edge=False)
    assert list(stripped.schema.items()) == [("valid_time", _TS), ("value", pl.Float64)]
    assert stripped.height == 0
    list(zip(stripped["valid_time"], stripped["value"], strict=True))


# ---------------------------------------------------------------------------
# pandas backend conversion carries the schema through unchanged
# ---------------------------------------------------------------------------


def test_pandas_backend_carries_empty_schema():
    meta = pl.DataFrame(
        {
            "series_id": [1],
            "retention": ["forever"],
            "canonical_unit": ["MW"],
            "data_type": ["actual"],
            "name": ["power"],
            "node_uuid": ["abc"],
            "path": ["P/T01"],
        }
    )
    out = _finish_read(None, _empty_ch_result(), meta, unit=None, output="frame")
    assert isinstance(out, pl.DataFrame)
    pdf = to_backend(out, "pandas")
    assert isinstance(pdf, pd.DataFrame)
    assert list(pdf.columns) == [name for name, _ in _DATA_COLS[(False, False)] + NODE_IDENTITY]
    assert len(pdf) == 0
