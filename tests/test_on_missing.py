"""``read(on_missing=)``: read past unregistered series instead of failing.

``resolve_manifest`` used to raise on the *first* unresolvable
``(owner, data_type, name)`` triple, so a manifest of 1,500 series returned
nothing because one series wasn't registered. Two changes:

* the default ``"raise"`` now reports *every* unresolved triple, structurally on
  the exception and by count in the message;
* ``on_missing="skip"`` drops them from the read and returns
  ``ReadResult(data, missing)`` instead.

Writes are deliberately left strict, since dropping rows there is data loss
rather than a completeness report. Structural manifest errors raise under both
settings, because they are caller bugs rather than catalog gaps.

The pure tests drive the resolver over a mocked connection; the live tests cover
the client return shapes, output/backend modes, and engine-vs-sequential parity.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import energydb as edb
import pandas as pd
import polars as pl
import pytest
from energydb import Client, ReadResult
from energydb._sync import _wrap
from energydb.errors import SeriesNotFoundError, ValidationError
from energydb.paths import resolve_manifest


def _mock_conn_with_series(rows: list[tuple]) -> MagicMock:
    """An async-aware mock connection for the ``await (await execute).fetchall()`` path."""
    cursor = MagicMock()
    cursor.fetchall = AsyncMock(return_value=rows)
    conn = MagicMock()
    conn.execute = AsyncMock(return_value=cursor)
    return conn


# Path-route row layout: path, data_type, name, series_id, canonical_unit,
# timeseries_type, retention, node_uuid::text.
def _path_row(path: str, series_id: int) -> tuple:
    return (path, "actual", "power", series_id, "MW", "FLAT", "forever", str(uuid4()))


def _path_manifest(*paths: str) -> pl.DataFrame:
    return pl.DataFrame({"path": list(paths), "data_type": ["actual"] * len(paths), "name": ["power"] * len(paths)})


# ---------------------------------------------------------------------------
# The "raise" default: same behaviour, but the whole gap in one error
# ---------------------------------------------------------------------------


def test_a_single_miss_keeps_the_historical_message():
    """One unresolved triple is the overwhelmingly common case and its message is
    already good; callers (and psd) match on this exact prefix."""
    conn = _mock_conn_with_series([])
    with pytest.raises(SeriesNotFoundError) as excinfo:
        asyncio.run(resolve_manifest(conn, _path_manifest("P/A")))

    assert str(excinfo.value) == "Series not registered for path='P/A', data_type='actual', name='power'."


def test_raise_reports_every_unresolved_triple_not_just_the_first():
    """**The regression this item exists for.** Three gaps used to cost three
    round-trips to discover; now one raise names all of them."""
    conn = _mock_conn_with_series([_path_row("P/OK", 1)])
    with pytest.raises(SeriesNotFoundError) as excinfo:
        asyncio.run(resolve_manifest(conn, _path_manifest("P/OK", "P/A", "P/B", "P/C")))

    err = excinfo.value
    assert err.route == "path"
    assert set(err.missing or []) == {
        ("P/A", "actual", "power"),
        ("P/B", "actual", "power"),
        ("P/C", "actual", "power"),
    }
    message = str(err)
    assert "3 triples in this manifest are unresolved" in message
    # Every one of them is named, and the resolvable series is not.
    for path in ("P/A", "P/B", "P/C"):
        assert f"path={path!r}" in message
    assert "P/OK" not in message


def test_the_message_truncates_but_the_exception_does_not():
    """A 1,500-series manifest with 1,500 gaps must not produce a 1,500-line
    message; the structured field is where the full list lives."""
    paths = [f"P/{i}" for i in range(9)]
    conn = _mock_conn_with_series([])
    with pytest.raises(SeriesNotFoundError) as excinfo:
        asyncio.run(resolve_manifest(conn, _path_manifest(*paths)))

    err = excinfo.value
    assert len(err.missing or []) == 9  # nothing lost structurally
    message = str(err)
    assert "9 triples in this manifest are unresolved" in message
    assert "(4 of 8 shown)" in message
    assert message.count("data_type='actual'") == 5  # the leader plus four more, none repeated


# ---------------------------------------------------------------------------
# "skip": drop the gaps, report them
# ---------------------------------------------------------------------------


def test_skip_drops_the_unresolved_rows_from_the_resolved_frame():
    """The unresolvable rows must not survive as null-``series_id`` rows: the
    left-join would happily keep them and ``_project_meta``'s ``.unique()`` would
    then emit a null-id series into the CH read."""
    conn = _mock_conn_with_series([_path_row("P/OK", 7)])
    resolved, summary = asyncio.run(resolve_manifest(conn, _path_manifest("P/OK", "P/GONE"), on_missing="skip"))

    assert resolved["path"].to_list() == ["P/OK"]
    assert resolved["series_id"].to_list() == [7]
    assert resolved["series_id"].null_count() == 0
    assert summary.missing.rows() == [("P/GONE", "actual", "power")]


def test_skip_reports_missing_as_a_utf8_frame_with_the_route_column():
    conn = _mock_conn_with_series([_path_row("P/OK", 7)])
    _resolved, summary = asyncio.run(resolve_manifest(conn, _path_manifest("P/OK", "P/GONE"), on_missing="skip"))

    assert summary.missing.columns == ["path", "data_type", "name"]
    assert summary.missing.dtypes == [pl.Utf8, pl.Utf8, pl.Utf8]


def test_skip_deduplicates_repeated_missing_triples():
    """A manifest carries one row per data point, so the same missing triple shows
    up many times; ``missing`` is the unique key set, not the row set."""
    conn = _mock_conn_with_series([])
    _resolved, summary = asyncio.run(
        resolve_manifest(conn, _path_manifest("P/GONE", "P/GONE", "P/GONE"), on_missing="skip")
    )

    assert summary.missing.rows() == [("P/GONE", "actual", "power")]


def test_skip_with_everything_missing_returns_an_empty_frame_not_a_raise():
    conn = _mock_conn_with_series([])
    resolved, summary = asyncio.run(resolve_manifest(conn, _path_manifest("P/A", "P/B"), on_missing="skip"))

    assert resolved.height == 0
    # Still the full resolved schema; downstream projection selects these by name.
    for col in ("path", "data_type", "name", "series_id", "canonical_unit", "retention", "node_uuid"):
        assert col in resolved.columns
    assert set(summary.missing.rows()) == {("P/A", "actual", "power"), ("P/B", "actual", "power")}


def test_skip_with_nothing_missing_is_indistinguishable_from_raise():
    conn = _mock_conn_with_series([_path_row("P/OK", 7)])
    manifest = _path_manifest("P/OK")
    strict, strict_summary = asyncio.run(resolve_manifest(_mock_conn_with_series([_path_row("P/OK", 7)]), manifest))
    lenient, lenient_summary = asyncio.run(resolve_manifest(conn, manifest, on_missing="skip"))

    assert lenient.drop("node_uuid").equals(strict.drop("node_uuid"))  # node_uuid is a fresh uuid4 per row builder
    assert lenient_summary.missing.is_empty()
    assert lenient_summary.missing.columns == ["path", "data_type", "name"]
    assert strict_summary.missing.is_empty()  # present and empty on the raise path too


def test_skip_on_the_uuid_route_stringifies_the_owner():
    """``missing`` never leaks internal reprs: a manifest of ``uuid.UUID`` objects
    comes back as ``Utf8``, same as every other public routing value."""
    present, absent = uuid4(), uuid4()
    rows = [(str(present), "actual", "power", 3, "MW", "FLAT", "forever", "P/A")]
    manifest = pl.DataFrame(
        {
            "node_uuid": [present, absent],  # UUID objects, not strings
            "data_type": ["actual", "actual"],
            "name": ["power", "power"],
        }
    )
    resolved, summary = asyncio.run(resolve_manifest(_mock_conn_with_series(rows), manifest, on_missing="skip"))

    assert resolved["series_id"].to_list() == [3]
    assert summary.missing.columns == ["node_uuid", "data_type", "name"]
    assert summary.missing.rows() == [(str(absent), "actual", "power")]


def test_skip_on_the_edge_uuid_route():
    present, absent = uuid4(), uuid4()
    rows = [(str(present), "actual", "flow", 5, "MW", "FLAT", "forever", "Line", None, "G/A", "G/B")]
    manifest = pl.DataFrame(
        {
            "edge_uuid": [str(present), str(absent)],
            "data_type": ["actual", "actual"],
            "name": ["flow", "flow"],
        }
    )
    resolved, summary = asyncio.run(resolve_manifest(_mock_conn_with_series(rows), manifest, on_missing="skip"))

    assert resolved["series_id"].to_list() == [5]
    assert summary.missing.rows() == [(str(absent), "actual", "flow")]


def test_skip_on_the_edge_triple_route_reports_all_five_key_columns():
    """The edge triple's identity *is* the quintuple, so a three-column report
    would not say which edge was missing."""
    rows = [("G/A", "G/B", "Line", None, "actual", "flow", 11, "MW", "FLAT", "forever", str(uuid4()))]
    manifest = pl.DataFrame(
        {
            "from_path": ["G/A", "G/A"],
            "to_path": ["G/B", "G/NOPE"],
            "edge_type": ["Line", "Line"],
            "data_type": ["actual", "actual"],
            "name": ["flow", "flow"],
        }
    )
    resolved, summary = asyncio.run(resolve_manifest(_mock_conn_with_series(rows), manifest, on_missing="skip"))

    assert resolved["series_id"].to_list() == [11]
    assert summary.missing.columns == ["from_path", "to_path", "edge_type", "data_type", "name"]
    assert summary.missing.rows() == [("G/A", "G/NOPE", "Line", "actual", "flow")]


# ---------------------------------------------------------------------------
# What "skip" does NOT cover
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("manifest", "match"),
    [
        pytest.param(
            pl.DataFrame({"data_type": ["actual"], "name": ["power"]}),
            "as a routing column",
            id="no-routing-column",
        ),
        pytest.param(
            pl.DataFrame({"path": ["P/A"], "node_uuid": [str(uuid4())], "data_type": ["actual"], "name": ["power"]}),
            "ambiguous routing columns",
            id="ambiguous-routing",
        ),
        pytest.param(
            pl.DataFrame({"path": [["P", "A"]], "data_type": ["actual"], "name": ["power"]}),
            "must be Utf8",
            id="non-utf8-path",
        ),
        pytest.param(
            pl.DataFrame(
                {"path": [None], "data_type": ["actual"], "name": ["power"]}, schema_overrides={"path": pl.Utf8}
            ),
            "No path values to resolve",
            id="all-null-path",
        ),
        pytest.param(
            pl.DataFrame({"path": ["P/A", None], "data_type": ["actual"] * 2, "name": ["power"] * 2}),
            "path=None",
            id="one-null-path",
        ),
        pytest.param(
            pl.DataFrame({"path": ["P/A"], "name": ["power"]}),
            "missing required columns",
            id="missing-data-type",
        ),
    ],
)
def test_structural_manifest_errors_raise_even_under_skip(manifest, match):
    """``on_missing`` governs catalog gaps only. A malformed manifest is a caller
    bug: silently reading nothing would hide it."""
    with pytest.raises(ValidationError, match=match):
        asyncio.run(resolve_manifest(_mock_conn_with_series([]), manifest, on_missing="skip"))


def test_an_invalid_on_missing_value_is_rejected():
    with pytest.raises(ValidationError) as excinfo:
        # The Literal already rules this out statically; the runtime check is for
        # the callers who reach energydb from untyped code (an API query param).
        asyncio.run(
            resolve_manifest(
                _mock_conn_with_series([]),
                _path_manifest("P/A"),
                on_missing="ignore",  # ty: ignore[invalid-argument-type]
            )
        )

    # The message must name the valid values; the caller has to be able to fix
    # the typo without reading the source.
    assert "['raise', 'skip']" in str(excinfo.value)
    assert "'ignore'" in str(excinfo.value)


def test_resolve_manifest_defaults_to_raise():
    """The write pipeline relies on this default: it never passes ``on_missing``,
    and must never silently drop rows."""
    with pytest.raises(SeriesNotFoundError):
        asyncio.run(resolve_manifest(_mock_conn_with_series([]), _path_manifest("P/A")))


# ---------------------------------------------------------------------------
# ReadResult
# ---------------------------------------------------------------------------


def test_read_result_is_a_public_unpackable_namedtuple():
    frame, gaps = pl.DataFrame({"a": [1]}), pl.DataFrame({"path": ["P/A"]})
    result = ReadResult(frame, gaps)
    data, missing = result

    assert edb.ReadResult is ReadResult
    assert isinstance(result, tuple)
    assert data is frame
    assert missing is gaps
    assert result.data is frame  # positional and attribute access agree
    assert result.missing is gaps


def test_the_sync_facade_passes_read_result_through_untouched():
    """``_wrap`` only proxies objects with an async surface. A ``NamedTuple`` of
    frames has none, so the sync ``Client`` must hand back the real thing,
    otherwise ``data, missing = client.read(...)`` would unpack a proxy.

    ``portal=None`` is the point: a portal-free object must never reach for one.
    """
    result = ReadResult(pl.DataFrame({"a": [1]}), pl.DataFrame())

    assert _wrap(result, portal=None) is result  # ty: ignore[invalid-argument-type]


# ---------------------------------------------------------------------------
# Live
# ---------------------------------------------------------------------------

pytestmark_live = pytest.mark.skipif(
    not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")),
    reason="TIMEDB_PG_DSN / TIMEDB_CH_URL not set",
)

BASE = datetime(2026, 3, 1, tzinfo=UTC)


@pytest.fixture
def client():
    """Two turbines with a registered ``actual/power`` series and data; a third
    turbine exists as a *node* but has no series registered: the catalog gap."""
    c = Client()
    c.delete()
    c.create()
    c.register_tree(
        edb.Portfolio(
            name="P",
            members=[edb.wind.WindTurbine(name=n, capacity=3.0) for n in ("T1", "T2", "T3")],
        )
    )
    for name in ("T1", "T2"):
        c.get_node("P", name).register_series(
            name="power", canonical_unit="MW", data_type="actual", timeseries_type="FLAT", retention="forever"
        )
    c.write(
        pl.DataFrame(
            {
                "path": ["P/T1", "P/T1", "P/T2", "P/T2"],
                "data_type": ["actual"] * 4,
                "name": ["power"] * 4,
                "valid_time": [BASE, BASE + timedelta(hours=1)] * 2,
                "value": [1.0, 2.0, 10.0, 20.0],
            }
        )
    )
    yield c
    c.delete()
    c.close()


def _manifest(*paths: str) -> pl.DataFrame:
    return pl.DataFrame({"path": list(paths), "data_type": ["actual"] * len(paths), "name": ["power"] * len(paths)})


@pytestmark_live
def test_one_unregistered_series_no_longer_costs_the_whole_batch(client):
    """**The psd payoff.** ``T3`` has no series; the other two still read, and the
    gap comes back named instead of having to be pre-filtered by a cached
    catalog snapshot."""
    with pytest.raises(SeriesNotFoundError):
        client.read(_manifest("P/T1", "P/T2", "P/T3"))

    data, missing = client.read(_manifest("P/T1", "P/T2", "P/T3"), on_missing="skip")

    # Byte-identical to reading the pre-filtered manifest: "skip" changes what
    # is asked for, never how the rest is read.
    assert data.equals(client.read(_manifest("P/T1", "P/T2")))
    assert missing.rows() == [("P/T3", "actual", "power")]


@pytestmark_live
def test_skip_returns_a_read_result_and_raise_returns_the_bare_frame(client):
    """The return-type switch is the whole backwards-compatibility story: it must
    happen on opt-in and only on opt-in."""
    bare = client.read(_manifest("P/T1"))
    assert isinstance(bare, pl.DataFrame)
    assert not isinstance(bare, ReadResult)

    wrapped = client.read(_manifest("P/T1"), on_missing="skip")
    assert isinstance(wrapped, ReadResult)
    assert wrapped.data.equals(bare)
    assert wrapped.missing.is_empty()
    assert wrapped.missing.columns == ["path", "data_type", "name"]


@pytestmark_live
def test_skip_with_every_triple_missing_yields_the_empty_read_shape(client):
    """No raise, no rows, and the gap fully reported: the all-or-nothing case a
    caller hits when a whole workflow's series were never registered."""
    manifest = _manifest("P/T3")

    frame_data, frame_missing = client.read(manifest, on_missing="skip")
    assert isinstance(frame_data, pl.DataFrame)
    assert frame_data.is_empty()
    assert frame_missing.rows() == [("P/T3", "actual", "power")]

    dict_data, dict_missing = client.read(manifest, output="by_path", on_missing="skip")
    assert dict_data == {}
    assert dict_missing.rows() == [("P/T3", "actual", "power")]


@pytestmark_live
def test_skip_with_by_path_output_omits_the_missing_keys(client):
    data, missing = client.read(_manifest("P/T1", "P/T3"), output="by_path", on_missing="skip")

    assert set(data) == {("P/T1", "actual", "power")}
    assert ("P/T3", "actual", "power") not in data
    assert missing.rows() == [("P/T3", "actual", "power")]


@pytestmark_live
def test_skip_follows_the_pandas_backend_for_both_elements(client):
    """``missing`` is user-facing output, so it converts at the same boundary
    ``data`` does: a caller who asked for pandas gets pandas throughout."""
    data, missing = client.read(_manifest("P/T1", "P/T3"), backend="pandas", on_missing="skip")

    assert isinstance(data, pd.DataFrame)
    assert isinstance(missing, pd.DataFrame)
    assert list(missing.columns) == ["path", "data_type", "name"]
    assert missing.to_dict("records") == [{"path": "P/T3", "data_type": "actual", "name": "power"}]


@pytestmark_live
def test_skip_is_identical_over_the_engine_and_the_sequential_path(client, monkeypatch):
    """The engine predicate is a superset built from the *unfiltered* manifest, so
    a skipped triple must not be able to leak values in through it. The values are
    semi-joined against the exact resolve, which is what makes that true; this
    test pins it.
    """
    import energydb._io as _io

    monkeypatch.setattr(_io, "_ENGINE_STRICT", True)  # an engine failure raises instead of hiding
    manifest = _manifest("P/T1", "P/T2", "P/T3")

    client._async._engine_unavailable = True
    sequential = client.read(manifest, on_missing="skip")
    client._async._engine_unavailable = False
    engine = client.read(manifest, on_missing="skip")

    assert client._async._engine_unavailable is False  # the engine really was used
    assert engine.data.equals(sequential.data)
    assert engine.missing.equals(sequential.missing)


@pytestmark_live
def test_read_relative_takes_on_missing_too(client):
    """``read`` and ``read_relative`` share the manifest resolve, so the parameter
    has to reach both: a caller on the relative window shouldn't have to switch
    APIs to survive a catalog gap."""
    # A huge issue_offset keeps the fixture's rows inside their window.
    data, missing = client.read_relative(
        _manifest("P/T1", "P/T3"),
        on_missing="skip",
        window_length=timedelta(hours=2),
        issue_offset=timedelta(days=365),
        start_window=BASE,
    )

    assert missing.rows() == [("P/T3", "actual", "power")]
    assert isinstance(data, pl.DataFrame)
    assert data.height == 2  # P/T1's rows really did come back


@pytestmark_live
def test_writes_stay_strict(client):
    """Skipping a *write* would be silent data loss, so ``on_missing`` must not
    reach the write pipeline in any form."""
    write_df = pl.DataFrame(
        {
            "path": ["P/T1", "P/T3"],
            "data_type": ["actual"] * 2,
            "name": ["power"] * 2,
            "valid_time": [BASE + timedelta(hours=5)] * 2,
            "value": [1.0, 2.0],
        }
    )
    with pytest.raises(SeriesNotFoundError):
        client.write(write_df)

    with pytest.raises(TypeError):
        client.write(write_df, on_missing="skip")


@pytestmark_live
def test_the_sync_client_returns_a_plain_read_result(client):
    """End-to-end through the portal: no ``_SyncProxy`` in sight, and it unpacks."""
    result = client.read(_manifest("P/T1", "P/T3"), on_missing="skip")

    assert type(result) is ReadResult
    data, missing = result
    assert isinstance(data, pl.DataFrame)
    assert isinstance(missing, pl.DataFrame)
