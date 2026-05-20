"""Pandas/polars boundary adapters for the public API.

Public ``Client`` and scope methods accept either pandas or polars
on input and return polars by default (``backend="polars"``). The internal
manifest pipeline (``_io``, ``_join``, ``paths``, ``series``) is polars
throughout — these helpers are the only conversion points.
"""

from __future__ import annotations

from typing import Literal, cast

import pandas as pd
import polars as pl

from energydb._join import EdgeSeriesKey, SeriesKey

Backend = Literal["polars", "pandas"]
Output = Literal["frame", "by_path"]


def to_polars(df: pl.DataFrame | pd.DataFrame) -> pl.DataFrame:
    """Coerce a user-supplied DataFrame to polars."""
    if isinstance(df, pd.DataFrame):
        return pl.from_pandas(df)
    return df


def to_backend(
    result: pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame],
    backend: Backend,
) -> (
    pl.DataFrame
    | pd.DataFrame
    | dict[SeriesKey, pl.DataFrame]
    | dict[SeriesKey, pd.DataFrame]
    | dict[EdgeSeriesKey, pl.DataFrame]
    | dict[EdgeSeriesKey, pd.DataFrame]
):
    """Convert an internal polars result to the user-requested backend.

    Handles both the long-format ``pl.DataFrame`` and the per-series
    ``dict[SeriesKey | EdgeSeriesKey, pl.DataFrame]`` shape. When
    ``backend="pandas"`` and the input is a dict, each value is converted
    in turn.
    """
    if backend == "polars":
        return result
    if backend == "pandas":
        if isinstance(result, dict):
            # ty narrows the dict's value type to ``object`` after the isinstance,
            # so cast explicitly back to ``pl.DataFrame`` — no runtime cost.
            d = cast(dict[SeriesKey | EdgeSeriesKey, pl.DataFrame], result)
            # dict invariance: the runtime dict carries a single key type per call
            # (driven by the routing column), but statically the union has to be
            # collapsed before assigning into the narrower return type.
            return cast(
                dict[SeriesKey, pd.DataFrame] | dict[EdgeSeriesKey, pd.DataFrame],
                {k: v.to_pandas() for k, v in d.items()},
            )
        return result.to_pandas()
    raise ValueError(f"backend must be 'polars' or 'pandas', got {backend!r}")
