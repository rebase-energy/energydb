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

Backend = Literal["polars", "pandas"]
Output = Literal["frame", "by_path"]


def to_polars(df: pl.DataFrame | pd.DataFrame) -> pl.DataFrame:
    """Coerce a user-supplied DataFrame to polars."""
    if isinstance(df, pd.DataFrame):
        return pl.from_pandas(df)
    return df


def to_backend(
    result: pl.DataFrame | dict[tuple, pl.DataFrame],
    backend: Backend,
) -> pl.DataFrame | pd.DataFrame | dict[tuple, pl.DataFrame] | dict[tuple, pd.DataFrame]:
    """Convert an internal polars result to the user-requested backend.

    Handles both the long-format ``pl.DataFrame`` and the per-series
    ``dict[tuple, pl.DataFrame]`` shape. When ``backend="pandas"`` and the
    input is a dict, each value is converted in turn.
    """
    if backend == "polars":
        return result
    if backend == "pandas":
        if isinstance(result, dict):
            # ty narrows the dict's value type to ``object`` after the isinstance,
            # so cast explicitly back to ``pl.DataFrame`` — no runtime cost.
            d = cast(dict[tuple, pl.DataFrame], result)
            return {k: v.to_pandas() for k, v in d.items()}
        return result.to_pandas()
    raise ValueError(f"backend must be 'polars' or 'pandas', got {backend!r}")
