"""Pandas/polars boundary adapters for the public API.

Public ``Client`` and scope methods accept either pandas or polars
on input and return pandas by default. The internal manifest pipeline
(``_io``, ``_join``, ``paths``, ``series``) is polars throughout — these
helpers are the only conversion points.
"""

from __future__ import annotations

from typing import Literal

import pandas as pd
import polars as pl

OutputType = Literal["pandas", "polars"]


def to_polars(df: pl.DataFrame | pd.DataFrame) -> pl.DataFrame:
    """Coerce a user-supplied DataFrame to polars."""
    if isinstance(df, pd.DataFrame):
        return pl.from_pandas(df)
    return df


def to_output(df: pl.DataFrame, output: OutputType) -> pl.DataFrame | pd.DataFrame:
    """Convert an internal polars result to the user-requested output type."""
    if output == "pandas":
        return df.to_pandas()
    if output == "polars":
        return df
    raise ValueError(f"output must be 'pandas' or 'polars', got {output!r}")
