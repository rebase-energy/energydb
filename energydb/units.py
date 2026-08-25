"""Unit conversion for energydb: thin wrapper around pint.

Moved from timedb so that timedb can drop the pint dependency.
"""

from __future__ import annotations

from functools import lru_cache

import pint
import polars as pl

from energydb.errors import EnergyDBError


class IncompatibleUnitError(EnergyDBError, ValueError):
    """Raised when units cannot be converted to each other."""


@lru_cache(maxsize=256)
def compute_unit_factor(from_unit: str, to_unit: str) -> float | None:
    """Return the pint conversion factor from *from_unit* to *to_unit*.

    Returns ``None`` when no multiplication is needed (same unit, or a
    dimensionless input, where the caller decides what dimensionless means).
    Raises :class:`IncompatibleUnitError` when not dimensionally compatible.
    """
    if from_unit == to_unit or from_unit == "dimensionless":
        return None
    ureg = pint.application_registry.get()
    try:
        return float(ureg.Quantity(1, from_unit).to(to_unit).magnitude)
    except pint.DimensionalityError as exc:
        raise IncompatibleUnitError(
            f"Cannot convert {from_unit!r} to {to_unit!r}: units are not dimensionally compatible."
        ) from exc


def apply_unit_factor(df: pl.DataFrame, from_unit: str, to_unit: str) -> pl.DataFrame:
    """Scale ``value`` column from *from_unit* to *to_unit* if they differ."""
    factor = compute_unit_factor(from_unit, to_unit)
    if factor is None:
        return df
    return df.with_columns(pl.col("value") * factor)
