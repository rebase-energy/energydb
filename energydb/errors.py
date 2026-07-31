"""Typed exception hierarchy for energydb.

Every exception energydb raises deliberately derives from
:class:`EnergyDBError`. Every class that replaced a bare ``ValueError`` raise
site *also* derives from :class:`ValueError`, so any existing
``except ValueError`` handler keeps working unchanged — the taxonomy is
additive by construction.

The not-found family carries structured identifier fields (``path``,
``uuid``, ``route``, ``missing``, …) so callers — API servers in particular —
can react programmatically instead of matching message text. Fields are
keyword-only, stored under their own name, and default to ``None`` when the
raise site doesn't know them. ``message`` stays ``args[0]``, so ``str(e)`` is
unchanged from the bare-``ValueError`` era.

This module sits at the bottom of the package dependency graph: it imports
nothing from the rest of energydb at runtime, so every other module can
import it freely. :class:`IncompatibleUnitError` keeps its definition in
:mod:`energydb.units` (import stability) and is re-exported here lazily via
:pep:`562` — ``units`` imports this module for :class:`EnergyDBError`, so a
module-level re-export would be a cycle.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    from energydb.units import IncompatibleUnitError


class EnergyDBError(Exception):
    """Base class for every exception energydb raises deliberately."""


# ---------------------------------------------------------------------------
# Not found — an *addressed* entity does not exist
# ---------------------------------------------------------------------------


class NotFoundError(EnergyDBError, ValueError):
    """An addressed entity does not exist."""


class NodeNotFoundError(NotFoundError):
    """A node addressed by path or by uuid does not exist.

    ``path`` is the ``/``-joined path that was addressed; ``uuid`` the
    addressed node uuid. Either may be ``None``: the site addressed the
    other way, or (bulk path resolution) several paths missed at once and no
    single one identifies the failure.
    """

    def __init__(self, message: str, *, path: str | None = None, uuid: UUID | None = None):
        super().__init__(message)
        self.path = path
        self.uuid = uuid


class EdgeNotFoundError(NotFoundError):
    """An edge addressed by uuid or by its ``(from, to, type)`` triple does not exist."""

    def __init__(
        self,
        message: str,
        *,
        uuid: UUID | None = None,
        from_path: str | None = None,
        to_path: str | None = None,
        edge_type: str | None = None,
    ):
        super().__init__(message)
        self.uuid = uuid
        self.from_path = from_path
        self.to_path = to_path
        self.edge_type = edge_type


class SeriesNotFoundError(NotFoundError):
    """One or more addressed series are not registered.

    ``route`` names the manifest route the lookup went through: ``"path"``,
    ``"node_uuid"``, ``"edge_uuid"``, or ``"edge_triple"``.

    ``missing`` carries *every* unresolved key, not just the one named in the
    message. Each entry is the route's owner identity followed by
    ``(data_type, name)`` — a 3-tuple for the single-column routes
    (``(owner, data_type, name)``), and a 5-tuple for ``"edge_triple"``, whose
    owner is itself the ``(from_path, to_path, edge_type)`` triple. Read the
    last two elements for the series, and the leading ones for the owner.
    """

    def __init__(
        self,
        message: str,
        *,
        route: str | None = None,
        missing: Sequence[tuple[str, ...]] | None = None,
    ):
        super().__init__(message)
        self.route = route
        # ``Sequence`` on the way in (``list`` is invariant, so a caller's
        # ``list[tuple[str, str, str]]`` would not be assignable); normalized to
        # a list on the way out so consumers get one predictable type.
        self.missing: list[tuple[str, ...]] | None = None if missing is None else list(missing)


# ---------------------------------------------------------------------------
# Conflict / validation / configuration
# ---------------------------------------------------------------------------


class AlreadyExistsError(EnergyDBError, ValueError):
    """Create-only violation: the entity, or a conflicting registration, already exists.

    Raised by ``register_tree`` on pre-existing or duplicate UUIDs, and when
    a series is re-registered with different immutable attributes.
    """


class ValidationError(EnergyDBError, ValueError):
    """Invalid arguments or an invalid operation.

    Bad kwarg combinations, invalid enum/choice values, missing required
    fields, payload references that point outside the tree, move-into-own-
    subtree, ``dry_run`` inside a ``transaction()``, and so on.
    """


class ManifestError(ValidationError):
    """Structurally invalid manifest.

    Missing or ambiguous routing columns, missing required columns, wrong
    dtypes, null routing values.
    """


class ConfigurationError(EnergyDBError, ValueError):
    """Client or environment misconfiguration (unusable conninfo, …)."""


_LAZY = {"IncompatibleUnitError": "energydb.units"}


def __getattr__(name: str) -> Any:
    """Lazily re-export :class:`~energydb.units.IncompatibleUnitError` (:pep:`562`).

    ``units`` imports this module for :class:`EnergyDBError`, so the
    re-export cannot be a module-level import without creating a cycle.
    """
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    return getattr(import_module(module), name)


__all__ = [
    "AlreadyExistsError",
    "ConfigurationError",
    "EdgeNotFoundError",
    "EnergyDBError",
    "IncompatibleUnitError",
    "ManifestError",
    "NodeNotFoundError",
    "NotFoundError",
    "SeriesNotFoundError",
    "ValidationError",
]
