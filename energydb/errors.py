"""Typed exception hierarchy for energydb.

Every exception energydb raises deliberately derives from
:class:`EnergyDBError`. Every class that replaced a bare ``ValueError`` raise
site *also* derives from :class:`ValueError`, so any existing
``except ValueError`` handler keeps working unchanged; the taxonomy is
additive by construction.

The not-found family carries structured identifier fields (``path``,
``uuid``, ``route``, ``missing``, …) so callers, API servers in particular,
can react programmatically instead of matching message text. Fields are
keyword-only, stored under their own name, and default to ``None`` when the
raise site doesn't know them. ``message`` stays ``args[0]``, so ``str(e)``
matches the bare-``ValueError`` form.

This module sits at the bottom of the package dependency graph: it imports
nothing from the rest of energydb at runtime, so every other module can
import it freely. :class:`IncompatibleUnitError` keeps its definition in
:mod:`energydb.units` (import stability) and is re-exported here lazily via
:pep:`562`: ``units`` imports this module for :class:`EnergyDBError`, so a
module-level re-export would be a cycle.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    from energydb.units import IncompatibleUnitError


class EnergyDBError(Exception):
    """Base class for every exception energydb raises deliberately."""


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
    """An edge addressed by uuid or by its ``(from, to, type[, name])`` key does not exist.

    ``name`` is the edge name that narrowed the lookup, or ``None`` when the
    caller addressed by the bare triple (which, for a multigraph, may match
    several edges, see :class:`AmbiguousEdgeError`).
    """

    def __init__(
        self,
        message: str,
        *,
        uuid: UUID | None = None,
        from_path: str | None = None,
        to_path: str | None = None,
        edge_type: str | None = None,
        name: str | None = None,
    ):
        super().__init__(message)
        self.uuid = uuid
        self.from_path = from_path
        self.to_path = to_path
        self.edge_type = edge_type
        self.name = name


class SeriesNotFoundError(NotFoundError):
    """One or more addressed series are not registered.

    ``route`` names the manifest route the lookup went through: ``"path"``,
    ``"node_uuid"``, ``"edge_uuid"``, or ``"edge_triple"``.

    ``missing`` carries *every* unresolved key, not just the one named in the
    message. Each entry is the route's owner identity followed by
    ``(data_type, name)``: a 3-tuple for the single-column routes
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
        # Sequence in because list is invariant; list out so consumers get one
        # predictable type.
        self.missing: list[tuple[str, ...]] | None = None if missing is None else list(missing)


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


class AmbiguousEdgeError(ValidationError):
    """An edge triple matches more than one edge and no ``name`` narrowed it.

    ``edge`` is a multigraph: ``(edge_type, from_node_uuid, to_node_uuid,
    name)`` is the unique key, so several *parallel* edges (the six circuits
    of a double-circuit corridor, say) can share one endpoint pair and type
    and are told apart by their ``name``. Any triple-addressed lookup that
    lands on more than one of them is a genuinely ambiguous address, and
    energydb refuses to guess.

    ``matches`` carries every candidate as ``{"uuid": UUID, "name": str |
    None}`` in a stable order, so an API server can render a "which circuit
    did you mean?" choice instead of parsing the message. The fix is in the
    message too: pass ``name=`` (fluent addressing) or add an ``edge_name``
    column (manifest routing).
    """

    def __init__(
        self,
        message: str,
        *,
        from_path: str | None = None,
        to_path: str | None = None,
        edge_type: str | None = None,
        matches: Sequence[Mapping[str, Any]] | None = None,
    ):
        super().__init__(message)
        self.from_path = from_path
        self.to_path = to_path
        self.edge_type = edge_type
        self.matches: list[dict[str, Any]] | None = None if matches is None else [dict(m) for m in matches]


class UnchangedScopeError(ValidationError):
    """``skip_unchanged`` was asked for with a comparison key that would lose data.

    Raised when ``unchanged_scope="valid_time"`` is requested explicitly for a
    manifest containing OVERLAPPING series: that key ignores
    ``knowledge_time``, so a genuine republication whose values happen to match
    the previous one would be dropped. ``overlapping_series_ids`` carries the
    offending series.
    """

    def __init__(self, message: str, *, overlapping_series_ids: Collection[int] | None = None):
        super().__init__(message)
        # Sorted, because the raise site holds a frozenset and an unordered
        # attribute makes assertions and log lines unstable.
        self.overlapping_series_ids: list[int] | None = (
            None if overlapping_series_ids is None else sorted(overlapping_series_ids)
        )


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
    "AmbiguousEdgeError",
    "ConfigurationError",
    "EdgeNotFoundError",
    "EnergyDBError",
    "IncompatibleUnitError",
    "ManifestError",
    "NodeNotFoundError",
    "NotFoundError",
    "SeriesNotFoundError",
    "UnchangedScopeError",
    "ValidationError",
]
