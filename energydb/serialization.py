"""Serialization between EnergyDataModel objects and database rows.

Thin layer over EDM's JSON round-trip system (``energydatamodel.json_io``).
Each EDM element maps to a single DB row with structural columns
(``node_type``/``edge_type``, ``name``, FKs) plus a single ``data`` JSONB
column carrying every remaining field — geometry (GeoJSON), tz (ZoneInfo),
commissioning_date (ISO), sensor height, and all domain properties.
"""

from __future__ import annotations

from typing import Any

import energydatamodel as edm
from energydatamodel.json_io import get_registry
from energydatamodel.reference import Reference
from psycopg.types.json import Jsonb


_EDGE_EXCLUDES = {"from_entity", "to_entity"}  # stored as FK columns


def _type_registry() -> dict[str, type]:
    """Name → class lookup for every EDM Element subclass (legacy re-export)."""
    return get_registry()


# ---------------------------------------------------------------------------
# Node serialization
# ---------------------------------------------------------------------------


def serialize_node(edm_obj) -> dict[str, Any]:
    """Convert any EDM Node to a dict suitable for database insertion.

    ``data`` carries everything except the structural ``node_type`` and
    ``name`` columns (which live as dedicated fields for uniqueness and
    lookup). Children are excluded — tree structure is stored via the
    ``parent_id`` column.
    """
    storage = edm.element_to_storage_dict(edm_obj)
    node_type = storage.pop("__type__")
    name = storage.pop("name", None) or node_type
    return {
        "node_type": node_type,
        "name": name,
        "data": Jsonb(storage),
    }


def reconstruct_node(row: dict[str, Any]):
    """Reconstruct an EDM Node from a database row dict.

    Expects ``row`` to have ``node_type``, ``name``, and ``data`` (a plain
    dict as returned by psycopg for JSONB columns).
    """
    node_type = row["node_type"]
    cls = _type_registry().get(node_type)
    if cls is None:
        raise ValueError(f"Unknown node type: {node_type}")
    if not issubclass(cls, (edm.Node, edm.Collection)):
        raise TypeError(
            f"node table row has type {node_type} which is not a Node or Collection subclass"
        )

    data = dict(row.get("data") or {})
    data["__type__"] = node_type
    if row.get("name"):
        data["name"] = row["name"]
    return edm.element_from_json(data)


# ---------------------------------------------------------------------------
# Edge serialization
# ---------------------------------------------------------------------------


def serialize_edge(edm_obj) -> dict[str, Any]:
    """Convert an EDM Edge to a dict suitable for database insertion.

    ``from_entity`` / ``to_entity`` are excluded from ``data`` — they're
    stored as ``from_node_id`` / ``to_node_id`` FK columns and resolved by
    the caller.
    """
    storage = edm.element_to_storage_dict(edm_obj, extra_excludes=_EDGE_EXCLUDES)
    edge_type = storage.pop("__type__")
    name = storage.pop("name", None)
    return {
        "edge_type": edge_type,
        "name": name,
        "data": Jsonb(storage),
    }


def reconstruct_edge(row: dict[str, Any]):
    """Reconstruct an EDM Edge from a database row dict.

    Sets ``from_entity`` / ``to_entity`` as :class:`Reference` objects
    pointing to the slash-joined node paths provided by the caller (which
    resolved them from the FK ``from_node_id`` / ``to_node_id`` columns).
    """
    edge_type = row["edge_type"]
    cls = _type_registry().get(edge_type)
    if cls is None:
        raise ValueError(f"Unknown edge type: {edge_type}")
    if not issubclass(cls, edm.Edge):
        raise TypeError(
            f"edge table row has type {edge_type} which is not an Edge subclass"
        )

    data = dict(row.get("data") or {})
    data["__type__"] = edge_type
    if row.get("name"):
        data["name"] = row["name"]
    obj = edm.element_from_json(data)

    if row.get("from_node_path"):
        obj.from_entity = Reference(row["from_node_path"])
    if row.get("to_node_path"):
        obj.to_entity = Reference(row["to_node_path"])
    return obj
