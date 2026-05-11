"""Serialization between EnergyDataModel objects and database rows.

Thin layer over EDM's JSON round-trip system (``energydatamodel.json_io``).
Each EDM element maps to a single DB row with structural columns
(``uuid``, ``node_type`` / ``edge_type``, ``name``, FK uuids)
plus a single ``data`` JSONB column carrying every remaining field —
geometry (GeoJSON), tz (ZoneInfo), commissioning_date (ISO), sensor
height, and all domain properties.

Identity is the EDM ``Element.id`` (UUID7) — round-tripped as the row's
``uuid`` PK on both ``node`` and ``edge``.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import energydatamodel as edm
from energydatamodel.json_io import get_registry
from energydatamodel.reference import Reference
from psycopg.types.json import Jsonb

# ``id`` is round-tripped via the dedicated ``uuid`` column, not through the
# ``data`` JSONB blob. Same for fields stored as their own columns.
_NODE_EXCLUDES = {"id", "timeseries"}
_EDGE_EXCLUDES = {"id", "from_element", "to_element", "timeseries"}


def _type_registry() -> dict[str, type]:
    """Name → class lookup for every EDM Element subclass (legacy re-export)."""
    return get_registry()


# ---------------------------------------------------------------------------
# Node serialization
# ---------------------------------------------------------------------------


def serialize_node(edm_obj) -> dict[str, Any]:
    """Convert any EDM Node to a dict suitable for database insertion.

    Returns a dict with structural columns (``uuid``, ``node_type``, ``name``)
    and a JSONB-wrapped ``data`` blob carrying everything else. Children are
    excluded — tree structure is stored via the ``parent_uuid`` column.
    """
    storage = edm.element_to_storage_dict(edm_obj, extra_excludes=_NODE_EXCLUDES)
    node_type = storage.pop("__type__")
    name = storage.pop("name", None) or node_type
    return {
        "uuid": edm_obj.id,
        "node_type": node_type,
        "name": name,
        "data": Jsonb(storage),
    }


def reconstruct_node(row: dict[str, Any]):
    """Reconstruct an EDM Node from a database row dict.

    Expects ``row`` to have ``uuid``, ``node_type``, ``name``, and ``data`` (a
    plain dict as returned by psycopg for JSONB columns).

    The reconstructed element's ``id`` is populated from ``row["uuid"]`` so
    the in-memory identity matches the persistent identity.
    """
    node_type = row["node_type"]
    cls = _type_registry().get(node_type)
    if cls is None:
        raise ValueError(f"Unknown node type: {node_type}")
    if not issubclass(cls, (edm.Node, edm.Collection)):
        raise TypeError(f"node table row has type {node_type} which is not a Node or Collection subclass")

    data = dict(row.get("data") or {})
    data["__type__"] = node_type
    if row.get("name"):
        data["name"] = row["name"]
    # Carry the persistent uuid into the EDM ``id`` field via the wire format.
    uuid_val = row["uuid"]
    data["id"] = str(uuid_val) if isinstance(uuid_val, UUID) else uuid_val
    return edm.element_from_json(data)


# ---------------------------------------------------------------------------
# Edge serialization
# ---------------------------------------------------------------------------


def serialize_edge(edm_obj) -> dict[str, Any]:
    """Convert an EDM Edge to a dict suitable for database insertion.

    The edge name is a human label, not an identifier (the triple
    ``(edge_type, from_node_uuid, to_node_uuid)`` is the upsert key).
    ``from_element`` / ``to_element`` are excluded from ``data`` — they're
    stored as ``from_node_uuid`` / ``to_node_uuid`` FK columns.
    """
    storage = edm.element_to_storage_dict(edm_obj, extra_excludes=_EDGE_EXCLUDES)
    edge_type = storage.pop("__type__")
    name = storage.pop("name", None)
    return {
        "uuid": edm_obj.id,
        "edge_type": edge_type,
        "name": name,
        "data": Jsonb(storage),
    }


def reconstruct_edge(row: dict[str, Any]):
    """Reconstruct an EDM Edge from a database row dict.

    Expects ``row`` to have ``uuid``, ``edge_type``, ``name``, ``data``,
    ``from_node_uuid``, ``to_node_uuid``. Endpoints are returned as
    :class:`Reference` objects holding the endpoint uuids — no path round-
    trip needed.
    """
    edge_type = row["edge_type"]
    cls = _type_registry().get(edge_type)
    if cls is None:
        raise ValueError(f"Unknown edge type: {edge_type}")
    if not issubclass(cls, edm.Edge):
        raise TypeError(f"edge table row has type {edge_type} which is not an Edge subclass")

    data = dict(row.get("data") or {})
    data["__type__"] = edge_type
    if row.get("name"):
        data["name"] = row["name"]
    uuid_val = row["uuid"]
    data["id"] = str(uuid_val) if isinstance(uuid_val, UUID) else uuid_val

    from_uuid = row.get("from_node_uuid")
    to_uuid = row.get("to_node_uuid")
    if from_uuid is not None:
        data["from_element"] = {"__ref__": str(from_uuid) if isinstance(from_uuid, UUID) else from_uuid}
    if to_uuid is not None:
        data["to_element"] = {"__ref__": str(to_uuid) if isinstance(to_uuid, UUID) else to_uuid}

    obj = edm.element_from_json(data)
    # Defensive: if the JSON blob carried stale endpoint refs, the FK columns
    # we just attached are authoritative — overwrite.
    if from_uuid is not None:
        obj.from_element = Reference(from_uuid if isinstance(from_uuid, UUID) else UUID(str(from_uuid)))
    if to_uuid is not None:
        obj.to_element = Reference(to_uuid if isinstance(to_uuid, UUID) else UUID(str(to_uuid)))
    return obj
