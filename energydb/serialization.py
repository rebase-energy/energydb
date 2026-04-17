"""Serialization between EnergyDataModel objects and database rows."""

from __future__ import annotations

from typing import Any

import energydatamodel as edm
from energydatamodel.json_io import get_registry
from energydatamodel.reference import Reference
from psycopg.types.json import Jsonb
from shapely.geometry import Point


def _type_registry() -> dict[str, type]:
    """Return a name → class lookup for every Entity subclass EDM knows about.

    Relies on EDM's auto-registration in :func:`register_builtin_entities` so
    we don't have to hand-maintain a class list as EDM grows.
    """
    return get_registry()


# ---------------------------------------------------------------------------
# Node serialization
# ---------------------------------------------------------------------------


def serialize_node(edm_obj) -> dict[str, Any]:
    """Convert any EDM Node to a dict suitable for database insertion.

    The DB columns ``latitude``/``longitude``/``altitude`` are kept for
    indexing and convenience; if the element has a ``Point`` geometry, the
    coordinates are extracted from it (otherwise they're ``None``).
    """
    node_type = type(edm_obj).__name__
    name = getattr(edm_obj, "name", None) or node_type

    # Derive scalar coords from a Point geometry, when present.
    geom = getattr(edm_obj, "geometry", None)
    latitude = geom.y if isinstance(geom, Point) else None
    longitude = geom.x if isinstance(geom, Point) else None
    # ``z`` is shapely's altitude axis on 3D points.
    altitude = geom.z if isinstance(geom, Point) and geom.has_z else None

    return {
        "node_type": node_type,
        "name": name,
        "properties": Jsonb(edm_obj.to_properties()),
        "latitude": latitude,
        "longitude": longitude,
        "altitude": altitude,
        "timezone": _get_timezone(edm_obj),
    }


def reconstruct_node(row: dict[str, Any]):
    """Reconstruct an EDM Node from a database row dict.

    Looks up the class via EDM's auto-registry, rebuilds a Point geometry
    from the scalar lat/lon columns if present, and merges in the
    ``properties`` JSONB.
    """
    node_type = row["node_type"]
    cls = _type_registry().get(node_type)
    if cls is None:
        raise ValueError(f"Unknown node type: {node_type}")

    if not issubclass(cls, edm.Node):
        raise TypeError(
            f"node table row has type {node_type} which is not a Node subclass"
        )

    kwargs: dict[str, Any] = {}
    if row.get("name"):
        kwargs["name"] = row["name"]

    lat = row.get("latitude")
    lon = row.get("longitude")
    alt = row.get("altitude")
    if lat is not None and lon is not None:
        kwargs["geometry"] = (
            Point(lon, lat, alt) if alt is not None else Point(lon, lat)
        )

    # Merge in domain-specific properties
    properties = row.get("properties", {})
    if isinstance(properties, dict):
        kwargs.update(properties)

    return cls(**kwargs)


# ---------------------------------------------------------------------------
# Edge serialization
# ---------------------------------------------------------------------------


def serialize_edge(edm_obj) -> dict[str, Any]:
    """Convert an EDM Edge to a dict suitable for database insertion.

    Extracts edge_type, name, properties, and directed flag.
    from_entity / to_entity resolution is handled by the caller (client.py)
    since it requires DB lookups.
    """
    edge_type = type(edm_obj).__name__
    name = getattr(edm_obj, "name", None)

    return {
        "edge_type": edge_type,
        "name": name,
        "properties": Jsonb(edm_obj.to_properties()),
        "directed": getattr(edm_obj, "directed", True),
    }


def reconstruct_edge(row: dict[str, Any]):
    """Reconstruct an EDM Edge from a database row dict.

    The from_entity and to_entity fields are set as Reference objects
    pointing to the resolved node paths.
    """
    edge_type = row["edge_type"]
    cls = _type_registry().get(edge_type)
    if cls is None:
        raise ValueError(f"Unknown edge type: {edge_type}")

    if not issubclass(cls, edm.Edge):
        raise TypeError(
            f"edge table row has type {edge_type} which is not an Edge subclass"
        )

    kwargs: dict[str, Any] = {}
    if row.get("name"):
        kwargs["name"] = row["name"]
    kwargs["directed"] = row.get("directed", True)

    # Set from/to as Reference objects with node paths
    if row.get("from_node_path"):
        kwargs["from_entity"] = Reference(row["from_node_path"])
    if row.get("to_node_path"):
        kwargs["to_entity"] = Reference(row["to_node_path"])

    # Merge in domain-specific properties
    properties = row.get("properties", {})
    if isinstance(properties, dict):
        kwargs.update(properties)

    return cls(**kwargs)


def _get_timezone(obj) -> str | None:
    """Extract timezone string from an EDM object."""
    tz = getattr(obj, "tz", None)
    if tz is None:
        return None
    return str(tz)
