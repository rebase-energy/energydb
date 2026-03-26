"""Serialization between EnergyDataModel objects and database rows."""

from __future__ import annotations

from typing import Any, Dict, Optional, Type

import energydatamodel as edm
from psycopg.types.json import Jsonb

# Maps asset_type strings to EnergyDataModel classes
ASSET_TYPE_MAP: Dict[str, Type[edm.EnergyAsset]] = {
    "WindTurbine": edm.WindTurbine,
    "WindFarm": edm.WindFarm,
    "PVSystem": edm.PVSystem,
    "SolarPowerArea": edm.SolarPowerArea,
    "Battery": edm.Battery,
    "HydroPowerPlant": edm.HydroPowerPlant,
    "HeatPump": edm.HeatPump,
}

# Maps collection_type strings to EnergyDataModel classes
COLLECTION_TYPE_MAP: Dict[str, Type] = {
    "Portfolio": edm.Portfolio,
    "Site": edm.Site,
    "EnergyCommunity": edm.EnergyCommunity,
}

# Fields stored as top-level columns (not in properties JSONB)
_BASE_FIELDS = {"name", "latitude", "longitude", "altitude", "timezone", "tz",
                "timeseries", "location", "_id"}


def serialize_asset(asset: edm.EnergyAsset) -> Dict[str, Any]:
    """Convert an EnergyAsset to a dict suitable for database insertion."""
    asset_type = type(asset).__name__
    data = asset.to_json().get(asset_type, {})

    # Separate base fields from domain-specific properties
    properties = {}
    for k, v in data.items():
        if k not in _BASE_FIELDS and v is not None:
            properties[k] = _make_json_safe(v)

    return {
        "asset_type": asset_type,
        "name": asset.name,
        "properties": Jsonb(properties),
        "latitude": getattr(asset, "latitude", None),
        "longitude": getattr(asset, "longitude", None),
        "altitude": getattr(asset, "altitude", None),
        "timezone": _get_timezone(asset),
    }


def reconstruct_asset(row: Dict[str, Any]) -> edm.EnergyAsset:
    """Reconstruct an EnergyAsset from a database row."""
    asset_type = row["asset_type"]
    cls = ASSET_TYPE_MAP.get(asset_type)
    if cls is None:
        raise ValueError(f"Unknown asset type: {asset_type}")

    kwargs = {}
    if row.get("name"):
        kwargs["name"] = row["name"]
    if row.get("latitude") is not None:
        kwargs["latitude"] = row["latitude"]
    if row.get("longitude") is not None:
        kwargs["longitude"] = row["longitude"]
    if row.get("altitude") is not None:
        kwargs["altitude"] = row["altitude"]

    # Merge in domain-specific properties
    properties = row.get("properties", {})
    if isinstance(properties, dict):
        kwargs.update(properties)

    return cls(**kwargs)


def serialize_collection(collection) -> Dict[str, Any]:
    """Convert a collection (Portfolio, Site, etc.) to a dict for DB insertion."""
    collection_type = type(collection).__name__
    return {
        "collection_type": collection_type,
        "name": getattr(collection, "name", None),
        "properties": Jsonb({}),
        "latitude": getattr(collection, "latitude", None),
        "longitude": getattr(collection, "longitude", None),
    }


def _get_timezone(asset: edm.EnergyAsset) -> Optional[str]:
    """Extract timezone string from an asset."""
    tz = getattr(asset, "tz", None) or getattr(asset, "timezone", None)
    if tz is None:
        return None
    return str(tz)


def _make_json_safe(value: Any) -> Any:
    """Convert values to JSON-serializable types."""
    if hasattr(value, "tolist"):
        return value.tolist()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    return value
