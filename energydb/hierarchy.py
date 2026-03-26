"""Hierarchy tree walk and reconstruction helpers."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

import energydatamodel as edm

from energydb.serialization import COLLECTION_TYPE_MAP


def walk_tree(
    collection,
    on_collection: Callable,
    on_asset: Callable,
    on_link: Callable,
    parent_id: Optional[int] = None,
):
    """Walk an EnergyDataModel collection tree, calling callbacks at each node.

    Args:
        collection: An EnergyDataModel collection (Portfolio, Site, etc.)
        on_collection: Called with (collection, parent_id) -> collection_id
        on_asset: Called with (asset) -> asset_id
        on_link: Called with (collection_id, asset_id)
        parent_id: Parent collection_id for nesting
    """
    col_id = on_collection(collection, parent_id)

    # Process direct assets
    for asset in getattr(collection, "assets", []):
        asset_id = on_asset(asset)
        on_link(col_id, asset_id)
        # Handle nested assets (e.g., WindFarm.wind_turbines)
        _walk_sub_assets(asset, on_asset, on_link, col_id)

    # Recurse into sub-collections
    for sub in getattr(collection, "collections", []):
        walk_tree(sub, on_collection, on_asset, on_link, parent_id=col_id)


def _walk_sub_assets(
    asset: edm.EnergyAsset,
    on_asset: Callable,
    on_link: Callable,
    col_id: int,
):
    """Handle asset types that contain sub-assets."""
    if isinstance(asset, edm.WindFarm) and getattr(asset, "wind_turbines", None):
        for turbine in asset.wind_turbines:
            t_id = on_asset(turbine)
            on_link(col_id, t_id)


def reconstruct_tree(
    collection_row: Dict[str, Any],
    get_assets: Callable[[int], List[Dict[str, Any]]],
    get_subcollections: Callable[[int], List[Dict[str, Any]]],
    reconstruct_asset_fn: Callable[[Dict[str, Any]], edm.EnergyAsset],
) -> Any:
    """Reconstruct a collection tree from database rows.

    Args:
        collection_row: The root collection row from DB
        get_assets: Function that returns asset rows for a collection_id
        get_subcollections: Function that returns child collection rows for a parent_id
        reconstruct_asset_fn: Function that converts an asset row to EnergyDataModel object
    """
    col_type = collection_row["collection_type"]
    col_id = collection_row["collection_id"]
    cls = COLLECTION_TYPE_MAP.get(col_type)
    if cls is None:
        raise ValueError(f"Unknown collection type: {col_type}")

    # Load assets for this collection
    asset_rows = get_assets(col_id)
    assets = [reconstruct_asset_fn(row) for row in asset_rows]

    # Recurse into sub-collections
    sub_rows = get_subcollections(col_id)
    subcollections = [
        reconstruct_tree(sub, get_assets, get_subcollections, reconstruct_asset_fn)
        for sub in sub_rows
    ]

    # Build the collection object
    kwargs = {"name": collection_row.get("name")}
    if assets:
        kwargs["assets"] = assets

    # Site uses 'assets' only; Portfolio/EnergyCommunity use both 'assets' and 'collections'
    if subcollections and hasattr(cls, "__dataclass_fields__") and "collections" in cls.__dataclass_fields__:
        kwargs["collections"] = subcollections
    elif subcollections and col_type == "Site":
        # Sites don't have sub-collections in EnergyDataModel, but we might
        # have nested sites stored as sub-collections with their assets
        pass

    # Add location fields for Site
    if col_type == "Site":
        if collection_row.get("latitude") is not None:
            kwargs["latitude"] = collection_row["latitude"]
        if collection_row.get("longitude") is not None:
            kwargs["longitude"] = collection_row["longitude"]

    return cls(**kwargs)
