"""EnergyDB — Energy database extending TimeDB with hierarchical asset management."""

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

from energydatamodel import (  # noqa: E402
    # Areas
    Area,
    # Semantic bases
    Asset,
    BiddingZone,
    Collection,
    ControlArea,
    Country,
    Edge,
    # Core hierarchy
    Element,
    EnergyCommunity,
    GridNode,
    # Quantities
    Kind,
    MultiSite,
    Node,
    NodeAsset,
    Portfolio,
    Quantity,
    Reference,
    Region,
    Scope,
    Sensor,
    # Containers
    Site,
    SynchronousArea,
    VirtualPowerPlant,
    WeatherCell,
    # Sub-namespaces — tech-specific equipment lives here
    battery,
    build_metric,
    building,
    # Vocabulary constructors
    cross_border_flow,
    electricity_demand,
    electricity_demand_area,
    electricity_supply,
    electricity_supply_area,
    gas_demand,
    gas_supply,
    grid,
    grid_frequency,
    heating_demand,
    heatpump,
    hydro,
    solar,
    spot_price,
    temperature,
    weather,
    wind,
)
from timedatamodel import (  # noqa: E402
    DataShape,
    DataType,
    Frequency,
    TimeSeries,
    TimeSeriesType,
)

from energydb._transaction import Transaction  # noqa: E402
from energydb.client import Client  # noqa: E402
from energydb.diff import EdgeChange, EdgeSnapshot, NodeChange, NodeSnapshot, TreeDiff  # noqa: E402
from energydb.scope import EdgeScope, NodeScope  # noqa: E402
from energydb.units import IncompatibleUnitError  # noqa: E402

__all__ = [
    # Client
    "Client",
    "IncompatibleUnitError",
    "NodeScope",
    "EdgeScope",
    "Transaction",
    # Diff
    "TreeDiff",
    "NodeChange",
    "EdgeChange",
    "NodeSnapshot",
    "EdgeSnapshot",
    # Core hierarchy
    "Element",
    "Node",
    "Edge",
    "Reference",
    # Semantic bases
    "Asset",
    "NodeAsset",
    "GridNode",
    "Sensor",
    "Collection",
    "Area",
    # Areas
    "BiddingZone",
    "ControlArea",
    "Country",
    "SynchronousArea",
    "WeatherCell",
    # Containers
    "Portfolio",
    "Site",
    "MultiSite",
    "Region",
    "EnergyCommunity",
    "VirtualPowerPlant",
    # Time series types
    "TimeSeries",
    "DataType",
    "DataShape",
    "Frequency",
    "TimeSeriesType",
    # Vocabulary constructors
    "cross_border_flow",
    "electricity_demand",
    "electricity_demand_area",
    "electricity_supply",
    "electricity_supply_area",
    "gas_demand",
    "gas_supply",
    "grid_frequency",
    "heating_demand",
    "spot_price",
    "temperature",
    # Quantities
    "Kind",
    "Quantity",
    "Scope",
    "build_metric",
    # Sub-namespaces (tech-specific equipment)
    "battery",
    "building",
    "grid",
    "heatpump",
    "hydro",
    "solar",
    "weather",
    "wind",
]
