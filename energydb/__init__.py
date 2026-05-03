"""EnergyDB — Energy database extending TimeDB with hierarchical asset management."""

from energydatamodel import (
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
    # Containers
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
    Site,
    SynchronousArea,
    VirtualPowerPlant,
    WeatherCell,
    build_metric,
    # Vocabulary constructors
    cross_border_flow,
    electricity_demand,
    electricity_demand_area,
    electricity_supply,
    electricity_supply_area,
    gas_demand,
    gas_supply,
    grid_frequency,
    heating_demand,
    spot_price,
    temperature,
)
from energydatamodel.battery import Battery
from energydatamodel.building import Building, House
from energydatamodel.grid import (
    Carrier,
    DeliveryPoint,
    EdgeAsset,
    Interconnection,
    JunctionPoint,
    Line,
    Link,
    Meter,
    Network,
    Pipe,
    SubNetwork,
    Transformer,
)
from energydatamodel.heatpump import HeatPump
from energydatamodel.hydro import HydroPowerPlant, HydroTurbine, Reservoir
from energydatamodel.solar import PVArray, PVSystem, SolarPowerArea
from energydatamodel.weather import (
    HumiditySensor,
    RadiationSensor,
    RainSensor,
    TemperatureSensor,
    WindSpeedSensor,
)
from energydatamodel.wind import WindFarm, WindPowerArea, WindTurbine
from timedatamodel import (
    DataShape,
    DataType,
    Frequency,
    TimeSeries,
    TimeSeriesDescriptor,
    TimeSeriesType,
)

from energydb.client import EnergyDBClient
from energydb.diff import EdgeChange, EdgeSnapshot, NodeChange, NodeSnapshot, TreeDiff
from energydb.scope import EdgeScope, NodeScope
from energydb.units import IncompatibleUnitError

__all__ = [
    # Client
    "EnergyDBClient",
    "IncompatibleUnitError",
    "NodeScope",
    "EdgeScope",
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
    "EdgeAsset",
    "GridNode",
    "Sensor",
    "Collection",
    "Area",
    # Assets
    "Battery",
    "Building",
    "House",
    "HeatPump",
    "HydroPowerPlant",
    "HydroTurbine",
    "Reservoir",
    "PVSystem",
    "PVArray",
    "SolarPowerArea",
    "WindFarm",
    "WindTurbine",
    "WindPowerArea",
    # Grid nodes
    "JunctionPoint",
    "Meter",
    "DeliveryPoint",
    # Edges
    "Line",
    "Link",
    "Transformer",
    "Pipe",
    "Interconnection",
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
    "SubNetwork",
    "Network",
    # Sensors
    "TemperatureSensor",
    "RadiationSensor",
    "WindSpeedSensor",
    "HumiditySensor",
    "RainSensor",
    # Time series types
    "TimeSeries",
    "TimeSeriesDescriptor",
    "DataType",
    "DataShape",
    "Frequency",
    "TimeSeriesType",
    # Powergrid value types
    "Carrier",
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
]
