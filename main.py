"""Example usage of EnergyDB."""

import pandas as pd
import numpy as np
import energydatamodel as edm

from energydb import EnergyDataClient


def main():
    edb = EnergyDataClient(conninfo="postgres://user:pass@localhost/energydb")
    edb.create()

    # Create time series
    index = pd.date_range("2025-01-01", periods=24, freq="h", tz="UTC")
    ts_power = edm.TimeSeries(name="active_power", df=pd.DataFrame({
        "valid_time": index,
        "value": np.clip(np.random.normal(2.5, 0.8, 24), 0, 3.5),
    }))

    # Build hierarchy with time series attached
    t1 = edm.WindTurbine(name="T01", capacity=3.5, hub_height=80, timeseries=[ts_power])
    t2 = edm.WindTurbine(name="T02", capacity=3.5, hub_height=80)
    pv = edm.PVSystem(name="PV01", capacity=10, surface_tilt=25)

    site_a = edm.Site(name="Offshore-1", assets=[t1, t2], latitude=55.0, longitude=3.0)
    site_b = edm.Site(name="Rooftop-1", assets=[pv], latitude=52.0, longitude=4.5)
    portfolio = edm.Portfolio(name="My Portfolio", collections=[site_a, site_b])

    # Save everything in one call
    edb.save(portfolio)

    # Read back
    turbine = edb.get_asset("T01")
    print(f"Turbine: {turbine.name}, capacity: {turbine.capacity}")

    ts = edb.get_asset_series("T01", role="active_power").read()
    print(f"Time series: {ts.to_pandas().shape[0]} rows")

    p = edb.get_portfolio("My Portfolio")
    print(f"Portfolio: {p.name}")

    edb.delete()


if __name__ == "__main__":
    main()
