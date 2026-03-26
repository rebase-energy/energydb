"""Example usage of EnergyDB."""

import energydatamodel as edm

from energydb import EnergyDataClient


def main():
    # Connect to database
    edb = EnergyDataClient(conninfo="postgres://user:pass@localhost/energydb")
    edb.create()

    # Build hierarchy using EnergyDataModel
    t1 = edm.WindTurbine(name="T01", capacity=3.5, hub_height=80)
    t2 = edm.WindTurbine(name="T02", capacity=3.5, hub_height=80)
    pv = edm.PVSystem(name="PV01", capacity=10, surface_tilt=25)

    site_a = edm.Site(name="Offshore-1", assets=[t1, t2], latitude=55.0, longitude=3.0)
    site_b = edm.Site(name="Rooftop-1", assets=[pv], latitude=52.0, longitude=4.5)

    portfolio = edm.Portfolio(name="My Portfolio", collections=[site_a, site_b])

    # Persist entire tree in one atomic transaction
    edb.save_portfolio(portfolio)

    # Attach time series to an asset
    edb.add_series_to_asset("T01", "active_power", role="active_power", unit="MW")
    edb.add_series_to_asset("T01", "wind_speed", role="wind_speed", unit="m/s")

    # Read back the asset
    turbine = edb.get_asset("T01")
    print(f"Turbine: {turbine.name}, capacity: {turbine.capacity}")

    # Reconstruct full tree
    p = edb.get_portfolio("My Portfolio")
    print(f"Portfolio: {p.name}")

    # Query across hierarchy
    wind_assets = edb.query_assets(portfolio="My Portfolio", asset_type="WindTurbine")
    print(f"Wind turbines in portfolio: {[a.name for a in wind_assets]}")

    # Cleanup
    edb.delete()


if __name__ == "__main__":
    main()
