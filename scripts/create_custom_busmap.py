
"""
this script aggregates all buses per country except germany (DE)

Outputs
-------
busmap_base_s_{cluster}.csv

Description
-----------
version 1.0.0

"""

import logging
from functools import reduce

#import geopandas as gpd
#import numpy as np
import pandas as pd
#import pypsa
#import scipy as sp
#from pypsa.clustering.spatial import busmap_by_stubs, get_clustering_from_busmap
#from scipy.sparse.csgraph import connected_components, dijkstra

from scripts._helpers import configure_logging, set_scenario_config
from scripts.cluster_network import busmap_for_admin_regions, cluster_regions

logger = logging.getLogger(__name__)

def get_bus_ids(busmap):
    return busmap["busmap"].unique()

def count_DE_buses(busmap):
    return len(busmap.loc[busmap["busmap"].str.contains("DE"), "busmap"].unique())

def agg_to_1busPerCountry_exceptDE(busmap):
    # change all mappings to one destination-bus except in germany (DE)
    bus_ids = get_bus_ids(busmap)
    busmap_new = busmap.copy()
    for bus in bus_ids:
        if "DE" not in bus and not bus.endswith(" 0"):
            busmap_new.loc[busmap_new["busmap"] == bus,"busmap"] = bus[0:3] + " 0"
    return busmap_new


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake

        snakemake = mock_snakemake("create_custom_busmap", base_network="osm")
    configure_logging(snakemake)
    set_scenario_config(snakemake)

    params = snakemake.params
    base_network = str(snakemake.wildcards.base_network)

    # import busmap file
    busmap = pd.read_csv(snakemake.input.busmap)

    bus_count = len(get_bus_ids(busmap))
    
    busmap_new = agg_to_1busPerCountry_exceptDE(busmap)

    bus_count_new = len(get_bus_ids(busmap_new))

    bus_count_DE = count_DE_buses(busmap_new)
    DE_buses_share = int((bus_count_DE / bus_count) * 100)
    DE_buses_share_new = int((bus_count_DE / bus_count_new) * 100)

    # export to csv
    busmap_new.to_csv(snakemake.output[0],index=False)

    logger.info(
        f"\nSimplified network outside DE:\n"
        f"{bus_count} buses initially\n"
        f"{bus_count_new} buses remaining\n"
        f"{bus_count_DE} remaining buses in DE\n"
        f"Share of DE buses is now {DE_buses_share_new}% (initially {DE_buses_share}%)\n"
    )