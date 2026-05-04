
"""
this script aggregates all buses per country except germany (DE)

Outputs
-------


Description
-----------

[description]
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
    return len(busmap.loc["DE" in busmap["busmap"],"busmap"])

def agg_to_1busPerCountry_exceptDE(busmap):
    # change all mappings to one destination-bus except in germany (DE)
    for bus in bus_ids:
        if "DE" not in bus and not bus.endswith(" 0"):
            busmap.loc[busmap["busmap"] == bus,"busmap"] = bus[0:3] + " 0"
    return busmap


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake

        snakemake = mock_snakemake("simplify_network")
    configure_logging(snakemake)
    set_scenario_config(snakemake)

    params = snakemake.params

    # import busmap file
    busmap = pd.read_csv(snakemake.input.busmap)

    bus_ids = get_bus_ids(busmap)
    
    busmap_new = agg_to_1busPerCountry_exceptDE(busmap)

    bus_ids_new = get_bus_ids(busmap_new)

    nbr_DE_buses_new = count_DE_buses(busmap_new)
    DE_buses_share = round(nbr_DE_buses_new / bus_ids, 2) * 100
    DE_buses_share_new = round(nbr_DE_buses_new / bus_ids_new, 2) * 100

    # export to csv
    busmap_new.to_csv(snakemake.output.busmap,index=False)

    logger.info(
        f"Simplified network:\n"
        f"{bus_ids} different buses initially\n"
        f"{bus_ids_new} different buses remaining\n"
        f"{nbr_DE_buses_new} remaining buses in DE\n"
        f"Share of DE buses is now {DE_buses_share_new}% (initially {DE_buses_share}%)\n"
    )