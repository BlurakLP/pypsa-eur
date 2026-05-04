"""
create mapping dictionary between buses and the bidding zones

Outputs
-------


Description
-----------

[description]
"""

import logging
from functools import reduce

import geopandas as gpd
import numpy as np
import pandas as pd
import pypsa
from shapely.geometry import Point
#import scipy as sp
#from pypsa.clustering.spatial import busmap_by_stubs, get_clustering_from_busmap
#from scipy.sparse.csgraph import connected_components, dijkstra

from scripts._helpers import configure_logging, set_scenario_config
from scripts.cluster_network import busmap_for_admin_regions, cluster_regions

logger = logging.getLogger(__name__)

# create mapping dictionary between buses and the bidding zones
def create_bz_mapping(RM_net, bz_shapes):
    # make Points out of the busses. Warning: float-conversion may lead to loss of precision. 
    bus_centerpoint_list = list(map(lambda x, y: Point(float(x), float(y)), RM_net.buses["x"], RM_net.buses["y"]))

    # get Points into series (bus name as index)
    buses_GeoSeries = gpd.GeoSeries(bus_centerpoint_list, index=range(0, len(bus_centerpoint_list)))

    mapping_dict = {}
    bus_within_bz_mask_list = []
    # make mask of buses that are not in DE
    notDE_mask = (RM_net.buses["country"] != "DE").tolist()
    bus_within_bz_mask_list.append(notDE_mask)

    # iterate bidding zones in DE: check in which bidding zone the bus lies (per centerpoint)
    for bz_idx in range(0, len(bz_shapes)):
        # make mask where True means bus lies within the bidding zone
        bus_within_bz_mask = bz_shapes.iloc[bz_idx]["geometry"].contains(buses_GeoSeries).tolist()
        bus_within_bz_mask_list.append(bus_within_bz_mask)

        # get the buses that lie within bz via the mask
        mapping = RM_net.buses[bus_within_bz_mask]

        # prepare appending to mapping dictionary
        keys = mapping.index.tolist()
        bz_label = bz_shapes.iloc[bz_idx]["zone_name"]
        values = [bz_label for i in keys]

        # append to mapping dictionary
        mapping_dict.update(dict(zip(keys, values)))

    # check if there are buses in DE that are not in any of the bidding zones (probably because of geometry issues in the above code)
    new_mask = ~np.logical_or.reduce(np.array(bus_within_bz_mask_list))
    unmapped_buses = RM_net.buses[new_mask]

    # make a column in missing_buses that contains a geopandas point of x and y
    unmapped_buses.loc[:,"point"] = [Point(float(x), float(y)) for x, y in zip(unmapped_buses.loc[:,"x"], unmapped_buses.loc[:,"y"])]

    # determine the nearest bidding zone to the unmapped bus
    unmapped_buses.loc[:,"nearest_bz_idx"] = [bus_center.distance(bz_shapes.loc[:, "geometry"]).idxmin() for bus_center in unmapped_buses.loc[:,"point"]]
    unmapped_buses.loc[:,"nearest_bz"] = [bz_shapes.loc[nearest_bz_idx, "zone_name"] for nearest_bz_idx in unmapped_buses.loc[:,"nearest_bz_idx"]]

    # get the buses with mask
    mapping = RM_net.buses[new_mask]
    # prepare appending to mapping dictionary
    keys = mapping.index.tolist()
    values = unmapped_buses.loc[:,"nearest_bz"].tolist()
    # append to mapping dictionary
    mapping_dict.update(dict(zip(keys, values)))

    # append all the buses outside DE unchanged
    buses_notDE = RM_net.buses[notDE_mask].index.tolist()
    # append to mapping dictionary
    mapping_dict.update(dict(zip(buses_notDE, buses_notDE)))

    return mapping_dict

if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake

        snakemake = mock_snakemake("simplify_network")
    configure_logging(snakemake)
    set_scenario_config(snakemake)

    params = snakemake.params

    # load network 
    RM_net = pypsa.Network(snakemake.input.RM_network)

    # get the busshapes of the target bidding zones
    bz_shapes = gpd.read_file(snakemake.input.bz_shapes)
    # get bidding zones in DE
    bz_shapes_DE = bz_shapes[bz_shapes["zone_name"].str.contains("DE")]

    # create the mapping between buses and bz
    mapping_dict = create_bz_mapping(RM_net, bz_shapes_DE)

    # save mapping as csv
    mapping_dict_df = pd.DataFrame.from_dict(mapping_dict, orient="index", columns=["MM_bus"])
    mapping_dict_df.index.name = "RM_bus"
    mapping_dict_df.to_csv(snakemake.output.mapping)

    logger.info(
        f"Created mapping between bidding zones and buses\n"
    )
