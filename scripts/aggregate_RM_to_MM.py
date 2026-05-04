"""
aggregate components of RM network to get MM network according to bidding zone configuration

Outputs
-------


Description
-----------

[description]
"""

import logging
from functools import reduce

import geopandas as gpd
#import numpy as np
import pandas as pd
import pypsa
#import scipy as sp
#from pypsa.clustering.spatial import busmap_by_stubs, get_clustering_from_busmap
#from scipy.sparse.csgraph import connected_components, dijkstra

from scripts._helpers import configure_logging, set_scenario_config
from scripts.cluster_network import busmap_for_admin_regions, cluster_regions

logger = logging.getLogger(__name__)

def aggregate_lines_or_links(df_components, aggregation_methods, mapping_dict, groupby_column="pairing"):
    # replace both connected buses according to mapping
    df_components.loc[df_components.loc[:,"bus0"].str.contains("DE"),"bus0"] = df_components.loc[df_components.loc[:,"bus0"].str.contains("DE"),"bus0"].apply(lambda x: mapping_dict[x])
    df_components.loc[df_components.loc[:,"bus1"].str.contains("DE"),"bus1"] = df_components.loc[df_components.loc[:,"bus1"].str.contains("DE"),"bus1"].apply(lambda x: mapping_dict[x])

    # delete the lines within buses
    df_components.loc[:,"same_bus"] = df_components.loc[:,"bus0"] == df_components.loc[:,"bus1"]
    df_components = df_components.loc[~df_components.loc[:,"same_bus"]]
    df_components = df_components.drop(columns=["same_bus"])

    # add a helper column with the bus names in alphabetical order (to find identical pairings)
    df_components["pairing"] = df_components.apply(lambda row: "_".join(sorted([row["bus0"], row["bus1"]])), axis=1)

    # aggregate the lines where "pairing" is the same and apply different aggregation methods per column
    df_components = df_components.groupby(groupby_column).agg(aggregation_methods)
    df_components = df_components.drop(columns=["pairing"])

    # convert multiindex into one
    if isinstance(df_components.index, pd.MultiIndex):
        df_components.index = df_components.index.map(lambda x: "_".join(map(str, x)))
    else:
        # Falls es nur ein einfacher Index ist, einfach als String belassen
        df_components.index = df_components.index.astype(str)

    df_components.index.name = "name"
    return df_components

# move components to new buses (only generators, loads and StorageUnits)
def transfer_components(compontent_df, mapping_dict):
    # connect the battery buses to the corresponding bidding zone
    components_DE = compontent_df.loc[(compontent_df.index.str.contains("DE"))].index.tolist()
    for component in components_DE:
        compontent_df.loc[component,"bus"] = mapping_dict[compontent_df.loc[component,"bus"]]
    
    return compontent_df

# optional TODO: work with the dataframe instead of .remove and .add just like in lines/links
# TODO: control (str or SeriesLike[str]) – P,Q,V control strategy for power flow, must be "PQ", "PV" or "Slack". 
# Note that this attribute is an output inherited from the controls of the generators attached to the bus. Do not set by hand.
def aggregate_buses(network, mapping_dict, bz_shapes_DE):
    # get all the buses in DE (the RM buses)
    RM_buses_without_battery = network.buses.loc[(network.buses.index.str.contains("DE")) & (~network.buses.index.str.contains("battery"))].index.tolist()
    # create centroid of the bidding zone shapes ("EPSG:3857" = web mercator projection)
    bz_shapes_DE.loc[:,"centroid"] = bz_shapes_DE.geometry.to_crs(crs="EPSG:3857").centroid.to_crs(crs=network.srid)

    # remove old buses from network
    network.remove("Bus", RM_buses_without_battery)
    # make a new bus per bidding zone
    for bus_idx in range(0,len(bz_shapes_DE)):
        network.add("Bus", name=bz_shapes_DE.iloc[bus_idx]["zone_name"], v_nom = 380, x=bz_shapes_DE.iloc[bus_idx]["centroid"].x, y=bz_shapes_DE.iloc[bus_idx]["centroid"].y, carrier="AC", v_mag_pu_set=1.0, v_mag_pu_min=0, v_mag_pu_max=np.inf, control="PQ", sub_network="0", country="DE", substation_lv=1.0, substation_off=1.0)

    # connect the battery buses to the corresponding bidding zone
    battery_buses = network.buses.loc[(network.buses.index.str.contains("DE")) & (network.buses.index.str.contains("battery"))].index.tolist()
    for bus in battery_buses:
        bz = mapping_dict[bus]
        network.buses.loc[bus,"location"] = bz
        # set the coordinates to of the battery bus to the bus of the bidding zone
        network.buses.loc[bus,"x"] = network.buses.loc[bz,"x"]
        network.buses.loc[bus,"y"] = network.buses.loc[bz,"y"]

    return network

if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake

        snakemake = mock_snakemake("simplify_network")
    configure_logging(snakemake)
    set_scenario_config(snakemake)

    params = snakemake.params

    # load network 
    RM_net = pypsa.Network(snakemake.input.RM_network)
    MM_net = RM_net.copy()

    # load mapping
    mapping = pd.read_csv(snakemake.input.mapping, index_col=0)
    mapping_dict = mapping.to_dict()["MM_bus"]

    # get the busshapes of the target bidding zones
    bz_shapes = gpd.read_file(snakemake.input.bz_shapes)
    # get bidding zones in DE
    bz_shapes_DE = bz_shapes[bz_shapes["zone_name"].str.contains("DE")]

    # aggregate the buses per bidding zone (only in DE)
    MM_net = aggregate_buses(MM_net, mapping_dict, bz_shapes_DE)
    # for every generator in DE: replace connecting bus with new bz-bus according to mapping dictionary
    MM_net.generators = transfer_components(MM_net.generators, mapping_dict)
    # for every load in DE: replace connecting bus with new bz-bus according to mapping dictionary, TODO: Maybe aggregate loads per bus?
    MM_net.loads = transfer_components(MM_net.loads, mapping_dict)
    # for every StorageUnit in DE: replace connecting bus with new bz-bus according to mapping dictionary
    MM_net.storage_units = transfer_components(MM_net.storage_units, mapping_dict)
    # stores doesn't have to be transfered because they are attached to the battery buses (and these are already transfered)
    # sub_networks doesn't have to be transfered, because there are none in DE

    MM_net.export_to_netcdf(snakemake.output.MM_network)

    logger.info(
        f"Derived market model network from the redispatch model network and the bz-bus mapping\n"
    )
