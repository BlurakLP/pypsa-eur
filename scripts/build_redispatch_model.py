"""
Transfer capacity and dispatch from a solved network to a prepared but unsolved network

Outputs
-------


Description
-----------

This script is partially based on the official redispatch example:
https://docs.pypsa.org/latest/examples/scigrid-redispatch/#load-example-network
"""

import logging
from functools import reduce

#import geopandas as gpd
#import numpy as np
import pandas as pd
import pypsa
#import scipy as sp
#from pypsa.clustering.spatial import busmap_by_stubs, get_clustering_from_busmap
#from scipy.sparse.csgraph import connected_components, dijkstra

from scripts._helpers import configure_logging, set_scenario_config
from scripts.cluster_network import busmap_for_admin_regions, cluster_regions

logger = logging.getLogger(__name__)

def add_dispatch(net_in, net_out):
    # get the dispatch from one network
    net_in_dispatch_gen = net_in.generators_t.p / net_in.generators.p_nom
    # set the dispatch in the redispatch model
    net_out.generators_t.p_min_pu = net_in_dispatch_gen
    net_out.generators_t.p_max_pu = net_in_dispatch_gen

    # get the dispatch from one network
    net_in_dispatch_storage = net_in.storage_units_t.p / net_in.storage_units.p_nom
    # set the dispatch in the redispatch model
    net_out.storage_units_t.p_min_pu = net_in_dispatch_storage
    net_out.storage_units_t.p_max_pu = net_in_dispatch_storage

    # get the dispatch from one network
    net_in_dispatch_store = net_in.stores_t.e / net_in.stores.e_nom
    # set the dispatch in the redispatch model
    net_out.stores_t.e_min_pu = net_in_dispatch_store
    net_out.stores_t.e_max_pu = net_in_dispatch_store

    return net_out

def add_redispatch_capacity(net_in, net_out):
    # create list of new (fictional) generators (for each generator: one for ramping up, one for ramping down)
    g_up = net_out.generators.copy()
    g_down = net_out.generators.copy()

    # name new generators
    g_up.index = g_up.index.map(lambda x: x + " ramp up")
    g_down.index = g_down.index.map(lambda x: x + " ramp down")

    # get the capacity that the generators can ramp up/down
    up_capacity = (net_in.get_switchable_as_dense("Generator", "p_max_pu") * net_in.generators.p_nom - net_in.generators_t.p).clip(0) / net_in.generators.p_nom
    down_capacity = -net_in.generators_t.p / net_in.generators.p_nom

    # ?
    up_capacity.columns = up_capacity.columns.map(lambda x: x + " ramp up")
    down_capacity.columns = down_capacity.columns.map(lambda x: x + " ramp down")

    # add the ramp-up/-down generators to the redispatch model network
    net_out.add("Generator", g_up.index, p_max_pu=up_capacity, **g_up.drop("p_max_pu", axis=1))
    net_out.add("Generator", g_down.index, p_min_pu=down_capacity, p_max_pu=0, **g_down.drop(["p_max_pu", "p_min_pu"], axis=1));

    return net_out

def fix_capacity(components_from_solved, network_template_RM, attr):
    # copy from solved network and adjust parameters
    components = components_from_solved.copy()
    extendable_components = components_from_solved[components_from_solved.loc[:, f"{attr}_nom_extendable"]].index
    components.loc[extendable_components,f"{attr}_nom"] = components_from_solved.loc[extendable_components,f"{attr}_nom_opt"]
    components.loc[extendable_components,f"{attr}_nom_min"] = components_from_solved.loc[extendable_components,f"{attr}_nom_opt"]
    components.loc[extendable_components,f"{attr}_nom_max"] = components_from_solved.loc[extendable_components,f"{attr}_nom_opt"]
    # components.loc[:,f"{attr}_extendable"] = False # not needed because I already fix the expansion limits
    
    # retrieve the detailed bus allocation
    for idx in components.index:
        components.loc[idx,"bus"] = network_template_RM.storage_units.loc[idx,"bus"]
    
    return components

def transfer_capacity(network_MM_solved, network_template_RM):
    # generators
    network_template_RM.generators = fix_capacity(network_MM_solved.generators, network_template_RM, "p")
    # stores
    network_template_RM.stores = fix_capacity(network_MM_solved.stores, network_template_RM, "e")
    # storage_units
    network_template_RM.storage_units = fix_capacity(network_MM_solved.storage_units, network_template_RM, "p")

    # leave the lines and links as they are in the RM (they were not extended and flow should be (re)calculated)

    return network_template_RM


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake

        snakemake = mock_snakemake("simplify_network")
    configure_logging(snakemake)
    set_scenario_config(snakemake)

    params = snakemake.params

    # load solved network of the market model run
    MM_net = pypsa.Network(snakemake.input.MM_network)

    # get the unsolved but prepared network of the redispatch model
    RM_net = pypsa.Network(snakemake.input.RM_network)

    # transfer the capacity and set extendability
    RM_net_with_capacity = transfer_capacity(MM_net, RM_net)
    # add the dispatch to the network
    RM_net_with_dispatch = add_dispatch(MM_net, RM_net_with_capacity)
    # add redispatch capacities
    RM_net_prepared = add_redispatch_capacity(MM_net, RM_net_with_dispatch)

    RM_net_prepared.export_to_netcdf(snakemake.output.network)

    logger.info(
        f"Added dispatch to the network\n"
    )
