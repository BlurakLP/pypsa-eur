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
    net_in_dispatch = net_in.generators_t.p / net_in.generators.p_nom

    # set the dispatch in the redispatch model
    net_out.generators_t.p_min_pu = net_in_dispatch
    net_out.generators_t.p_max_pu = net_in_dispatch

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


def transfer_capacity(network_MM_solved, network_template_RM):

    RM_net = network_template_RM.copy()

    # generators: copy from solved MM and adjust parameters
    RM_net.generators = network_MM_solved.generators.copy()
    RM_net.generators.loc[:,"p_nom_extendable"] = False

    # stores: copy from solved MM and adjust parameters
    RM_net.stores = network_MM_solved.stores.copy()
    RM_net.stores.loc[:,"p_nom_extendable"] = False

    # storage_units: copy from solved MM and adjust parameters
    RM_net.storage_units = network_MM_solved.storage_units.copy()
    RM_net.storage_units.loc[:,"p_nom_extendable"] = False

    # for generators, stores and storage_units: get the bus allocation from before the market model
    for idx in RM_net.generators.index:
        RM_net.generators.loc[idx,"bus"] = network_template_RM.generators.loc[idx,"bus"]

    for idx in RM_net.stores.index:
        RM_net.stores.loc[idx,"bus"] = network_template_RM.stores.loc[idx,"bus"]

    for idx in RM_net.storage_units.index:
        RM_net.storage_units.loc[idx,"bus"] = network_template_RM.storage_units.loc[idx,"bus"]

    # for lines and links: transfer the extendability from before the market model
    for idx in RM_net.links.index:
        RM_net.links.loc[idx,"p_nom_extendable"] = network_template_RM.links.loc[idx,"p_nom_extendable"]

    for idx in RM_net.lines.index:
        RM_net.lines.loc[idx,"p_nom_extendable"] = network_template_RM.lines.loc[idx,"p_nom_extendable"]

    return RM_net


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
