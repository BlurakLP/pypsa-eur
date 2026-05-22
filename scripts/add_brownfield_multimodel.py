# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT
"""
Prepares brownfield data from previous planning horizon.
"""

import logging

import numpy as np
import pandas as pd
import pypsa
import xarray as xr

from scripts._helpers import (
    configure_logging,
    get_snapshots,
    sanitize_custom_columns,
    set_scenario_config,
    update_config_from_wildcards,
)
from scripts.add_electricity import flatten, sanitize_carriers
from scripts.add_existing_baseyear import add_build_year_to_new_assets

logger = logging.getLogger(__name__)
idx = pd.IndexSlice

def remove_redispatch_generators(n):
    redispatch_generators = n.generators[(n.generators.index.str.contains("ramp up") | n.generators.index.str.contains("ramp down"))].index
    n.remove("Generator", redispatch_generators)
    return n

def add_brownfield(
    n,
    n_p,
    year,
    capacity_threshold=None,
):
    """
    Add brownfield capacity from previous network.

    Parameters
    ----------
    n : pypsa.Network
        Network to add brownfield to
    n_p : pypsa.Network
        Previous network to get brownfield from
    year : int
        Planning year
    capacity_threshold : float
        Threshold for removing assets with low capacity
    """
    logger.info(f"Preparing brownfield for the year {year}")

    n_p = remove_redispatch_generators(n_p)

    # electric transmission grid set optimised capacities of previous as minimum
    n.lines.s_nom_min = n_p.lines.s_nom_opt
    # Clamp s_nom_max to be at least s_nom_min to prevent solver infeasibility
    # from floating-point differences between s_nom_opt and s_nom_max
    n.lines.s_nom_max = n.lines.s_nom_max.clip(lower=n.lines.s_nom_min)
    dc_i = n.links[n.links.carrier == "DC"].index
    n.links.loc[dc_i, "p_nom_min"] = n_p.links.loc[dc_i, "p_nom_opt"]
    n.links.loc[dc_i, "p_nom_max"] = n.links.loc[dc_i, "p_nom_max"].clip(
        lower=n.links.loc[dc_i, "p_nom_min"]
    )

    for c in n_p.components[["Link", "Generator", "Store"]]:
        if c.static.empty:
            continue
        attr = "e" if c.name == "Store" else "p"

        # first, remove generators, links and stores that track
        # CO2 or global EU values since these are already in n
        n_p.remove(c.name, c.static.index[c.static.lifetime == np.inf])

        # remove assets whose build_year + lifetime <= year
        n_p.remove(
            c.name, c.static.index[c.static.build_year + c.static.lifetime <= year]
        )

        # copy over assets but fix their capacity
        c.static[f"{attr}_nom"] = c.static[f"{attr}_nom_opt"]
        c.static[f"{attr}_nom_extendable"] = False

        n.add(c.name, c.static.index, **c.static)

        # copy time-dependent
        selection = n.component_attrs[c.name].type.str.contains(
            "series"
        ) & n.component_attrs[c.name].status.str.contains("Input")
        for tattr in n.component_attrs[c.name].index[selection]:
            # TODO: Needs to be rewritten to
            n._import_series_from_df(c.dynamic[tattr], c.name, tattr)


def disable_grid_expansion_if_limit_hit(n):
    """
    Check if transmission expansion limit is already reached; then turn off.

    In particular, this function checks if the total transmission
    capital cost or volume implied by s_nom_min and p_nom_min are
    numerically close to the respective global limit set in
    n.global_constraints. If so, the nominal capacities are set to the
    minimum and extendable is turned off; the corresponding global
    constraint is then dropped.
    """
    types = {"expansion_cost": "capital_cost", "volume_expansion": "length"}
    for limit_type in types:
        glcs = n.global_constraints.query(f"type == 'transmission_{limit_type}_limit'")

        for name, glc in glcs.iterrows():
            total_expansion = (
                (
                    n.lines.query("s_nom_extendable")
                    .eval(f"s_nom_min * {types[limit_type]}")
                    .sum()
                )
                + (
                    n.links.query("carrier == 'DC' and p_nom_extendable")
                    .eval(f"p_nom_min * {types[limit_type]}")
                    .sum()
                )
            ).sum()

            # Allow small numerical differences
            if np.abs(glc.constant - total_expansion) / glc.constant < 1e-6:
                logger.info(
                    f"Transmission expansion {limit_type} is already reached, disabling expansion and limit"
                )
                extendable_acs = n.lines.query("s_nom_extendable").index
                n.lines.loc[extendable_acs, "s_nom_extendable"] = False
                n.lines.loc[extendable_acs, "s_nom"] = n.lines.loc[
                    extendable_acs, "s_nom_min"
                ]

                extendable_dcs = n.links.query(
                    "carrier == 'DC' and p_nom_extendable"
                ).index
                n.links.loc[extendable_dcs, "p_nom_extendable"] = False
                n.links.loc[extendable_dcs, "p_nom"] = n.links.loc[
                    extendable_dcs, "p_nom_min"
                ]

                n.global_constraints.drop(name, inplace=True)


def adjust_renewable_profiles(n, input_profiles, params, year):
    """
    Adjusts renewable profiles according to the renewable technology specified,
    using the latest year below or equal to the selected year.
    """

    # temporal clustering
    dr = get_snapshots(params["snapshots"], params["drop_leap_day"])
    snapshotmaps = (
        pd.Series(dr, index=dr).where(lambda x: x.isin(n.snapshots), pd.NA).ffill()
    )

    for carrier in params["carriers"]:
        if carrier == "hydro":
            continue

        with xr.open_dataset(getattr(input_profiles, "profile_" + carrier)) as ds:
            if ds.indexes["bus"].empty or "year" not in ds.indexes:
                continue

            ds = ds.stack(bus_bin=["bus", "bin"])

            closest_year = max(
                (y for y in ds.year.values if y <= year), default=min(ds.year.values)
            )

            p_max_pu = ds["profile"].sel(year=closest_year).to_pandas()
            p_max_pu.columns = p_max_pu.columns.map(flatten) + f" {carrier}"

            # temporal_clustering
            p_max_pu = p_max_pu.groupby(snapshotmaps).mean()

            # replace renewable time series
            n.generators_t.p_max_pu.loc[:, p_max_pu.columns] = p_max_pu


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake

        snakemake = mock_snakemake(
            "add_brownfield",
            clusters="39",
            opts="",
            sector_opts="",
            planning_horizons=2050,
        )

    configure_logging(snakemake)  # pylint: disable=E0606
    set_scenario_config(snakemake)

    update_config_from_wildcards(snakemake.config, snakemake.wildcards)

    logger.info(f"Preparing brownfield from the file {snakemake.input.network_p}")

    year = int(snakemake.wildcards.planning_horizons)

    n = pypsa.Network(snakemake.input.network)

    adjust_renewable_profiles(n, snakemake.input, snakemake.params, year)

    add_build_year_to_new_assets(n, year)

    n_p = pypsa.Network(snakemake.input.network_p)

    add_brownfield(
        n,
        n_p,
        year,
        capacity_threshold=snakemake.params.threshold_capacity,
    )

    disable_grid_expansion_if_limit_hit(n)

    n.meta = dict(snakemake.config, **dict(wildcards=dict(snakemake.wildcards)))

    sanitize_custom_columns(n)
    sanitize_carriers(n, snakemake.config)
    n.export_to_netcdf(snakemake.output[0])
