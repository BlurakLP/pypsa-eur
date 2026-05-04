#
# SPDX-License-Identifier: MIT


rule create_bz_bus_mapping:
    input:
        network=resources(
            "networks/base_s_{clusters}_{planning_horizons}_{multi_model}.nc"
        ),
        bz_shapes=f"data/busshapes/{bz_config}.csv",
    output:
        resources(f"bz_bus_mapping_{clusters}_{bz_config}.csv")
    log:
        logs("create_bz_bus_mapping")
    benchmark:
        benchmarks("create_bz_bus_mapping")
    wildcard_constraints:
        # TODO: The first planning_horizon needs to be aligned across scenarios
        # snakemake does not support passing functions to wildcard_constraints
        # reference: https://github.com/snakemake/snakemake/issues/2703
        planning_horizons=config["scenario"]["planning_horizons"][0],  #only applies to baseyear
    params:
        baseyear=config_provider("scenario", "planning_horizons", 0),
    message:
        "Creating mapping between buses and bidding-zones (according to custom bidding zone setup)."
    script:
        scripts("create_bz_bus_mapping.py")



rule create_custom_busmap:
    input:
        network=resources(
            "busmap_base_s_{cluster}.csv"
        ),
    output:
        custom_busmap = f"data/busmaps/{bz_config}.csv"
    log:
        logs("create_custom_busmap")
    benchmark:
        benchmarks("create_custom_busmap")
    params:
        baseyear=config_provider("scenario", "planning_horizons", 0),
    message:
        "Creating custom busmap where the buses outside DE are aggregated to one bus per country."
    script:
        scripts("create_custom_busmap.py")