#
# SPDX-License-Identifier: MIT

wildcard_constraints:
    bz_config="\d+"


rule create_bz_bus_mapping:
    input:
        RM_networknetwork=resources("networks/base_s_{clusters}_RM.nc"),
        bz_shapes=f"data/busshapes/{bz_config}_shapes.geojson",
    output:
        mapping = resources(f"bz_bus_mapping_{clusters}_{bz_config}.csv")
    log:
        logs("create_bz_bus_mapping")
    benchmark:
        benchmarks("create_bz_bus_mapping")
    params:
        bz_config=config_provider("scenario", "bz_config"),
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
        custom_busmap = f"busmap_base_s_{cluster}.csv"
    log:
        logs("create_custom_busmap")
    benchmark:
        benchmarks("create_custom_busmap")
    params:
        cluster=config_provider("scenario", "cluster"),
    message:
        "Creating custom busmap where the buses outside DE are aggregated to one bus per country."
    script:
        scripts("create_custom_busmap.py")

rule aggregate_RM_to_MM:
    input:
        RM_network=resources("networks/base_s_{clusters}_RM.nc"),
        mapping=resources(f"bz_bus_mapping_{clusters}_{bz_config}.csv"),
        bz_shapes=f"data/busshapes/{bz_config}_shapes.geojson",
    output:
        MM_network=resources("networks/base_s_{clusters}_RM.nc")
    log:
        logs("aggregate_RM_to_MM")
    benchmark:
        benchmarks("aggregate_RM_to_MM")
    params:
        bz_config=config_provider("scenario", "bz_config"),
    message:
        "Aggregating the network of the redispatch model to the network of the market model"
    script:
        scripts("aggregate_RM_to_MM.py")