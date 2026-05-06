
wildcard_constraints:
    bz_config="\d+"

ruleorder: add_existing_baseyear > add_brownfield

rule create_custom_busmap:
    input:
        busmap=resources("busmap_base_s_{clusters}.csv")
    output:
        resources("base_s_{clusters}_{base_network}.csv")
    log:
        logs("create_custom_busmap_{clusters}_{base_network}")
    benchmark:
        benchmarks("create_custom_busmap_{clusters}_{base_network}")
    message:
        "Creating custom busmap where the buses outside DE are aggregated to one bus per country."
    script:
        scripts("create_custom_busmap.py")

rule create_bz_bus_mapping:
    input:
        RM_network=resources("networks/base_s_{clusters}_{opts}_{planning_horizons}_RM.nc"),
        bz_shapes=f"data/busshapes/{bz_config}_shapes.geojson",
    output:
        resources(f"bz_bus_mapping_{clusters}_{bz_config}.csv")
    log:
        logs("create_bz_bus_mapping_{clusters}_{bz_config}")
    benchmark:
        benchmarks("create_bz_bus_mapping_{clusters}_{bz_config}")
    wildcard_constraints:
        # TODO: The first planning_horizon needs to be aligned across scenarios
        # snakemake does not support passing functions to wildcard_constraints
        # reference: https://github.com/snakemake/snakemake/issues/2703
        planning_horizons=config["scenario"]["planning_horizons"][0],  #only applies to baseyear
    params:
        bz_config=config_provider("scenario", "bz_config"),
    message:
        "Creating mapping between buses and bidding-zones (according to custom bidding zone setup)."
    script:
        scripts("create_bz_bus_mapping.py")

rule create_RM_and_MM:
    input:
        network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_brownfield.nc"),
        mapping=resources(f"bz_bus_mapping_{clusters}_{bz_config}.csv"),
        bz_shapes=f"data/busshapes/{bz_config}_shapes.geojson",
    output:
        RM_network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_RM_pre-transfer.nc")
        MM_network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_MM.nc")
    log:
        logs("create_RM_and_MM_{planning_horizons}")
    benchmark:
        benchmarks("create_RM_and_MM_{planning_horizons}")
    params:
        bz_config=config_provider("scenario", "bz_config"),
    message:
        "Aggregating the network of the redispatch model to the network of the market model"
    script:
        scripts("aggregate_RM_to_MM.py")

rule transfer_dispatch:
    input:
        RM_network=RESULTS + "networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_RM_pre-transfer.nc",
        MM_network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_MM.nc")
    output:
        MM_network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_MM.nc")
    log:
        logs("transfer_dispatch_{planning_horizons}")
    benchmark:
        benchmarks("transfer_dispatch_{planning_horizons}")
    params:
        bz_config=config_provider("scenario", "bz_config"),
    message:
        "Aggregating the network of the redispatch model to the network of the market model"
    script:
        scripts("transfer_dispatch.py")

rule solve_network:
    input:
        network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_{model_type}.nc"),
    output:
        network=RESULTS + "networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_{model_type}.nc",
        config=RESULTS + "configs/config.base_s_{clusters}_elec_{opts}_{planning_horizons}_{model_type}.yaml",
        model=(
            RESULTS + "models/base_s_{clusters}_elec_{opts}_{planning_horizons}_{model_type}.nc"
            if config["solving"]["options"]["store_model"]
            else []
        ),
    log:
        solver=normpath(RESULTS + "logs/solve_network/base_s_{clusters}_elec_{opts}_{planning_horizons}_{model_type}_solver.log"),
        memory=RESULTS + "logs/solve_network/base_s_{clusters}_elec_{opts}_{planning_horizons}_{model_type}_memory.log",
        python=RESULTS + "logs/solve_network/base_s_{clusters}_elec_{opts}_{planning_horizons}_{model_type}_python.log",
    benchmark:
        (RESULTS + "benchmarks/solve_network/base_s_{clusters}_elec_{opts}_{planning_horizons}_{model_type}")
    shadow:
        shadow_config
    threads: solver_threads
    resources:
        mem_mb=memory,
        runtime=config_provider("solving", "runtime", default="6h"),
    params:
        solving=config_provider("solving"),
        foresight=config_provider("foresight"),
        co2_sequestration_potential=config_provider("sector", "co2_sequestration_potential", default=200),
        custom_extra_functionality=input_custom_extra_functionality,
    message:
        "Solving electricity network optimization for {wildcards.clusters} clusters and {wildcards.opts} electric options"
    script:
        scripts("solve_network.py")

rule add_existing_baseyear:
    input:
        network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}.nc"),
        powerplants=resources("powerplants_s_{clusters}.csv"),
        costs=lambda w: resources(f"costs_{config_provider('scenario', 'planning_horizons',0)(w)}_processed.csv")
    output:
        resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_brownfield.nc")
    log:
        logs("add_existing_baseyear_base_s_{clusters}_{opts}_{planning_horizons}.log")
    benchmark:
        benchmarks("add_existing_baseyear/base_s_{clusters}_{opts}_{planning_horizons}")
    wildcard_constraints:
        # TODO: The first planning_horizon needs to be aligned across scenarios
        # snakemake does not support passing functions to wildcard_constraints
        # reference: https://github.com/snakemake/snakemake/issues/2703
        planning_horizons=config["scenario"]["planning_horizons"][0],  #only applies to baseyear
    threads: 1
    resources:
        mem_mb=3000,
    params:
        baseyear=config_provider("scenario", "planning_horizons", 0),
        existing_capacities=config_provider("existing_capacities"),
        carriers=config_provider("electricity", "renewable_carriers"),
        costs=config_provider("costs"),
        energy_totals_year=config_provider("energy", "energy_totals_year"),
    message:
        "Adding existing infrastructure for base year for {wildcards.clusters} clusters, {wildcards.planning_horizons} planning horizons, {wildcards.opts} electric options"
    script:
        scripts("add_existing_baseyear.py")

def input_profile_tech_brownfield(w):
    return {
        f"profile_{tech}": resources("profile_{clusters}_" + tech + ".nc")
        for tech in config_provider("electricity", "renewable_carriers")(w)
        if tech != "hydro"
    }

rule add_brownfield:
    input:
        unpack(input_profile_tech_brownfield),
        network=resources("networks/base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}.nc"),
        network_p=solved_previous_horizon,  #solved network at previous time step
    output:
        resources("networks/base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}_brownfield.nc")
    log:
        logs("add_brownfield_base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}.log")
    benchmark:
        benchmarks("add_brownfield/base_s_{clusters}_{opts}_{sector_opts}_{planning_horizons}")
    params:
        threshold_capacity=config_provider("existing_capacities", "threshold_capacity"),
        snapshots=config_provider("snapshots"),
        drop_leap_day=config_provider("enable", "drop_leap_day"),
        carriers=config_provider("electricity", "renewable_carriers"),
    message:
        "Adding brownfield constraints for existing infrastructure for {wildcards.clusters} clusters, {wildcards.planning_horizons} planning horizons, {wildcards.opts} electric options and {wildcards.sector_opts} sector options"
    script:
        scripts("add_brownfield.py")

rule solve_multi-model_networks:
    input:
        expand(
            RESULTS
            + "networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_RM.nc",
            **config["scenario"],
            run=config["run"]["name"],
        ),
    message:
        "Collecting solved multi-model network files"