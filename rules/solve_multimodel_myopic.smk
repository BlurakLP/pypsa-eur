BZ_CONFIG = config["scenario"]["bz_config"][0]
ruleorder: add_existing_baseyear_multimodel > add_brownfield_multimodel
if not config_provider("scenario", "bz_config"):
    ruleorder: get_custom_bidding_zones > build_bidding_zones

rule solve_multimodel_networks:
    input:
        expand(
            RESULTS
            + "networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_RM.nc",
            **config["scenario"],
            run=config["run"]["name"],
        ),
        expand(
            RESULTS
            + "networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_MM.nc",
            **config["scenario"],
            run=config["run"]["name"],
        ),
    message:
        "Collecting solved multi-model network files"


def input_profile_tech_brownfield(w):
    return {
        f"profile_{tech}": resources("profile_{clusters}_" + tech + ".nc")
        for tech in config_provider("electricity", "renewable_carriers")(w)
        if tech != "hydro"
    }


def solved_previous_horizon_multimodel(w):
    planning_horizons = config_provider("scenario", "planning_horizons")(w)
    i = planning_horizons.index(int(w.planning_horizons))

    planning_horizon_p = str(planning_horizons[i - 1])
    model_out = "RM"

    return (
        RESULTS
        + "networks/base_s_{clusters}_elec_{opts}_"
        + planning_horizon_p
        + "_"
        + model_out
        + ".nc"
    )


rule get_custom_bidding_zones:
    input:
        bidding_zones=f"data/bidding_zones_custom/{BZ_CONFIG}.geojson"
    output:
        file=resources("bidding_zones.geojson"),
    log:
        logs(f"get_custom_bidding_zones{BZ_CONFIG}.log"),
    message:
        "Getting the custom bidding zones from data/bidding_zones_custom"
    shell:
        "cp {input.bidding_zones} {output.file} > {log} 2>&1"

  

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
        RM_network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_brownfield.nc"),
        bz_shapes=resources("bidding_zones.geojson"),
    output:
        mapping=resources("{BZ_CONFIG}_bz_bus_mapping_{clusters}_{opts}_{planning_horizons}.csv"),
    log:
        logs("create_bz_bus_mapping_{BZ_CONFIG}_{clusters}_{opts}_{planning_horizons}"),
    benchmark:
        benchmarks("create_bz_bus_mapping_{BZ_CONFIG}_{clusters}_{opts}_{planning_horizons}")
    wildcard_constraints:
        # TODO: The first planning_horizon needs to be aligned across scenarios
        # snakemake does not support passing functions to wildcard_constraints
        # reference: https://github.com/snakemake/snakemake/issues/2703
        # planning_horizons=config["scenario"]["planning_horizons"][0],  #only applies to baseyear
    message:
        "Creating mapping between buses and bidding-zones (according to custom bidding zone setup)."
    script:
        scripts("create_bz_bus_mapping.py")



rule add_existing_baseyear_multimodel:
    input:
        network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}.nc"),
        powerplants=resources("powerplants_s_{clusters}.csv"),
        costs=lambda w: resources(f"costs_{config_provider('scenario', 'planning_horizons',0)(w)}_processed.csv"),
    output:
        resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_brownfield.nc"),
    log:
        logs("add_existing_baseyear_base_s_{clusters}_{opts}_{planning_horizons}.log"),
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
        sector=config_provider("sector"),
        existing_capacities=config_provider("existing_capacities"),
        carriers=config_provider("electricity", "renewable_carriers"),
        costs=config_provider("costs"),
        energy_totals_year=config_provider("energy", "energy_totals_year"),
    message:
        "Adding existing infrastructure for base year for {wildcards.clusters} clusters, {wildcards.planning_horizons} planning horizons, {wildcards.opts} electric options"
    script:
        scripts("add_existing_baseyear_multimodel.py")



rule add_brownfield_multimodel:
    input:
        unpack(input_profile_tech_brownfield),
        network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}.nc"),
        network_p=solved_previous_horizon_multimodel,  #solved RM/MM network at previous time step
    output:
        resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_brownfield.nc"),
    log:
        logs("add_brownfield_base_s_{clusters}_{opts}_{planning_horizons}.log"),
    benchmark:
        benchmarks("add_brownfield/base_s_{clusters}_{opts}_{planning_horizons}")
    params:
        threshold_capacity=config_provider("existing_capacities", "threshold_capacity"),
        snapshots=config_provider("snapshots"),
        drop_leap_day=config_provider("enable", "drop_leap_day"),
        carriers=config_provider("electricity", "renewable_carriers"),
    message:
        "Adding brownfield constraints for existing infrastructure for {wildcards.clusters} clusters, {wildcards.planning_horizons} planning horizons, {wildcards.opts} electric options"
    script:
        scripts("add_brownfield_multimodel.py")



rule build_market_model:
    input:
        network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_brownfield.nc"),
        mapping=resources(f"{BZ_CONFIG}_bz_bus_mapping_{{clusters}}_{{opts}}_{{planning_horizons}}.csv"),
        bz_shapes=resources("bidding_zones.geojson"),
    output:
        MM_network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_MM_unsolved.nc"),
    log:
        logs("build_market_model_{clusters}_{opts}_{planning_horizons}"),
    benchmark:
        benchmarks("build_market_model_{clusters}_{opts}_{planning_horizons}")
    params:
        bz_config=config_provider("scenario", "bz_config"),
    message:
        "Aggregating the network to the network of the market model"
    script:
        scripts("build_market_model.py")



rule solve_network_multimodel:
    input:
        network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_{model_type}_unsolved.nc"),
    output:
        network=RESULTS + "networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_{model_type}.nc",
        config=RESULTS + "configs/config.base_s_{clusters}_elec_{opts}_{planning_horizons}_{model_type}.yaml",
        model=(RESULTS + "models/base_s_{clusters}_elec_{opts}_{planning_horizons}_{model_type}.nc"),
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
        co2_sequestration_potential=config_provider(
            "sector", "co2_sequestration_potential", default=200
        ),
        custom_extra_functionality=input_custom_extra_functionality,
    message:
        "Solving electricity network optimization for {wildcards.clusters} clusters and {wildcards.opts} electric options"
    script:
        scripts("solve_network.py")



rule build_redispatch_model:
    input:
        MM_network=(RESULTS + "networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_MM.nc"),
        RM_network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_brownfield.nc"),
    output:
        network=resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}_RM_unsolved.nc"),
    log:
        logs("build_redispatch_model_{clusters}_{opts}_{planning_horizons}"),
    benchmark:
        benchmarks("build_redispatch_model_{clusters}_{opts}_{planning_horizons}")
    message:
        "Transfering the capacity and dispatch from the solved market model to the unsolved redispatch model. Also setting the extendability correctly for RM."
    script:
        scripts("build_redispatch_model.py")



rule prepare_network_multimodel:
    input:
        resources("networks/base_s_{clusters}_elec.nc"),
        costs=lambda w: (resources("costs_{planning_horizons}_processed.csv")),
        co2_price=lambda w: (
            resources("co2_price.csv")
            if config_provider("costs", "emission_prices", "dynamic")(w)
            else []
        ),
    output:
        resources("networks/base_s_{clusters}_elec_{opts}_{planning_horizons}.nc"),
    log:
        logs("prepare_network_base_s_{clusters}_elec_{opts}_{planning_horizons}.log"),
    benchmark:
        benchmarks("prepare_network_base_s_{clusters}_elec_{opts}_{planning_horizons}")
    threads: 1
    resources:
        mem_mb=4000,
    params:
        time_resolution=config_provider("clustering", "temporal", "resolution_elec"),
        links=config_provider("links"),
        lines=config_provider("lines"),
        co2base=config_provider("electricity", "co2base"),
        co2limit_enable=config_provider("electricity", "co2limit_enable", default=False),
        co2limit=config_provider("electricity", "co2limit"),
        gaslimit_enable=config_provider("electricity", "gaslimit_enable", default=False),
        gaslimit=config_provider("electricity", "gaslimit"),
        emission_prices=config_provider("costs", "emission_prices"),
        adjustments=config_provider("adjustments", "electricity"),
        autarky=config_provider("electricity", "autarky", default={}),
        drop_leap_day=config_provider("enable", "drop_leap_day"),
        transmission_limit=config_provider("electricity", "transmission_limit"),
    message:
        "Preparing network for model with {wildcards.clusters} clusters and options {wildcards.opts}"
    script:
        scripts("prepare_network.py")


