import os

os.environ["COLLECTIONS_DIR"] = "./collections/"
os.environ["STRUCTURES_DIR"] = "./structures"

from data_adapter.preprocessing import Adapter  # noqa: E402
from data_adapter.structure import Structure  # noqa: E402

from data_adapter_oemof.build_datapackage import DataPackage  # noqa: E402

structure = Structure(
    "SEDOS_Modellstruktur", process_sheet="Processes_steel_industry_2"
)
adapter = Adapter(
    "steel_industry_test_adapted",
    structure=structure,
)
process_adapter_map = {
    "modex_tech_storage_battery": "StorageAdapter",
    "modex_tech_generator_gas": "ConversionAdapter",
    "modex_tech_wind_turbine_onshore": "VolatileAdapter",
    "modex_demand": "LoadAdapter",
    "pow_combustion_gt_natgas": "ConversionAdapter",
    "x2x_import_natural_gas": "CommodityAdapter",
    "x2x_import_crudesteel": "CommodityAdapter",
    "x2x_import_coke_oven_gas": "CommodityAdapter",
    "x2x_import_h2": "CommodityAdapter",
    "ind_steel_casting_1": "MIMOAdapter",
    "ind_steel_demand": "LoadAdapter",
}

parameter_map = {
    "DEFAULT": {
        "marginal_cost": "cost_var",
        "fixed_cost": "fixed_costs",
        "capacity_cost": "capital_costs",
    },
    "ExtractionTurbineAdapter": {
        "carrier_cost": "fuel_costs",
        "capacity": "installed_capacity",
    },
    "StorageAdapter": {
        "capacity_potential": "expansion_limit",
        "capacity": "installed_capacity",
        "invest_relation_output_capacity": "e2p_ratio",
        "inflow_conversion_factor": "input_ratio",
        "outflow_conversion_factor": "output_ratio",
    },
    "modex_tech_wind_turbine_onshore": {"profile": "onshore"},
}

dp = DataPackage.build_datapackage(
    adapter=adapter,
    process_adapter_map=process_adapter_map,
    parameter_map=parameter_map,
)
dp.save_datapackage_to_csv("./datapackage")
