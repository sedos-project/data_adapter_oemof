import os

os.makedirs("./collections", exist_ok=True)
os.makedirs("./structures", exist_ok=True)

os.environ["COLLECTIONS_DIR"] = "./collections/"
os.environ["STRUCTURES_DIR"] = "./structures"

from data_adapter.databus import download_collection  # noqa: E402
from data_adapter.preprocessing import Adapter  # noqa: E402
from data_adapter.structure import Structure  # noqa: E402

from data_adapter_oemof.build_datapackage import DataPackage  # noqa: E402


# download_collection(
#     "https://databus.openenergyplatform.org/nailend/collections/emob_test_collection_1"
# )

structure = Structure("sedos_johannes_emob", process_sheet="Processes_tra_road_car")
transport_adapter = Adapter(
    "transport_test_adapter",
    structure=structure,
)
process_adapter_map = {
    "tra_road_mcar_fcev_pass_0": "ConversionAdapter",
    "tra_road_mcar_fcev_pass_1": "ConversionAdapter",
    "tra_road_mcar_bev_pass_0": "BevAdapter",
    "tra_road_mcar_bev_pass_1": "BevAdapter",
    "tra_road_mcar_hyb_pass_gasoline_0": "ConversionAdapter",
    "tra_road_mcar_hyb_pass_gasoline_1": "ConversionAdapter",
    "tra_road_mcar_hyb_pass_diesel_0": "ConversionAdapter",
    "tra_road_mcar_hyb_pass_diesel_1": "ConversionAdapter",
    "tra_road_mcar_ice_pass_gasoline_0": "ConversionAdapter",
    "tra_road_mcar_ice_pass_gasoline_1": "ConversionAdapter",
    "tra_road_mcar_ice_pass_diesel_0": "ConversionAdapter",
    "tra_road_mcar_ice_pass_diesel_1": "ConversionAdapter"
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
    "BevAdapter": {
        # "tabular": "capacity_e_inst",  #todo: ?
        "charging_power_flex": "capacity_tra_connection_flex_max",
        "availability_flex": "capacity_tra_connection_flex_timeseries_upper",
        "charging_power_inflex": "capacity_tra_connection_inflex_max",
        "availability_inflex": "capacity_tra_connection_inflex_timeseries_fixed",
        # "tabular": "capacity_tra_vehicles_inst",  #todo: ?
        "fixed_costs": "cost_fix_tra",
        # "tabular": "cost_inv_tra",  #todo: ?
        # "tabular": "cost_var",  # part of DEFAULT
        # "tabular": "demand_annual",  #todo: ?
        # "tabular": "demand_timeseries_fixed",  #todo: ?
        "efficiency_sto_in": "efficiency_sto_in",
        "efficiency_sto_out": "efficiency_sto_out",
        "efficiency_mob_electrical": "efficiency_tra_electrical",
        "efficiency_mob_g2v": "efficiency_tra_g2v",
        "efficiency_mob_v2g": "efficiency_tra_v2g",
        "lifetime": "lifetime",
        # "tabular": "mileage",  #todo: ?
        # "tabular": "occupancy_rate",  #todo: ?
        # "tabular": "share_tra_flex_g2v",  #todo: ?
        # "tabular": "share_tra_flex_v2g",  #todo: ?
        # "tabular": "share_tra_inflex",  #todo: ?
        # "tabular": "sto_init",  #todo: ?
        "max_storage_level": "sto_max_timeseries",
        "min_storage_level": "sto_min_timeseries",
        "loss_rate": "sto_self_discharge",
        # "tabular": "wacc",  #todo: ?
    },
}

dp = DataPackage.build_datapackage(
    adapter=adapter,
    process_adapter_map=process_adapter_map,
    parameter_map=parameter_map,
)
dp.save_datapackage_to_csv("./datapackage")
