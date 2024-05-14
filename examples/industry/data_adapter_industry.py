import os
import pathlib

os.environ["COLLECTIONS_DIR"] = "./collections/"
os.environ["STRUCTURES_DIR"] = ""

from data_adapter.databus import download_collection  # noqa
from data_adapter.preprocessing import Adapter  # noqa: E402
from data_adapter.structure import Structure  # noqa: E402
from data_adapter_oemof.build_datapackage import DataPackage  # noqa: E402

from oemof.solph._energy_system import EnergySystem
from oemof.solph import Model

from oemof.tabular.datapackage import building  # noqa F401
from oemof.tabular.datapackage.reading import deserialize_constraints, deserialize_energy_system

from oemof.tabular.facades import Excess, Commodity, Conversion, Load, Volatile
from oemof_industry.mimo_converter import MIMO
from oemof.solph.buses import Bus

EnergySystem.from_datapackage = classmethod(deserialize_energy_system)

Model.add_constraints_from_datapackage = deserialize_constraints
#
# Download Collection
# Due to Nan values in "ind_scalar" type column datapackage.json must be adjusted after download

from data_adapter.databus import download_collection
download_collection(
         "https://databus.openenergyplatform.org/felixmaur/collections/steel_industry_test/"
     )
structure = Structure(
    "Industriestruktur",
    process_sheet="process_set_steel_casting",
    parameter_sheet="parameter_input_output_steel_ca",
    helper_sheet="steel_casting_helper",
)

adapter = Adapter(
    "steel_industry_test",
    structure=structure,
)
process_adapter_map = {
    "modex_tech_storage_battery": "StorageAdapter",
    "modex_tech_generator_gas": "ConversionAdapter",
    "modex_tech_wind_turbine_onshore": "VolatileAdapter",
    "modex_demand": "LoadAdapter",
    "ind_scalars": "LoadAdapter",
    "pow_combustion_gt_natgas": "ConversionAdapter",
    "x2x_import_natural_gas": "CommodityAdapter",
    "x2x_import_crudesteel": "CommodityAdapter",
    "x2x_import_coke_oven_gas": "CommodityAdapter",
    "x2x_import_h2": "CommodityAdapter",
    "ind_steel_casting_1": "MIMOAdapter",
    "ind_steel_casting_0": "MIMOAdapter",
    "ind_steel_hyddri_1": "MIMOAdapter",
    "ind_steel_demand": "LoadAdapter",
    "x2x_import_elec": "CommodityAdapter",
    "ind_steel_boiler_0": "ConversionAdapter",
    "excess_co2": "ExcessAdapter",
    "excess_ch4": "ExcessAdapter",
    "excess_n2o": "ExcessAdapter",
    "helper_sink_exo_steel": "LoadAdapter",
    "ind_timeseries": "LoadAdapter",
    "helper_sink_exo_steel": "LoadAdapter",

}

parameter_map = {
    "DEFAULT": {},
    "StorageAdapter": {
        "capacity_potential": "expansion_limit",
        "capacity": "installed_capacity",
        "invest_relation_output_capacity": "e2p_ratio",
        "inflow_conversion_factor": "input_ratio",
        "outflow_conversion_factor": "output_ratio",
    },
    "MIMOAdapter": {
        "capacity_cost": "cost_fix_capacity_w",
        "capacity": "capacity_w_resid",
        "expandable": "capacity_w_abs_new_max",
    },
    "modex_tech_wind_turbine_onshore": {"profile": "onshore"},
}

dp = DataPackage.build_datapackage(
    adapter=adapter,
    process_adapter_map=process_adapter_map,
    parameter_map=parameter_map,
)
datapackage_path = pathlib.Path(__file__).parent / "datapackage"
dp.save_datapackage_to_csv(str(datapackage_path))


es = EnergySystem.from_datapackage(path = "datapackage/datapackage.json",
                                   typemap= {"bus": Bus,
                                       "excess":Excess,
                                             "commodity": Commodity,
                                             "conversion": Conversion,
                                             "load": Load,
                                             "volatile": Volatile,
                                             "mimo": MIMO},
                                   )

m = Model(es)
m.solve()