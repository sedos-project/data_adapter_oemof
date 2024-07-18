import logging
import pathlib

import pandas as pd
from data_adapter.databus import download_collection  # noqa
from data_adapter.preprocessing import Adapter  # noqa: E402
from data_adapter.structure import Structure  # noqa: E402
from oemof.solph import Model, processing
from oemof.solph._energy_system import EnergySystem
from oemof.solph.buses import Bus
from oemof.tabular.datapackage import building  # noqa F401
from oemof.tabular.datapackage.reading import (
    deserialize_constraints,
    deserialize_energy_system,
)
from oemof.tabular.facades import Commodity, Conversion, Excess, Load, Volatile
from oemof_industry.mimo_converter import MIMO

from data_adapter_oemof.build_datapackage import DataPackage  # noqa: E402

logger = logging.getLogger()

EnergySystem.from_datapackage = classmethod(deserialize_energy_system)

Model.add_constraints_from_datapackage = deserialize_constraints
"""
Download Collection

Some datasets must be adjusted due to wrong formatting in comments
    - x2x_import_hydrogen_renewable
    - x2x_p2gas_aec_1
    - x2x_p2gas_pemec_1
    - x2x_x2gas_mpyr_1


Also adjust Modelstructure:
    Delete lines:
        - helper sinks in HelperO1
        - red marked lines in ProcessO1 (not yet uploaded or deleted data)
"""

# from data_adapter.databus import download_collection
# download_collection(
#          "https://databus.openenergyplatform.org/felixmaur/collections/steel_industry_test/"
#      )
logger.info("Reading Structure")
structure = Structure(
    "SEDOS_Modellstruktur",
    process_sheet="Processes_O1",
    parameter_sheet="Parameter_Input-Output",
    helper_sheet="Helper_O1",
)

adapter = Adapter(
    "steel_industry_test",
    structure=structure,
)

logger.info("Building Adapter Map")

# create dicitonary with all found in and outputs
process_adapter_map = pd.concat(
    [
        pd.read_excel(
            io=structure.structure_file,
            sheet_name="Processes_O1",
            usecols=("process", "facade adapter (oemof)"),
            index_col="process",
        ),
        pd.read_excel(
            io=structure.structure_file,
            sheet_name="Helper_O1",
            usecols=("process", "facade adapter (oemof)"),
            index_col="process",
        ),
    ]
).to_dict(orient="dict")["facade adapter (oemof)"]

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
logger.info("Building datapackage...")
dp = DataPackage.build_datapackage(
    adapter=adapter,
    process_adapter_map=process_adapter_map,
    parameter_map=parameter_map,
)
datapackage_path = pathlib.Path(__file__).parent / "datapackage"
dp.save_datapackage_to_csv(str(datapackage_path))


logger.info("Building EnergySystem")

es = EnergySystem.from_datapackage(
    path="datapackage/datapackage.json",
    typemap={
        "bus": Bus,
        "excess": Excess,
        "commodity": Commodity,
        "conversion": Conversion,
        "load": Load,
        "volatile": Volatile,
        "mimo": MIMO,
    },
)

logger.info("Building Model...")
m = Model(es)
logger.info("Solving Model...")
m.solve(solver="cbc")
logger.warning(m.solver_results["Solver"][0]["Termination condition"])
print(m.solver_results["Solver"][0]["Termination condition"])
logger.info("Reding Results")
results = processing.results(m)
logger.info("Writing Results and Goodbye :)")
