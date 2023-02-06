from data_adapter.preprocessing import get_process_df
from data_adapter.structure import get_energy_structure
from oemof.solph.helpers import extend_basic_path

from data_adapter_oemof.build_datapackage import (
    build_datapackage,
    save_datapackage_to_csv,
)

tmp_path = extend_basic_path("data_adatper_oemof")


def test_build_tabular_datapackage():

    es_structure = get_energy_structure()

    processes = es_structure.keys()

    # This is a placeholder as long as get_energy_structure does not provide
    # all necessary information.
    processes = ["offshore wind farm"]

    process_data = {
        process: get_process_df("modex_test_renewable", process)
        for process in processes
    }

    datapackage = build_datapackage(**process_data)

    save_datapackage_to_csv(datapackage, tmp_path)
