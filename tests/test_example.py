from data_adapter.structure import get_energy_structure
from data_adapter.preprocessing import get_process_df


from data_adapter_oemof.adapters import TYPE_MAP
from data_adapter_oemof.mappings import PROCESS_TYPE_MAP


def test_build_tabular_datapackage():

    es_structure = get_energy_structure()

    processes = es_structure.keys()

    # This is a placeholder as long as get_energy_structure does not provide
    # all necessary information.
    processes = ["offshore wind farm"]

    for process in processes:
        data = get_process_df("modex_test_renewable", process)

        process_type = PROCESS_TYPE_MAP[process]

        adapter = TYPE_MAP[process_type]

        p = adapter.parametrize_dataclass(data)
