import json
import os

os.environ[
    "COLLECTION_DIR"
] = "/home/local/RL-INSTITUT/felix.maurer/rli/Felix.Maurer/SEDOS/Python/data_adapter_oemof/tests/collections"
os.environ[
    "STRUCTURES_DIR"
] = "/home/local/RL-INSTITUT/felix.maurer/rli/Felix.Maurer/SEDOS/Python/data_adapter_oemof/tests/structure/"
from data_adapter.preprocessing import get_process
from data_adapter.structure import get_energy_structure
from data_adapter.databus import download_collection

from utils import PATH_TEST_FILES, PATH_TMP, check_if_csv_dirs_equal
from data_adapter_oemof.build_datapackage import (
    build_datapackage,
    save_datapackage_to_csv,
)


path_default = (
    PATH_TEST_FILES
    / "_files"
    / "tabular_datapackage_mininmal_example"
    / "data"
    / "elements"
)


def test_build_tabular_datapackage():

    # download Collection:
    # uncomment as soon as pin is removed
    #download_collection(
    #    "https://energy.databus.dbpedia.org/felixmaur/collections/minimal_example/"
    #)

    es_structure = get_energy_structure(structure="minimal_structure")
    print(es_structure)
    processes = es_structure.keys()
    # This is a placeholder as long as get_energy_structure does not provide
    # all necessary information.
    print(processes)
    process_data = {
        process: get_process("minimal_example", process, links="minimal_links") for process in processes
    }

    datapackage = build_datapackage(**process_data)

    save_datapackage_to_csv(datapackage, PATH_TMP)

    check_if_csv_dirs_equal(PATH_TMP, path_default)
