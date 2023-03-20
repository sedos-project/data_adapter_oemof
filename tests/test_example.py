import setup_environment

setup_environment.setup()
from data_adapter.preprocessing import get_process
from data_adapter.structure import get_energy_structure

# from data_adapter.databus import download_collection

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
    # download_collection(
    #    "https://energy.databus.dbpedia.org/felixmaur/collections/minimal_example/"
    # )

    es_structure = get_energy_structure(structure="minimal_structure")
    process_data = {
        process: get_process("minimal_example", process, links="minimal_links")
        for process in es_structure.keys()
    }

    datapackage = build_datapackage(es_structure, **process_data)

    save_datapackage_to_csv(datapackage, PATH_TMP)

    check_if_csv_dirs_equal(PATH_TMP, path_default)
    # FIXME: Get them closer together
    #  - Add names
    #  - Add type
    #  - Drop Nan-columns
# Todo: Write Modelbuilder test (I think solver test will not be necessary for now?)
