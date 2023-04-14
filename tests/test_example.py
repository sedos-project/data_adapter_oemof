import setup_environment

import pandas as pd

setup_environment.setup()
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
    download_collection(
        "https://energy.databus.dbpedia.org/felixmaur/collections/hack-a-thon/"
    )

    es_structure = get_energy_structure(structure="minimal_structure")
    process_data = {
        process: get_process("minimal_example", process, links="minimal_links")
        for process in es_structure.keys()
    }

    datapackage = build_datapackage(es_structure, **process_data)

    save_datapackage_to_csv(datapackage, PATH_TMP)

    check_if_csv_dirs_equal(PATH_TMP, path_default)
    # FIXME: Get them closer together
    #  - Bus naming with regions -> get regions funktion von Hendrik
    #  - multiple inputs/outputs


def test_test():
    df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df2 = pd.DataFrame({"b": [3.0, 4.0], "a": [1, 2]})
    pd.testing.assert_frame_equal(
        df1,
        df2,
        check_less_precise=True,
        check_column_type=False,
        check_dtype=False,
        check_like=True,
    )
