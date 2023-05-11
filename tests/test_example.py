import setup_environment

import pandas as pd

setup_environment.setup()
from data_adapter.preprocessing import Adapter
from data_adapter.structure import get_energy_structure

from data_adapter.databus import download_collection
from data_adapter import preprocessing
from utils import PATH_TEST_FILES, PATH_TMP, check_if_csv_dirs_equal
from data_adapter_oemof.build_datapackage import datapackage


path_default = (
    PATH_TEST_FILES
    / "_files"
    / "tabular_datapackage_mininmal_example"
    / "data"
    / "elements"
)


def test_build_tabular_datapackage():
    download_collection(
        "https://energy.databus.dbpedia.org/felixmaur/collections/minimal_example/"
    )

    adapter =  Adapter("minimal_example", structure_name="minimal_structure", links_name="minimal_links")
    process_data = {
        process: adapter.get_process(process)
        for process in adapter.get_structure().keys()

    }

    dta = datapackage.build_datapackage(es_structure, **process_data)

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
