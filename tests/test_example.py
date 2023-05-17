
from data_adapter.preprocessing import Adapter

from data_adapter.databus import download_collection
from utils import PATH_TEST_FILES, PATH_TMP, check_if_csv_dirs_equal
from data_adapter_oemof.build_datapackage import DataPackage


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

    adapter = Adapter(
        "hack-a-thon",
        structure_name="structure",
        links_name="links",
    )
    dta = DataPackage.build_datapackage(adapter=adapter)

    check_if_csv_dirs_equal(PATH_TMP, path_default)
    # FIXME: Get them closer together
    #  - Bus naming with regions -> get regions funktion von Hendrik
    #  - multiple inputs/outputs
