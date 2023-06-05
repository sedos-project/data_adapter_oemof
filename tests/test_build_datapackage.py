import pandas

from data_adapter.preprocessing import Adapter

from data_adapter.databus import download_collection
from utils import PATH_TEST_FILES, PATH_TMP, check_if_csv_dirs_equal
from data_adapter_oemof.build_datapackage import DataPackage, refactor_timeseries


path_default = (
    PATH_TEST_FILES
    / "_files"
    / "tabular_datapackage_mininmal_example"
    / "data"
    / "elements"
)


def test_refactor_timeseries():
    timeseries = pandas.DataFrame(
        {
            "timeindex_start": ["01:00:00", "01:00:00", "09:00:00"],
            "timeindex_stop": ["03:00:00", "03:00:00", "10:00:00"],
            "timeindex_resolution": ["P0DT01H00M00S", "P0DT01H00M00S", "P0DT01H00M00S"],
            "region": ["BB", "HH", "HH"],
            "onshore": [[1, 2, 3], [4, 5, 6], [33, 34]],
            "offshore": [[7, 8, 9], [10, 11, 12], [35, 36]],
        }
    )

    refactored_ts = refactor_timeseries(timeseries)
    expected_df = pandas.DataFrame(
        {
            "offshore_BB": [7.0, 8, 9, None, None],
            "offshore_HH": [10.0, 11, 12, 35, 36],
            "onshore_BB": [1.0, 2, 3, None, None],
            "onshore_HH": [4.0, 5, 6, 33, 34]
        },
        index=pandas.DatetimeIndex(
            ["01:00:00", "02:00:00", "03:00:00", "09:00:00", "10:00:00"]
        ),
    )
    pandas.testing.assert_frame_equal(expected_df, refactored_ts)


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
