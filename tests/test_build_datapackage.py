import os

import pandas
import unittest

import pandas as pd
from data_adapter.preprocessing import Adapter
from unittest import mock

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
            "onshore_HH": [4.0, 5, 6, 33, 34],
        },
        index=pandas.DatetimeIndex(
            ["01:00:00", "02:00:00", "03:00:00", "09:00:00", "10:00:00"]
        ),
    )
    expected_df = expected_df.sort_index(axis=1)
    refactored_ts = refactored_ts.sort_index(axis=1)
    pandas.testing.assert_frame_equal(expected_df, refactored_ts)


def test_scalar_building():
    """
    Test to eval if process is read in correctly and scalar data is created accordingly
    TODO:
        - Write Test data-set
        - Write result data

    :return:
    """


def test_build_datapackage():
    """
    Test build Datapackage with mocking Adapter
    Returns
    -------
    """

    # Create a mock adapter object for testing
    mock_adapter = mock.Mock(spec=Adapter)
    # Mock the required methods and attributes of the Adapter
    mock_adapter.get_structure.return_value = {
        "modex_tech_storage_battery": {
            "default": {"inputs": ["electricity"], "outputs": []}
        },
        "modex_tech_generator_gas": {
            "emission_factor": {"inputs": ["ch4"], "outputs": ["co2"]},
            "default": {"inputs": ["ch4"], "outputs": ["electricity", "heat"]},
        },
        "modex_tech_wind_turbine_onshore": {
            "default": {"inputs": ["onshore"], "outputs": ["electricity"]}
        },
    }

    # Define different return values for get_process based on the structure
    def mock_get_process(process_name):
        """
        Adding side effects and .scalar and .timeseries
        Parameters
        ----------
        process_name

        Returns
        -------
        mocked function .get_process for Adapter

        """
        if process_name == "modex_tech_storage_battery":
            process_mock = mock.Mock()
            process_mock.scalars = pd.DataFrame(
                {
                    "region": {0: "BB", 1: "BB", 2: "BB"},
                    "year": {0: 2016, 1: 2030, 2: 2050},
                    "installed_capacity": {0: 17.8, 1: 17.8, 2: 17.8},
                    "e2p_ratio": {0: 3.3, 1: 3.3, 2: 3.3},
                }
            )
            process_mock.timeseries = pd.DataFrame()
            return process_mock
        elif process_name == "modex_tech_generator_gas":
            process_mock = mock.Mock()
            process_mock.scalars = pd.DataFrame(
                {
                    "region": {0: "BB", 1: "BB", 2: "BB"},
                    "year": {0: 2016, 1: 2030, 2: 2050},
                    "installed_capacity": {
                        0: 330.2,
                        1: 330.2014,
                        2: 330.2014,
                    },
                    "emission_factor": {0: 0.2, 1: 0.2, 2: 0.2},
                    "fuel_costs": {0: 25.9, 1: 25.9, 2: 49.36},
                }
            )
            process_mock.timeseries = pd.DataFrame()
            return process_mock
        elif process_name == "modex_tech_wind_turbine_onshore":
            process_mock = mock.Mock()
            process_mock.scalars = pd.DataFrame(
                {
                    "region": {0: "BB", 1: "BB", 2: "BB"},
                    "year": {0: 2016, 1: 2030, 2: 2050},
                    "installed_capacity": {
                        0: 5700.03,
                        1: 5700.03375,
                        2: 5700.03375,
                    },
                    "fixed_costs": {0: 23280.0, 1: 12600.0, 2: 11340.0},
                    "lifetime": {0: 25.4, 1: 30.0, 2: 30.0},
                    "wacc": {0: 0.07, 1: 0.07, 2: 0.07},
                }
            )
            process_mock.timeseries = pd.DataFrame(
                {
                    "region": {0: "BB", 1: "BB", 2: "BB"},
                    "timeindex_start": {
                        0: "2016-01-01T00:00:00",
                        1: "2030-01-01T00:00:00",
                        2: "2050-01-01T00:00:00",
                    },
                    "timeindex_stop": {
                        0: "2016-01-01T02:00:00",
                        1: "2030-01-01T02:00:00",
                        2: "2050-01-01T02:00:00",
                    },
                    "timeindex_resolution": {
                        0: "P0DT01H00M00S",
                        1: "P0DT01H00M00S",
                        2: "P0DT01H00M00S",
                    },
                    "onshore": {
                        0: [0.0516, 0.051, 0.0444],
                        1: [0.0516, 0.051, 0.0444],
                        2: [0.0516, 0.051, 0.0444],
                    },
                    "version": {0: "v8", 1: "v8", 2: "v8"},
                }
            )
            return process_mock

    mock_adapter.get_process.side_effect = mock_get_process
    # Call the method with the mock adapter
    result = DataPackage.build_datapackage(mock_adapter)
    result.save_datapackage_to_csv("_files/build_datapackage_test")

    check_if_csv_dirs_equal("_files/build_datapackage_goal", "_files/build_datapackage_test")




def test_save_datapackage():
    pass


def test_build_tabular_datapackage_from_adapter():
    download_collection(
        "https://energy.databus.dbpedia.org/felixmaur/collections/hack-a-thon/"
    )

    adapter = Adapter(
        "hack-a-thon",
        structure_name="structure",
        links_name="links",
    )
    dta = DataPackage.build_datapackage(adapter=adapter)
    dir = os.path.join(os.getcwd(), "_files", "tabular_datapackage_hack_a_thon")
    dta.save_datapackage_to_csv(dir)

    check_if_csv_dirs_equal(PATH_TMP, path_default)
    # FIXME: Get them closer together
    #  - Bus naming with regions -> get regions funktion von Hendrik
    #  - multiple inputs/outputs
