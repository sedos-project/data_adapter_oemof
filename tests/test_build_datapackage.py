import json
import os

import pandas as pd
import pytest
from data_adapter.databus import download_collection
from data_adapter.preprocessing import Adapter
from pandas import Timestamp
from setup_mock import define_mock
from utils import PATH_TEST_FILES, check_if_csv_dirs_equal

from data_adapter_oemof.build_datapackage import DataPackage

path_default = PATH_TEST_FILES / "_files"


def test_refactor_timeseries():
    timeseries = pd.DataFrame(
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
    expected_df = pd.DataFrame(
        {
            "offshore_BB": [7.0, 8, 9, None, None],
            "offshore_HH": [10.0, 11, 12, 35, 36],
            "onshore_BB": [1.0, 2, 3, None, None],
            "onshore_HH": [4.0, 5, 6, 33, 34],
        },
        index=pd.DatetimeIndex(
            ["01:00:00", "02:00:00", "03:00:00", "09:00:00", "10:00:00"]
        ),
    )
    expected_df = expected_df.sort_index(axis=1)
    refactored_ts = refactored_ts.sort_index(axis=1)
    pd.testing.assert_frame_equal(expected_df, refactored_ts)


def test_build_datapackage():
    """
    Test build Datapackage with mocking Adapter
    Returns
    -------
    """

    # Call the method with the mock adapter
    test_path = os.path.join(path_default, "build_datapackage_test")
    goal_path = os.path.join(path_default, "build_datapackage_goal")
    # Define different return values for get_process based on the structure

    mock = define_mock()

    result = DataPackage.build_datapackage(
        adapter=mock.mock_adapter,
        process_adapter_map=mock.process_adapter_map,
        parameter_map=mock.parameter_map,
    )
    result.save_datapackage_to_csv(test_path)

    check_if_csv_dirs_equal(
        goal_path,
        test_path,
    )


@pytest.mark.skip(reason="Tackled in different Branch")
def test_build_tabular_datapackage_from_adapter():
    download_collection(
        "https://energy.databus.dbpedia.org/felixmaur/collections/hack-a-thon/"
    )

    adapter = Adapter(
        "hack-a-thon",
        structure_name="structure",
        links_name="links",
    )
    process_adapter_map = {
        "modex_tech_storage_battery": "StorageAdapter",
        "modex_tech_generator_gas": "ConversionAdapter",
        "modex_tech_wind_turbine_onshore": "VolatileAdapter",
        "modex_demand": "LoadAdapter",
    }

    parameter_map = {
        "DEFAULT": {
            "marginal_cost": "variable_costs",
            "fixed_cost": "fixed_costs",
            "capacity_cost": "capital_costs",
        },
        "ExtractionTurbineAdapter": {
            "carrier_cost": "fuel_costs",
            "capacity": "installed_capacity",
        },
        "StorageAdapter": {
            "capacity_potential": "expansion_limit",
            "capacity": "installed_capacity",
            "invest_relation_output_capacity": "e2p_ratio",
            "inflow_conversion_factor": "input_ratio",
            "outflow_conversion_factor": "output_ratio",
        },
        "modex_tech_wind_turbine_onshore": {"profile": "onshore"},
    }

    dta = DataPackage.build_datapackage(
        adapter=adapter,
        process_adapter_map=process_adapter_map,
        parameter_map=parameter_map,
    )
    dir = os.path.join(path_default, "tabular_datapackage_hack_a_thon")
    dta.save_datapackage_to_csv(dir)

    check_if_csv_dirs_equal(
        dir, os.path.join(path_default, "tabular_datapackage_hack_a_thon_goal")
    )
    # FIXME: Demand is in different Format than expected.


@pytest.mark.skip(reason="Needs period csv implementation first.")
def test_read_datapackage():
    # FIXME: Period csv is missing.
    # return "FIXME first"
    # es = EnergySystem.from_datapackage(
    #     "_files/build_datapackage_test/datapackage.json",
    #     typemap={
    #         "load": Load,
    #         "dispatchable": Dispatchable,
    #         "bus": Bus,
    #         "link": Link,
    #         "storage": Storage,
    #         "volatile": Volatile,
    #         "conversion": Conversion,
    #     },
    # )
    # model = Model(es)
    pass


def test_period_csv_creation():
    sequence_created = DataPackage.get_periods_from_parametrized_sequences(
        {
            "bus": pd.DataFrame(),
            "dispatchable": [],
            "wind_power": pd.DataFrame(
                {
                    "onshore_BB": {
                        "2016-01-01T00:00:00": 0.0516,
                        "2016-01-01T01:00:00": 0.051,
                        "2016-01-01T02:00:00": 0.0444,
                        "2030-01-01T00:00:00": 0.0526,
                        "2030-01-01T01:00:00": 0.051,
                        "2030-01-01T02:00:00": 0.0444,
                        "2050-01-01T00:00:00": 0.0536,
                        "2050-01-01T01:00:00": 0.051,
                        "2050-01-01T02:00:00": 0.0444,
                    },
                }
            ),
        }
    )
    sequence_goal = pd.DataFrame(
        {
            "periods": {
                Timestamp("2016-01-01 00:00:00"): 0,
                Timestamp("2016-01-01 01:00:00"): 0,
                Timestamp("2016-01-01 02:00:00"): 0,
                Timestamp("2030-01-01 00:00:00"): 1,
                Timestamp("2030-01-01 01:00:00"): 1,
                Timestamp("2030-01-01 02:00:00"): 1,
                Timestamp("2050-01-01 00:00:00"): 2,
                Timestamp("2050-01-01 01:00:00"): 2,
                Timestamp("2050-01-01 02:00:00"): 2,
            },
            "timeincrement": {
                Timestamp("2016-01-01 00:00:00"): 1,
                Timestamp("2016-01-01 01:00:00"): 1,
                Timestamp("2016-01-01 02:00:00"): 1,
                Timestamp("2030-01-01 00:00:00"): 1,
                Timestamp("2030-01-01 01:00:00"): 1,
                Timestamp("2030-01-01 02:00:00"): 1,
                Timestamp("2050-01-01 00:00:00"): 1,
                Timestamp("2050-01-01 01:00:00"): 1,
                Timestamp("2050-01-01 02:00:00"): 1,
            },
        }
    )
    sequence_goal.index.name = "timeindex"
    pd.testing.assert_frame_equal(sequence_goal, sequence_created)


def test_tsam():
    """
    Uses Mock to create datapackage and then applies timeseries aggregation to it.
    Returns
    -------

    """

    tsam_folder = os.path.join(path_default, "tsam")

    # Define Test mock - same as above
    mock = define_mock()

    result = DataPackage.build_datapackage(
        adapter=mock.mock_adapter,
        process_adapter_map=mock.process_adapter_map,
        parameter_map=mock.parameter_map,
        location_to_save_to=tsam_folder,
    )

    #############################################################################
    # start tsam
    #############################################################################

    # read tsam config json

    with open(os.path.join(tsam_folder, "tsam_config.json"), "r") as f:
        tsam_config = json.load(f)

    result.time_series_aggregation(
        tsam_config=tsam_config, location_to_save_to=tsam_folder
    )
    check_if_csv_dirs_equal(tsam_folder, os.path.join(tsam_folder, "..", "tsam_goal"))
