import os
from unittest import mock

import pandas
import pandas as pd
import pytest
from data_adapter.databus import download_collection
from data_adapter.preprocessing import Adapter
from oemof.tabular.datapackage import EnergySystem, Model
from oemof.tabular.facades import Bus, Commodity, Dispatchable, Load, Storage, Volatile
from pandas import Timestamp
from utils import PATH_TEST_FILES, check_if_csv_dirs_equal

from data_adapter_oemof.build_datapackage import DataPackage, refactor_timeseries

path_default = PATH_TEST_FILES / "_files"

# Todo: reduce warnings


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


def test_build_datapackage():
    """
    Test build Datapackage with mocking Adapter
    Returns
    -------
    """

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
                    "capacity": {0: 17.8, 1: 17.8, 2: 17.8},
                    "invest_relation_output_capacity": {0: 3.3, 1: 3.3, 2: 3.3},
                    "tech": {
                        0: "storage_battery",
                        1: "storage_battery",
                        2: "storage_battery",
                    },
                    "carrier": {
                        0: "Lithium",
                        1: "Lithium",
                        2: "Lithium",
                    },
                    "fixed_costs": {0: 1, 1: 2, 2: 3},
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
                        1: 330.2,
                        2: 330.2,
                    },
                    "emission_factor": {
                        0: 0.2,
                        1: 0.2,
                        2: 0.2,
                    },  # Todo: not implemented in any Facade yet?!
                    "fuel_costs": {0: 25.9, 1: 25.9, 2: 49.36},
                    "tech": {
                        0: "generator_gas",
                        1: "generator_gas",
                        2: "generator_gas",
                    },
                    "carrier": {
                        0: "gas",
                        1: "gas",
                        2: "gas",
                    },
                    "condensing_efficiency": {
                        0: 0.16,
                        1: 0.3,
                        2: 0.5,
                    },
                    "electric_efficiency": {0: 0.35, 1: 0.35, 2: 0.4},
                    "thermal_efficiency": {0: 0.5, 1: 0.5, 2: 0.45},
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
                    "tech": {
                        0: "wind_turbine_onshore",
                        1: "wind_turbine_onshore",
                        2: "wind_turbine_onshore",
                    },
                    "carrier": {
                        0: "wind",
                        1: "wind",
                        2: "wind",
                    },
                }
            )
            process_mock.timeseries = pd.DataFrame(
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
            )
            return process_mock

    # Create a mock adapter object for testing
    mock_adapter = mock.Mock(spec=Adapter)
    # Mock the required methods and attributes of the Adapter
    mock_adapter.get_structure.return_value = {
        "modex_tech_storage_battery": {
            "default": {"inputs": ["electricity"], "outputs": []}
        },
        "modex_tech_generator_gas": {
            "default": {"inputs": ["ch4"], "outputs": ["electricity"]},
        },
        "modex_tech_wind_turbine_onshore": {
            "default": {"inputs": ["onshore"], "outputs": ["electricity"]}
        },
    }

    mock_adapter.get_process.side_effect = mock_get_process
    # Call the method with the mock adapter
    test_path = os.path.join(path_default, "build_datapackage_test")
    goal_path = os.path.join(path_default, "build_datapackage_goal")
    process_adapter_map = {
        "modex_tech_generator_gas": "ExtractionTurbineAdapter",
        "modex_tech_storage_battery": "StorageAdapter",
        "modex_tech_wind_turbine_onshore": "VolatileAdapter",
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
        "modex_tech_wind_turbine_onshore": {"profile": "onshore"},
    }
    result = DataPackage.build_datapackage(
        adapter=mock_adapter,
        process_adapter_map=process_adapter_map,
        parameter_map=parameter_map,
    )
    result.save_datapackage_to_csv(test_path)

    check_if_csv_dirs_equal(
        goal_path,
        test_path,
    )


def test_build_tabular_datapackage_from_adapter():
    download_collection(
        "https://databus.openenergyplatform.org/felixmaur/collections/hack-a-thon/"
    )

    adapter = Adapter(
        "hack-a-thon",
        structure_name="structure",
    )
    process_adapter_map = {
        "modex_tech_storage_battery": "StorageAdapter",
        "modex_tech_wind_turbine_onshore": "VolatileAdapter",
        "modex_tech_load_load": "LoadAdapter",
        "modex_tech_storage_pumped": "StorageAdapter",
        "modex_tech_generator_steam": "DispatchableAdapter",
        "modex_tech_photovoltaics_utility": "VolatileAdapter",
        "modex_com_lignite": "CommodityAdapter",
        "modex_tech_chp_steam": "DispatchableAdapter",
    }

    parameter_map = {
        "DEFAULT": {
            "marginal_cost": "variable_costs",
            "fixed_cost": "fixed_costs",
            "capacity_cost": "capital_costs",
            "capacity": "installed_capacity",
            "capacity_potential": "expansion_limit",
            "carrier_cost": "fuel_costs",
        },
        "StorageAdapter": {
            "invest_relation_output_capacity": "e2p_ratio",
            "inflow_conversion_factor": "input_ratio",
            "outflow_conversion_factor": "output_ratio",
        },
        "CommodityAdapter": {
            "amount": "natural_domestic_limit",
        },
        "DispatchableAdapter": {
            "efficiency": "output_ratio",
        },
        # "modex_tech_wind_turbine_onshore": {"profile": "onshore"},
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


def test_read_datapackage():
    es = EnergySystem.from_datapackage(
        "_files/tabular_datapackage_hack_a_thon_goal/datapackage.json",
        typemap={
            "load": Load,
            "dispatchable": Dispatchable,
            "extraction_turbine": Dispatchable,
            "bus": Bus,
            "storage": Storage,
            "volatile": Volatile,
            "commodity": Commodity,
        },
    )
    model = Model(es)
    # Test Model yet to come
    test_model = 1
    assert model, test_model


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
