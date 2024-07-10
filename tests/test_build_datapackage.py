import json
import os

import pandas as pd
import pytest
from data_adapter.databus import download_collection
from data_adapter.preprocessing import Adapter
from data_adapter.structure import Structure
from oemof.solph import EnergySystem
from oemof.tabular.datapackage import Model
from oemof.tabular.facades import Bus, Commodity, Dispatchable, Load, Storage, Volatile
from pandas import Timestamp
from setup_mock import define_mock
from utils import PATH_TEST_FILES, check_if_csv_dirs_equal

from data_adapter_oemof.build_datapackage import DataPackage

path_default = PATH_TEST_FILES / "_files"


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


def test_build_tabular_datapackage_from_adapter():
    download_collection(
        "https://databus.openenergyplatform.org/felixmaur/collections/hack-a-thon/"
    )
    structure = Structure(
        "SEDOS_Modellstruktur",
        process_sheet="hack-a-thon",
        parameter_sheet="Parameter_IO_hack-a-thon",
        helper_sheet="empty_helper",
    )

    adapter = Adapter(
        "hack-a-thon",
        structure=structure,
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


@pytest.mark.skip(reason="Pumped storage data has no lifetime yet")
def test_read_datapackage():
    es = EnergySystem.from_datapackage(
        os.path.join(
            path_default, "tabular_datapackage_hack_a_thon", "datapackage.json"
        ),
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
    assert model, model


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
    pd.testing.assert_frame_equal(sequence_goal, sequence_created, check_dtype=False)


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


@pytest.mark.skip(reason="Waiting for registered helper set on databus")
def test_decomissioning():
    """
    Tests Decomissioning with ind_steel_cast example
    Returns
    -------

    """
    from examples.industry import data_adapter_industry

    data_adapter_industry


def test_filling_nans():
    """
    Test to fill nan values on rows where Investment is 0 or capacity is 0
    Returns
    -------

    """
    structure = Structure(
        "test_fill_nans",
        process_sheet="Processes_O1",
        parameter_sheet="Parameter_Input-Output",
        helper_sheet="Helper_O1",
    )
    adapter = Adapter(
        "test_fill_nans",
        structure=structure,
    )

    process_adapter_map = {
        "ind_steel_casting_0": "MIMOAdapter",
        "ind_steel_hyddri_1": "MIMOAdapter",
    }

    parameter_map = {
        "DEFAULT": {"capacity_w_inst_0": "capacity", "capacity": "capacity_w_inst_0"}
    }

    data_package = DataPackage.build_datapackage(
        adapter, process_adapter_map, parameter_map
    )

    assert (
        data_package.parametrized_elements["ind_steel_casting_0"][
            "conversion_factor_sec_methane"
        ][0]
        == 1.5774000000000008
    )
    assert data_package.parametrized_elements["ind_steel_hyddri_1"][
        "conversion_factor_sec_elec_ind"
    ][0] == [
        0.7000000000000001,
        0.7000000000000001,
        0.7000000000000001,
        0.7000000000000001,
        0.7,
        0.7,
        0.7,
        0.7,
        0.7,
        0.7,
    ]


# Check wheter result would be correct
