import unittest

import pandas as pd

from data_adapter_oemof.adapters import ExtractionTurbineAdapter, VolatileAdapter


def test_get_with_mapping():
    adapter = VolatileAdapter(
        process_name="modex_tech_wind_turbine_onshore",
        data={
            "region": "TH",
            "custom_capacity": 100.0,
        },
        timeseries=pd.DataFrame(),
        structure={"inputs": [], "outputs": ["electricity"]},
        parameter_map={"VolatileAdapter": {"capacity": "custom_capacity"}},
        bus_map={},
    )

    expected = {
        "bus": "electricity",
        "capacity": 100.0,
        "carrier": "carrier",
        "name": "modex_tech_wind_turbine_onshore--0",
        "region": "TH",
        "tech": "tech",
        "type": "volatile",
    }

    unittest.TestCase().assertDictEqual(expected, adapter.facade_dict)


def test_get_with_sequence():
    adapter = ExtractionTurbineAdapter(
        process_name="modex_tech_generator_gas",
        data={
            "region": "DE",
            "installed_capacity": 200,
        },
        timeseries=pd.DataFrame(
            {
                "condensing_efficiency_DE": [7, 8, 9],
                "electric_efficiency_DE": [10, 11, 12],
            },
            index=["2016-01-01 01:00:00", "2035-01-01 01:00:00", "2050-01-01 01:00:00"],
        ),
        structure={"inputs": ["ch4"], "outputs": ["electricity"]},
        parameter_map={
            "ExtractionTurbineAdapter": {"capacity": "installed_capacity"},
        },
        bus_map={},
    )

    expected = {
        "type": "extraction_turbine",
        "carrier": "carrier",
        "tech": "tech",
        "condensing_efficiency": "condensing_efficiency_DE",
        "electric_efficiency": "electric_efficiency_DE",
        "capacity": 200,
        "region": "DE",
        "electricity_bus": "electricity",
        "heat_bus": "electricity",
        "fuel_bus": "ch4",
        "name": "modex_tech_generator_gas--1",
    }

    unittest.TestCase().assertDictEqual(expected, adapter.facade_dict)


def test_get_busses():
    adapter = ExtractionTurbineAdapter(
        process_name="modex_tech_generator_gas",
        data={
            "custom_region": "TH",
            "custom_capacity": 100.0,
        },
        timeseries=pd.DataFrame(
            {"onshore_BB": [1, 2, 3], "onshore_HH": [4, 5, 6]},
            index=["2016-01-01 01:00:00", "2035-01-01 01:00:00", "2050-01-01 01:00:00"],
        ),
        structure={"inputs": ["ch4"], "outputs": ["electricity", "heat"]},
        parameter_map={"region": "custom_region", "capacity": "custom_capacity"},
        bus_map={
            "ExtractionTurbineAdapter": {
                "electricity_bus": "electricity",
                "heat_bus": "heat",
                "fuel_bus": "ch4",
            }
        },
    )
    expected = {
        "carrier": "carrier",
        "electricity_bus": "electricity",
        "fuel_bus": "ch4",
        "heat_bus": "heat",
        "name": "modex_tech_generator_gas--2",
        "tech": "tech",
        "type": "extraction_turbine",
    }

    unittest.TestCase().assertDictEqual(expected, adapter.facade_dict)


def test_default_bus_mapping():
    adapter = VolatileAdapter(
        process_name="modex_tech_wind_turbine_onshore",
        data={
            "custom_region": "TH",
            "custom_capacity": 100.0,
        },
        timeseries=pd.DataFrame(
            {"onshore_BB": [1, 2, 3], "onshore_HH": [4, 5, 6]},
            index=["2016-01-01 01:00:00", "2035-01-01 01:00:00", "2050-01-01 01:00:00"],
        ),
        structure={"inputs": [], "outputs": ["from_struct"]},
        parameter_map={"region": "custom_region", "capacity": "custom_capacity"},
        bus_map={},
    )
    expected = {
        "bus": "from_struct",
        "carrier": "carrier",
        "name": "modex_tech_wind_turbine_onshore--3",
        "tech": "tech",
        "type": "volatile",
    }
    unittest.TestCase().assertDictEqual(expected, adapter.facade_dict)

    adapter = VolatileAdapter(
        process_name="modex_tech_wind_turbine_onshore",
        data={
            "custom_region": "TH",
            "custom_capacity": 100.0,
        },
        timeseries=pd.DataFrame(
            {"onshore_BB": [1, 2, 3], "onshore_HH": [4, 5, 6]},
            index=["2016-01-01 01:00:00", "2035-01-01 01:00:00", "2050-01-01 01:00:00"],
        ),
        structure={"inputs": [], "outputs": ["electricity"]},
        parameter_map={"region": "custom_region", "capacity": "custom_capacity"},
        bus_map={
            "VolatileAdapter": {"bus": "from_bus_name_map"},
        },
    )
    expected = {
        "bus": "from_bus_name_map",
        "carrier": "carrier",
        "name": "modex_tech_wind_turbine_onshore--4",
        "tech": "tech",
        "type": "volatile",
    }
    unittest.TestCase().assertDictEqual(expected, adapter.facade_dict)


def test_get_matched_busses():
    adapter = ExtractionTurbineAdapter(
        process_name="modex_tech_wind_turbine_onshore",
        parameter_map={"region": "custom_region", "capacity": "custom_capacity"},
        timeseries=pd.DataFrame(
            {"onshore_BB": [1, 2, 3], "onshore_HH": [4, 5, 6]},
            index=["2016-01-01 01:00:00", "2035-01-01 01:00:00", "2050-01-01 01:00:00"],
        ),
        data={
            "custom_region": "TH",
            "custom_capacity": 100.0,
        },
        bus_map={},
        structure={"inputs": ["ch4fuel"], "outputs": ["elec", "heating"]},
    )

    expected = {
        "carrier": "carrier",
        "electricity_bus": "elec",
        "fuel_bus": "ch4fuel",
        "heat_bus": "heating",
        "name": "modex_tech_wind_turbine_onshore--5",
        "tech": "tech",
        "type": "extraction_turbine",
    }
    unittest.TestCase().assertDictEqual(expected, adapter.facade_dict)


def test_get_sequence_name():
    """
    test for getting sequence name and recognizing sequences within mapper
    :return:
    """
    adapter = VolatileAdapter(
        process_name="modex_tech_wind_turbine_onshore",
        bus_map={},
        structure={"inputs": [], "outputs": ["electricity"]},
        data={
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
        },
        timeseries=pd.DataFrame(
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
        parameter_map={"modex_tech_wind_turbine_onshore": {"profile": "onshore"}},
    )
    expected = {
        "type": "volatile",
        "carrier": {0: "wind", 1: "wind", 2: "wind"},
        "tech": {
            0: "wind_turbine_onshore",
            1: "wind_turbine_onshore",
            2: "wind_turbine_onshore",
        },
        "profile": "onshore_BB",
        "lifetime": 25,
        "fixed_costs": {0: 23280.0, 1: 12600.0, 2: 11340.0},
        "region": {0: "BB", 1: "BB", 2: "BB"},
        "year": {0: 2016, 1: 2030, 2: 2050},
        "bus": "electricity",
        "name": "modex_tech_wind_turbine_onshore--0",
    }
    unittest.TestCase().assertDictEqual(expected, adapter.facade_dict)
