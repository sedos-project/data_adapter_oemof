import dataclasses
import os
import typing

import pandas as pd
import numpy as np

from data_adapter_oemof.adapters import (
    ExtractionTurbineAdapter,
    LinkAdapter,
    VolatileAdapter,
)
from data_adapter_oemof.mappings import Mapper
from unittest import mock
from data_adapter.preprocessing import Adapter



def test_get_with_mapping():
    adapter = VolatileAdapter

    timeseries = pd.DataFrame(
        {"onshore_BB": [1, 2, 3], "onshore_HH": [4, 5, 6]},
        index=["2016-01-01 01:00:00", "2035-01-01 01:00:00", "2050-01-01 01:00:00"],
    )

    mapping = {"VolatileAdapter": {"capacity": "custom_capacity"}}
    data = pd.DataFrame(
        {
            "region": {1: "TH"},
            "custom_capacity": {1: 100.0},
        }
    )

    mapper = Mapper(
        adapter=adapter,
        process_name="modex_tech_wind_turbine_onshore",
        data=data,
        timeseries=timeseries,
        mapping=mapping,
    )

    expected = pd.DataFrame(
        {
            "region": {1: "TH"},
            "capacity": {1: 100.0},
        }
    )

    for key, _ in expected.items():
        assert expected[key].values == mapper.get(key).values


def test_get_with_sequence():
    adapter = ExtractionTurbineAdapter

    timeseries = pd.DataFrame(
        {"condensing_efficiency_DE": [7, 8, 9], "electric_efficiency_DE": [10, 11, 12]},
        index=["2016-01-01 01:00:00", "2035-01-01 01:00:00", "2050-01-01 01:00:00"],
    )

    mapping = {
        "ExtractionTurbineAdapter": {"capacity": "installed_capacity"},
    }

    data = {
        "region": "DE",
        "installed_capacity": 200,
    }

    mapper = Mapper(
        adapter=adapter,
        process_name="modex_tech_generator_gas",
        data=data,
        timeseries=timeseries,
        mapping=mapping,
    )

    expected = {
        "region": "DE",
        "capacity": 200.0,
        "electric_efficiency": "electric_efficiency_DE",
        "condensing_efficiency": "condensing_efficiency_DE",
    }

    type = {i.name: i.type for i in dataclasses.fields(mapper.adapter)}

    for key, _ in expected.items():
        assert expected[key] == mapper.get(key, type.get(key))


def test_get_busses():
    adapter = ExtractionTurbineAdapter
    mapping = {"region": "custom_region", "capacity": "custom_capacity"}
    timeseries = pd.DataFrame(
        {"onshore_BB": [1, 2, 3], "onshore_HH": [4, 5, 6]},
        index=["2016-01-01 01:00:00", "2035-01-01 01:00:00", "2050-01-01 01:00:00"],
    )
    data = {
        "custom_region": "TH",
        "custom_capacity": 100.0,
    }

    bus_map = {
        "ExtractionTurbineAdapter": {
            "electricity_bus": "electricity",
            "heat_bus": "heat",
            "fuel_bus": "ch4",
        }
    }

    struct = {"default": {"inputs": ["ch4"], "outputs": ["electricity", "heat"]}}

    expected = {"electricity_bus": "electricity", "heat_bus": "heat", "fuel_bus": "ch4"}

    mapper = Mapper(
        adapter=adapter,
        process_name="modex_tech_generator_gas",
        data=data,
        timeseries=timeseries,
        mapping=mapping,
        bus_map=bus_map,
    )

    assert expected == mapper.get_busses(struct=struct)


def test_default_bus_mapping():
    adapter = VolatileAdapter
    mapping = {"region": "custom_region", "capacity": "custom_capacity"}
    timeseries = pd.DataFrame(
        {"onshore_BB": [1, 2, 3], "onshore_HH": [4, 5, 6]},
        index=["2016-01-01 01:00:00", "2035-01-01 01:00:00", "2050-01-01 01:00:00"],
    )
    data = {
        "custom_region": "TH",
        "custom_capacity": 100.0,
    }

    struct = {
        "default": {"inputs": [], "outputs": ["from_struct"]}
    }

    expected = {
        "bus": "from_struct",
    }
    # from structure
    mapper = Mapper(
        adapter=adapter,
        process_name="modex_tech_wind_turbine_onshore",
        data=data,
        timeseries=timeseries,
        mapping=mapping,
        bus_map={}
    )

    assert mapper.get_busses(struct=struct) == expected

    mapping = {"region": "custom_region", "capacity": "custom_capacity"}
    data = {
        "custom_region": "TH",
        "custom_capacity": 100.0,
    }

    struct = {"default": {"inputs": [], "outputs": ["electricity"]}}

    expected = {"bus": "from_bus_name_map"}

    mapper = Mapper(
        adapter=adapter,
        process_name="modex_tech_wind_turbine_onshore",
        data=data,
        timeseries=timeseries,
        mapping=mapping,
        bus_map={"VolatileAdapter":
                     {"bus": "from_bus_name_map"},
                 }
    )

    assert mapper.get_busses(struct=struct) == expected


def test_get_matched_busses():
    adapter = ExtractionTurbineAdapter
    mapping = {"region": "custom_region", "capacity": "custom_capacity"}
    timeseries = pd.DataFrame(
        {"onshore_BB": [1, 2, 3], "onshore_HH": [4, 5, 6]},
        index=["2016-01-01 01:00:00", "2035-01-01 01:00:00", "2050-01-01 01:00:00"],
    )
    data = {
        "custom_region": "TH",
        "custom_capacity": 100.0,
    }

    bus_map = {}

    struct = {"default": {"inputs": ["ch4fuel"], "outputs": ["elec", "heating"]}}

    expected = {"electricity_bus": "elec", "heat_bus": "heating", "fuel_bus": "ch4fuel"}

    mapper = Mapper(
        adapter=adapter,
        process_name="modex_tech_wind_turbine_onshore",
        data=data,
        timeseries=timeseries,
        mapping=mapping,
        bus_map=bus_map,
    )

    assert expected == mapper.get_busses(struct=struct)


def test_get_sequence_name():
    """
    test for getting sequence name and recognizing sequences within mapper
    :return:
    """
    adapter = ExtractionTurbineAdapter
    scalars = pd.DataFrame(
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
    timeseries = pd.DataFrame(
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
    structure = {
        "modex_tech_wind_turbine_onshore": {
            "default": {"inputs": ["onshore"], "outputs": ["electricity"]}
        },
    }

    for component_data in scalars.to_dict(orient="records"):
        mapper = Mapper(
            adapter,
            process_name="modex_tech_wind_turbine_onshore",
            data=component_data,
            timeseries=timeseries,
        )
        profile_col = mapper.get("profile", field_type=typing.Sequence)

        assert profile_col == "onshore_BB"
