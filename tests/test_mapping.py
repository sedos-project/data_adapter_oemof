import dataclasses
import typing

import pandas as pd
import numpy as np
from test_build_datapackage import refactor_timeseries

from data_adapter_oemof.adapters import (
    FACADE_ADAPTERS,
    ExtractionTurbineAdapter,
    LinkAdapter,
    VolatileAdapter,
)
from data_adapter_oemof.mappings import Mapper


def test_get_with_mapping():
    adapter = FACADE_ADAPTERS["VolatileAdapter"]

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
    adapter = FACADE_ADAPTERS["ExtractionTurbineAdapter"]

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
    adapter = FACADE_ADAPTERS["ExtractionTurbineAdapter"]
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

    struct = {"inputs": ["ch4"], "outputs": ["electricity", "heat"]}

    expected = {"electricity_bus": "electricity", "heat_bus": "heat", "fuel_bus": "ch4"}

    mapper = Mapper(
        adapter=adapter,
        process_name="modex_tech_generator_gas",
        data=data,
        timeseries=timeseries,
        mapping=mapping,
        bus_map=bus_map,
    )

    assert expected == mapper.get_busses(cls=ExtractionTurbineAdapter, struct=struct)


def test_default_bus_mapping():
    adapter = FACADE_ADAPTERS["VolatileAdapter"]
    mapping = {"region": "custom_region", "capacity": "custom_capacity"}
    timeseries = pd.DataFrame(
        {"onshore_BB": [1, 2, 3], "onshore_HH": [4, 5, 6]},
        index=["2016-01-01 01:00:00", "2035-01-01 01:00:00", "2050-01-01 01:00:00"],
    )
    data = {
        "custom_region": "TH",
        "custom_capacity": 100.0,
    }

    struct = {"inputs": ["electricity_bus_1"], "outputs": ["electricity_bus_2"]}

    expected = {
        "from_bus": "electricity_bus_1",
        "to_bus": "electricity_bus_2",
    }

    mapper = Mapper(
        adapter=adapter,
        process_name="modex_tech_wind_turbine_onshore",
        data=data,
        timeseries=timeseries,
        mapping=mapping,
    )

    assert mapper.get_busses(cls=LinkAdapter, struct=struct) == expected

    mapping = {"region": "custom_region", "capacity": "custom_capacity"}
    data = {
        "custom_region": "TH",
        "custom_capacity": 100.0,
    }

    struct = {"inputs": [], "outputs": ["electricity"]}

    expected = {"bus": "electricity"}

    mapper = Mapper(
        adapter=adapter,
        process_name="modex_tech_wind_turbine_onshore",
        data=data,
        timeseries=timeseries,
        mapping=mapping,
    )

    assert mapper.get_busses(cls=VolatileAdapter, struct=struct) == expected


def test_get_matched_busses():
    adapter = FACADE_ADAPTERS["VolatileAdapter"]
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

    struct = {"inputs": ["ch4fuel"], "outputs": ["elec", "heating"]}

    expected = {"electricity_bus": "elec", "heat_bus": "heating", "fuel_bus": "ch4fuel"}

    mapper = Mapper(
        adapter=adapter,
        process_name="modex_tech_wind_turbine_onshore",
        data=data,
        timeseries=timeseries,
        mapping=mapping,
        bus_map=bus_map,
    )

    assert expected == mapper.get_busses(cls=ExtractionTurbineAdapter, struct=struct)


def test_get_sequence_name():
    """
    test for getting sequence name and recognizing sequences within mapper
    :return:
    """
    adapter = FACADE_ADAPTERS["ExtractionTurbineAdapter"]

    scalar_data = pd.DataFrame(
        {
            "region": {0: "TH", 1: "HH"},
            "year": {0: 2011, 1: 2011},
            "ammount": {0: 5000.0, 1: 10000},
            "type": {0: "conversion", 1: "conversion"},
        }
    )

    timeseries = pd.DataFrame(
        {
            "region": {0: "TH", 1: "HH"},
            "timeindex_start": {0: "2011-01-01T00:00:00Z", 1: "2011-01-01T02:00:00Z"},
            "timeindex_stop": {0: "2011-01-01T02:00:00Z", 1: "2011-01-01T04:00:00Z"},
            "timeindex_resolution": {0: "P0DT01H00M00S", 1: "P0DT01H00M00S"},
            "electricity": {0: [1, 2, 3], 1: [2, 3, 4]},
            "heat": {0: [5, 6, 7], 1: [8, 9, 10]},
        }
    )
    timeseries = refactor_timeseries(timeseries)

    structure = {
        "conversion": {
            "default": {"inputs": ["ch4"], "outputs": ["electricity", "heat"]}
        }
    }
    for component_data in scalar_data.to_dict(orient="records"):
        mapper = Mapper(
            adapter,
            process_name="modex_tech_wind_turbine_onshore",
            data=component_data,
            timeseries=timeseries,
        )
        heat_col = mapper.get("heat", field_type=typing.Sequence)
        electricity_col = mapper.get("electricity", field_type=typing.Sequence)

        assert heat_col == "heat_" + component_data["region"]
        assert electricity_col == "electricity_" + component_data["region"]
