import typing

import pandas as pd

from data_adapter_oemof.mappings import Mapper
from data_adapter_oemof.adapters import (
    ExtractionTurbineAdapter,
    VolatileAdapter,
    LinkAdapter,
)
from test_build_datapackage import refactor_timeseries
from data_adapter.preprocessing import Adapter


def test_get_with_mapping():
    mapping = {"region": "custom_region", "capacity": "custom_capacity"}
    data = {
        "custom_region": "TH",
        "custom_capacity": 100.0,
    }

    mapper = Mapper(mapping=mapping, data=data)

    expected = {
        "region": "TH",
        "capacity": 100.0,
    }
    mapped = {}
    for key, value in expected.items():
        mapped[key] = mapper.get(key)
    assert mapped == expected


def test_get_defaults():
    pass


def test_get_busses():
    mapping = {"region": "custom_region", "capacity": "custom_capacity"}
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

    mapper = Mapper(mapping=mapping, data=data, bus_map=bus_map)

    assert expected == mapper.get_busses(cls=ExtractionTurbineAdapter, struct=struct)


def test_default_bus_mapping():
    mapping = {"region": "custom_region", "capacity": "custom_capacity"}
    data = {
        "custom_region": "TH",
        "custom_capacity": 100.0,
    }

    struct = {"inputs": ["electricity_bus_1"], "outputs": ["electricity_bus_2"]}

    expected = {
        "from_bus": "electricity_bus_1",
        "to_bus": "electricity_bus_2",
    }

    mapper = Mapper(mapping=mapping, data=data)

    assert mapper.get_busses(cls=LinkAdapter, struct=struct) == expected

    mapping = {"region": "custom_region", "capacity": "custom_capacity"}
    data = {
        "custom_region": "TH",
        "custom_capacity": 100.0,
    }

    struct = {"inputs": [], "outputs": ["electricity"]}

    expected = {"bus": "electricity"}

    mapper = Mapper(mapping=mapping, data=data)

    assert mapper.get_busses(cls=VolatileAdapter, struct=struct) == expected


def test_get_matched_busses():
    mapping = {"region": "custom_region", "capacity": "custom_capacity"}
    data = {
        "custom_region": "TH",
        "custom_capacity": 100.0,
    }

    bus_map = {}

    struct = {"inputs": ["ch4fuel"], "outputs": ["elec", "heating"]}

    expected = {"electricity_bus": "elec", "heat_bus": "heating", "fuel_bus": "ch4fuel"}

    mapper = Mapper(mapping=mapping, data=data, bus_map=bus_map)

    assert expected == mapper.get_busses(cls=ExtractionTurbineAdapter, struct=struct)


def test_get_sequence_name():
    """
    test for getting sequence name and recognizing sequences within mapper
    :return:
    """

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

    structure = {
        "conversion": {
            "default": {"inputs": ["ch4"], "outputs": ["electricity", "heat"]}
        }
    }

    timeseries = refactor_timeseries(timeseries)

    adapter = Adapter(
        "minimal_example",
        structure_name="minimal_structure",
        links_name="minimal_links",
    )

    mapper = Mapper(data=scalar_data, timeseries=timeseries)
    heat_col = mapper.get("heat", field_type=typing.Sequence)
