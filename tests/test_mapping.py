import numpy as np

from data_adapter_oemof.adapters import TYPE_MAP
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
    adapter = TYPE_MAP["volatile"]

    timeseries = pd.DataFrame(
        {"onshore_BB": [1, 2, 3], "onshore_HH": [4, 5, 6]},
        index=["2016-01-01 01:00:00", "2035-01-01 01:00:00", "2050-01-01 01:00:00"],
    )

    mapping = {
        "VolatileAdapter": {"region": "custom_region", "capacity": "custom_capacity"}
    }
    data = {
        "custom_region": "TH",
        "custom_capacity": 100.0,
    }

    mapper = Mapper(adapter=adapter, data=data, timeseries=timeseries, mapping=mapping)

    expected = {
        "region": "TH",
        "capacity": 100.0,
    }

    mapped = {}
    for key, _ in expected.items():
        mapped[key] = mapper.get(key)
    assert mapped == expected


def test_get_defaults():
    adapter = TYPE_MAP["volatile"]
    timeseries = pd.DataFrame(
        {"onshore_BB": [1, 2, 3], "onshore_HH": [4, 5, 6]},
        index=["2016-01-01 01:00:00", "2035-01-01 01:00:00", "2050-01-01 01:00:00"],
    )

    data = {
        "technology": "WindOnshore",  # Necessary for global_parameter_map
        "carrier": "Wind",  # TODO workaround until PR #20 is merged
        # "profile": "onshore",  # TODO workaround until PR #20 is merged
        "region": "TH",
        "installed_capacity": 100,
    }
    struct = {"default": {"inputs": ["onshore"], "outputs": ["electricity"]}}

    parametrized_component = adapter.parametrize_dataclass(
        data=data, timeseries=timeseries, struct=struct
    )

    expected_component = {
        "bus": "electricity",
        "carrier": "Wind",
        "tech": "WindOnshore",
        "profile": "onshore",
        "capacity": 100,
        "capacity_cost": None,
        "capacity_potential": np.inf,
        "capacity_minimum": None,
        "expandable": False,
        "marginal_cost": 0,
        "output_parameters": {},
        "name": "TH-Wind-WindOnshore",
        "type": "Volatile",
    }

    assert expected_component == parametrized_component.as_dict()


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
    electricity_col = mapper.get("electricity", field_type=typing.Sequence)

    expected_heat = pd.Series(["heat_TH", "heat_HH"], name="region")
    expected_electricity = pd.Series(
        ["electricity_TH", "electricity_HH"], name="region"
    )
    pd.testing.assert_series_equal(
        left=heat_col, right=expected_heat, check_category_order=False
    )
    pd.testing.assert_series_equal(
        left=electricity_col, right=expected_electricity, check_category_order=False
    )
