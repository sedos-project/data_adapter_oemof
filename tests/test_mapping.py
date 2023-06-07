import numpy as np

from data_adapter_oemof.adapters import TYPE_MAP
from data_adapter_oemof.mappings import Mapper
from data_adapter_oemof.adapters import (
    ExtractionTurbineAdapter,
    VolatileAdapter,
    LinkAdapter,
)


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
    for key, _ in expected.items():
        mapped[key] = mapper.get(key)
    assert mapped == expected


def test_get_defaults():
    adapter = TYPE_MAP["volatile"]
    data = {
        "technology": "WindOnshore",  # Necessary for global_parameter_map
        "carrier": "Wind",  # TODO workaround until PR #20 is merged
        "profile": "onshore",  # TODO workaround until PR #20 is merged
        "region": "TH",
        "installed_capacity": 100,
    }
    struct = {"default": {"inputs": ["onshore"], "outputs": ["electricity"]}}

    parametrized_component = adapter.parametrize_dataclass(data, struct, None)

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
