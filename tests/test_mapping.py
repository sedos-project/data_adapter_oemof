from data_adapter_oemof.mappings import Mapper
from data_adapter_oemof.adapters import ExtractionTurbineAdapter, VolatileAdapter


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

    bus_map = {"ExtractionTurbineAdapter": {"electricity_bus": "electricity",
                                            "heat_bus": "heat",
                                            #"fuel_bus": "ch4"
                                            }}

    struct = {'inputs': ['ch4'], 'outputs': ['electricity', 'heat']}

    mapper = Mapper(mapping=mapping, data=data, bus_map=bus_map)
    mapper.get_busses(cls=ExtractionTurbineAdapter, struct=struct)
    print(mapper.get_busses(cls=ExtractionTurbineAdapter, struct=struct))