from data_adapter_oemof.dataclasses import (
    Commodity,
    Conversion,
    Load,
    Storage,
    Volatile,
)
from data_adapter_oemof.mappings import Mapper
from data_adapter_oemof import calculations


class Adapter:
    def __init__(self, datacls, data=None, builders=None):
        if builders is None:
            builders = BUILDER_MAP

        self.datacls = datacls
        self.data = data
        self.builders = builders

    def parametrize_dataclass(self):

        build_element = self.builders[self.datacls]

        instance = build_element(self)

        return instance


def build_commodity(self):
    instance = self.datacls()
    return instance


def build_conversion(self):
    instance = self.datacls()
    return instance


def build_load(self):
    instance = self.datacls()
    return instance


def build_storage(self):
    instance = self.datacls()
    return instance


def build_volatile(self):
    mapper = Mapper(self.data)

    instance = self.datacls(
        name=calculations.get_name(
            mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
        ),
        type=mapper.get("type"),
        carrier=mapper.get("carrier"),
        tech=mapper.get("tech"),
        capacity=mapper.get("capacity"),
        capacity_cost=calculations.get_capacity_cost(
            mapper.get("overnight_cost"),
            mapper.get("fixed_cost"),
            mapper.get("lifetime"),
            mapper.get("wacc"),
        ),
        bus=mapper.get("bus"),  # get_param("bus"),
        marginal_cost=mapper.get("marginal_cost"),
        profile=8,  # None,
        output_parameters=8,  # None,
    )
    return instance


BUILDER_MAP = {
    Commodity: build_commodity,
    Conversion: build_conversion,
    Load: build_load,
    Storage: build_storage,
    Volatile: build_volatile,
}
