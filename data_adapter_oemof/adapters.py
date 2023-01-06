from data_adapter_oemof.dataclasses import (
    Commodity,
    Conversion,
    Load,
    Storage,
    Volatile,
)


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
    instance = self.datacls(
        name="calculations.get_name(data.region, data.carrier, data.tech)",
        type="self.data",
        carrier=self.data["carrier"],
        tech=self.data["tech"],
        capacity=self.data["capacity"],  # get_param("capacity"),
        capacity_cost=self.data["capacity_cost"],  # get_capacity_cost("capacity_cost"),
        bus=8,  # get_param("bus"),
        marginal_cost=8,  # get_param("marginal_cost"),
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
