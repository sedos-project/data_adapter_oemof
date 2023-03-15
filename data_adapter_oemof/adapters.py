import dataclasses

import pandas as pd
from oemof.tabular import facades
from oemof.solph import Bus

from data_adapter_oemof import calculations
from data_adapter_oemof.mappings import Mapper

#Todo: Build all dataadapters, build

def facade_adapter(cls):
    r"""
    Sets type of Busses to str
    Sets 'build_solph_components' to False in __init__.
    """
    # set type of bus to str
    for field in dataclasses.fields(cls):
        if field.type == Bus:
            field.type = str

    original_init = cls.__init__

    # do not build solph components
    def new_init(*args, **kwargs):
        original_init(*args, build_solph_components=False, **kwargs)

    cls.__init__ = new_init

    return cls


def get_default_mappings(cls, mapper):
    dictionary = {
        field.name: mapper.get(field.name) for field in dataclasses.fields(cls)
    }
    return dictionary


@facade_adapter
class CommodityAdapter(facades.Commodity):
    @classmethod
    def parametrize_dataclass(cls, data: dict, struct):
        mapper = Mapper(data)
        defaults = get_default_mappings(cls, mapper)

        attributes = {
            "name": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            # "capacity_cost": calculations.get_capacity_cost(
            #     mapper.get("overnight_cost"),
            #     mapper.get("fixed_cost"),
            #     mapper.get("lifetime"),
            #     mapper.get("wacc"),
            # ),
        }
        defaults.update(attributes)

        return cls(**defaults)


@facade_adapter
class ConversionAdapter(facades.Conversion):
    @classmethod
    def parametrize_dataclass(cls, data: dict, struct):
        mapper = Mapper(data)
        defaults = get_default_mappings(cls, mapper)

        attributes = {
            "name": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            # "capacity_cost": calculations.get_capacity_cost(
            #     mapper.get("overnight_cost"),
            #     mapper.get("fixed_cost"),
            #     mapper.get("lifetime"),
            #     mapper.get("wacc"),
            # ),
        }
        defaults.update(attributes)

        return cls(**defaults)


@facade_adapter
class LoadAdapter(facades.Load):
    @classmethod
    def parametrize_dataclass(cls, data: dict, struct):
        mapper = Mapper(data)
        defaults = get_default_mappings(cls)

        attributes = {
            "name": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            # "capacity_cost": calculations.get_capacity_cost(
            #     mapper.get("overnight_cost"),
            #     mapper.get("fixed_cost"),
            #     mapper.get("lifetime"),
            #     mapper.get("wacc"),
            # ),
        }
        defaults.update(attributes)

        return cls(**defaults)


@facade_adapter
class StorageAdapter(facades.Storage):
    @classmethod
    def parametrize_dataclass(cls, data: dict, struct):
        mapper = Mapper(data)
        defaults = get_default_mappings(cls, mapper)

        attributes = {
            "name": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            # Todo: decide where such calculations shall be made -> mapper?
            #   Essentially when there are multiple ways to obtain "capacity_cost" (either by calculation or if it
            #   is already existing in dataset)
            # "capacity_cost": calculations.get_capacity_cost(
            #     mapper.get("overnight_cost"),
            #     mapper.get("fixed_cost"),
            #     mapper.get("lifetime"),
            #     mapper.get("wacc"),
            # ),
        }
        defaults.update(attributes)

        return cls(**defaults)


@facade_adapter
class VolatileAdapter(facades.Volatile):
    @classmethod
    def parametrize_dataclass(cls, data: dict, struct):
        mapper = Mapper(data)
        defaults = get_default_mappings(cls, mapper)

        attributes = {
            "name": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            # "capacity_cost": calculations.get_capacity_cost(
            #     mapper.get("overnight_cost"),
            #     mapper.get("fixed_cost"),
            #     mapper.get("lifetime"),
            #     mapper.get("wacc"),
            # ),
        }
        defaults.update(attributes)

        return cls(**defaults)


TYPE_MAP = {
    "commodity": CommodityAdapter,
    "conversion": ConversionAdapter,
    "load": LoadAdapter,
    "storage": StorageAdapter,
    "volatile": VolatileAdapter,
    "dispatchable": ConversionAdapter,
    "battery_storage": StorageAdapter,
}
