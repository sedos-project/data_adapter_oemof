import dataclasses

from oemof.tabular import facades
from oemof.solph import Bus

from data_adapter_oemof import calculations
from data_adapter_oemof.mappings import Mapper


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
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@facade_adapter
class ConversionAdapter(facades.Conversion):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@facade_adapter
class LoadAdapter(facades.Load):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@facade_adapter
class StorageAdapter(facades.Storage):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@facade_adapter
class VolatileAdapter(facades.Volatile):
    @classmethod
    def parametrize_dataclass(cls, data: dict):
        mapper = Mapper(data)
        defaults = get_default_mappings(cls, mapper)

        attributes = {
            "name": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            "capacity_cost": calculations.get_capacity_cost(
                mapper.get("overnight_cost"),
                mapper.get("fixed_cost"),
                mapper.get("lifetime"),
                mapper.get("wacc"),
            ),
        }
        defaults.update(attributes)
        return cls(**defaults)


TYPE_MAP = {
    "commodity": CommodityAdapter,
    "conversion": ConversionAdapter,
    "load": LoadAdapter,
    "storage": VolatileAdapter,
    "volatile": VolatileAdapter,
    "dispatchable": VolatileAdapter,
    "battery_storage": StorageAdapter,
}
