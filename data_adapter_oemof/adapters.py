import dataclasses

from oemof.tabular import facades

from data_adapter_oemof import calculations
from data_adapter_oemof.mappings import Mapper


def not_build_solph_components(cls):
    r"""
    Sets 'build_solph_components' to False in __init__.
    """
    original_init = cls.__init__

    def new_init(self, *args, **kwargs):
        kwargs["build_solph_components"] = False

        original_init(self, *args, **kwargs)

    cls.__init__ = new_init

    return cls


def get_default_mappings(cls, mapper):
    dictionary = {
        field.name: mapper.get(field.name) for field in dataclasses.fields(cls)
    }
    return dictionary


@not_build_solph_components
class CommodityAdapter(facades.Commodity):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


class ConversionAdapter(facades.Conversion):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@not_build_solph_components
class LoadAdapter(facades.Load):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@not_build_solph_components
class StorageAdapter(facades.Storage):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@not_build_solph_components
class VolatileAdapter(facades.Volatile):
    @classmethod
    def parametrize_dataclass(cls, data: dict):
        mapper = Mapper(data)
        defaults = get_default_mappings(cls, mapper)

        attributes = {
            "label": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            "capacity_cost": calculations.get_capacity_cost(capacity_cost=mapper.get("capacity_cost"),
                                                            lifetime=mapper.get("lifetime"), wacc=mapper.get("wacc"),
                                                            fixed_cost=mapper.get("fixed_cost")),
        }
        defaults.update(attributes)
        print(defaults)
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
