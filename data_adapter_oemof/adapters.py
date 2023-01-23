from oemof.tabular import facades

from data_adapter_oemof.mappings import Mapper
from data_adapter_oemof import calculations


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
    def parametrize_dataclass(cls, data):
        mapper = Mapper(data)

        instance = cls(
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
        )
        return instance


TYPE_MAP = {
    "commodity": CommodityAdapter,
    "conversion": ConversionAdapter,
    "load": LoadAdapter,
    "storage": StorageAdapter,
    "volatile": VolatileAdapter,
}
