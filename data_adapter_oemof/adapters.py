from oemof.tabular import facades

from data_adapter_oemof.mappings import Mapper
from data_adapter_oemof import calculations


class CommodityAdapter(facades.Commodity):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


class ConversionAdapter(facades.Conversion):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


class LoadAdapter(facades.Load):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


class StorageAdapter(facades.Storage):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


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
