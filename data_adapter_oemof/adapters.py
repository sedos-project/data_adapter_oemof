import dataclasses
import logging

from oemof.tabular import facades
from oemof.solph import Bus

from data_adapter_oemof import calculations
from data_adapter_oemof.mappings import Mapper

logger = logging.getLogger()


class Adapter:
    extra_attributes = ("name", "type")

    def as_dict(self):
        """
        Adds function to return DataFrame from adapter.

        This mixin is necessary as `pd.DataFrame(dataclass_instance)` will only create columns for attributes already present in dataclass.
        But we add custom_attributes (i.e. "name") which would be neglected.
        """
        fields = dataclasses.fields(self)
        data = {field.name: getattr(self, field.name) for field in fields}
        for attr in self.extra_attributes:
            data[attr] = getattr(self, attr)
        return data

    @classmethod
    def parametrize_dataclass(cls, data: dict, struct, process_type):
        mapper = Mapper(data)
        defaults = mapper.get_default_mappings(cls, struct)
        attributes = {
            "name": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            "region": mapper.get("region"),
            "year": mapper.get("year"),
            "profile": mapper.get("profile")
        }
        defaults.update(attributes)

        return defaults


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
    def new_init(self, *args, **kwargs):
        custom_attributes = {}
        original_fields = tuple(field.name for field in dataclasses.fields(cls))
        for key in tuple(kwargs.keys()):
            if key not in original_fields:
                custom_attributes[key] = kwargs.pop(key)

        original_init(self, *args, build_solph_components=False, **kwargs)

        for key, value in custom_attributes.items():
            setattr(self, key, value)

    cls.__init__ = new_init

    return cls


@facade_adapter
class CommodityAdapter(facades.Commodity, Adapter):
    """CommodityAdapter"""


@facade_adapter
class ConversionAdapter(facades.Conversion, Adapter):
    """ConversionAdapter"""


@facade_adapter
class LoadAdapter(facades.Load, Adapter):
    """LoadAdapter"""


@facade_adapter
class StorageAdapter(facades.Storage, Adapter):
    """StorageAdapter"""


@facade_adapter
class ExtractionTurbineAdapter(facades.ExtractionTurbine, Adapter):
    """ExtractionTurbineAdapter"""

    inputs = ["fuel_bus"]
    outputs = ["electricity_bus", "heat_bus"]

    @classmethod
    def parametrize_dataclass(cls, data: dict, struct, process_type):
        defaults = super().parametrize_dataclass(data, struct, process_type)
        defaults.update({"type": "ExtractionTurbine"})
        return cls(**defaults)


@facade_adapter
class VolatileAdapter(facades.Volatile, Adapter):
    inputs = []
    outputs = ["electricity"]

    @classmethod
    def parametrize_dataclass(cls, data: dict, struct, process_type):
        defaults = super().parametrize_dataclass(data, struct, process_type)
        defaults.update({
            "facade": "volatile",
        })
        return defaults


TYPE_MAP = {
    "commodity": VolatileAdapter,
    "conversion": VolatileAdapter,
    "load": VolatileAdapter,
    "storage": VolatileAdapter,
    "volatile": VolatileAdapter,
    "dispatchable": VolatileAdapter,
    "battery_storage": VolatileAdapter,
}
