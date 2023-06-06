import dataclasses
import logging
import warnings

import pandas

from oemof.tabular import facades
from oemof.solph import Bus

from data_adapter_oemof import calculations
from data_adapter_oemof.mappings import Mapper


logger = logging.getLogger()


class Adapter:
    type: str = "adapter"
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
    def parametrize_dataclass(
        cls, data: dict, timeseries: pandas.DataFrame, struct
    ) -> "Adapter":
        return cls(**cls.get_default_parameters(data, timeseries, struct))

    @classmethod
    def get_default_parameters(
        cls, data: dict, timeseries: pandas.DataFrame, struct: dict
    ) -> dict:
        mapper = Mapper(data, timeseries)
        defaults = mapper.get_default_mappings(cls, struct)
        attributes = {
            "name": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            "region": mapper.get("region"),
            "year": mapper.get("year"),
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
class DispatchableAdapter(facades.Dispatchable, Adapter):
    """
    Dispatchable Adapter
    """
    type = "dispatchable"


@facade_adapter
class HeatPumpAdapter(facades.HeatPump, Adapter):
    """
    HeatPump Adapter
    """
    type = "heat_pump"


@facade_adapter
class Link(facades.Link, Adapter):
    """
    Link Adapter
    """
    type = "link"


@facade_adapter
class ReservoirAdapter(facades.Reservoir, Adapter):
    """
    Reservoir Adapter
    """
    type = "reservoir"


@facade_adapter
class ExcessAdapter(facades.Excess, Adapter):
    """
    Excess Adapter
    """
    type = "excess"


@facade_adapter
class BackpressureTurbineAdapter(facades.BackpressureTurbine, Adapter):
    """
    BackpressureTurbine Adapter
    """
    type = "backpressure_turbine"


@facade_adapter
class CommodityAdapter(facades.Commodity, Adapter):
    """
    CommodityAdapter
    """
    type = "commodity"


@facade_adapter
class ConversionAdapter(facades.Conversion, Adapter):
    """
    ConversionAdapter
    To use Conversion, map the inputs and outputs within the structure to avoid deduction failure.
    """
    type = "conversion"


@facade_adapter
class LoadAdapter(facades.Load, Adapter):
    """
    LoadAdapter
    """
    type = "load"


@facade_adapter
class StorageAdapter(facades.Storage, Adapter):
    """
    StorageAdapter
    """
    type = "storage"


@facade_adapter
class ExtractionTurbineAdapter(facades.ExtractionTurbine, Adapter):
    """
    ExtractionTurbineAdapter
    """
    type = "extraction_trubine"


@facade_adapter
class VolatileAdapter(facades.Volatile, Adapter):
    """
    VolatileAdapter
    """
    type = "volatile"


TYPE_MAP = {
    "commodity": CommodityAdapter,
    "conversion": ConversionAdapter,
    "load": LoadAdapter,
    "storage": StorageAdapter,
    "volatile": VolatileAdapter,
    "dispatchable": DispatchableAdapter,
    "battery_storage": StorageAdapter,
}
