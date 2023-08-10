import dataclasses
import logging

import oemof.solph
import pandas

from oemof.tabular import facades
from oemof.solph import Bus

from data_adapter_oemof import calculations
from data_adapter_oemof.mappings import Mapper


logger = logging.getLogger()


class Adapter:
    type: str = "adapter"
    extra_attributes = ("name", "type", "year")

    def as_dict(self):
        """
        Adds function to return DataFrame from adapter.

        This mixin is needed because `pd.DataFrame(dataclass_instance)` creates columns
         only for existing dataclass attributes.
        But we add custom_attributes (i.e. "name") which would be neglected.
        """
        fields = dataclasses.fields(self)
        data = {}
        for field in fields:
            value = getattr(self, field.name)
            data[field.name] = value
            if isinstance(value, oemof.solph._plumbing._Sequence):
                if value.periodic_values:
                    data[field.name] = value.periodic_values
                elif len(value) != 0:
                    data[field.name] = value.data
                else:
                    data[field.name] = value.default
        # data = {field.name: field_value
        #         for field in fields
        #         if isinstance((field_value := getattr(self, field.name)), oemof.solph._plumbing._Sequence)
        #         }
        for attr in self.extra_attributes:
            data[attr] = getattr(self, attr)
        return data

    @classmethod
    def parametrize_dataclass(
        cls,
        struct,
        mapper: Mapper,
    ) -> "Adapter":
        return cls(**cls.get_default_parameters(struct, mapper))

    @classmethod
    def get_default_parameters(cls, struct: dict, mapper: Mapper) -> dict:
        defaults = {
            "type": cls.type,
        }
        # Add mapped attributes
        mapped_values = mapper.get_default_mappings(struct)
        defaults.update(mapped_values)
        # Add additional attributes
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
class LinkAdapter(facades.Link, Adapter):
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

    type = "extraction_turbine"


@facade_adapter
class VolatileAdapter(facades.Volatile, Adapter):
    """
    VolatileAdapter
    """

    type = "volatile"


# Create a dictionary of all adapter classes defined in this module
FACADE_ADAPTERS = {
    name: adapter for name, adapter in globals().items() if name.endswith("Adapter")
}
