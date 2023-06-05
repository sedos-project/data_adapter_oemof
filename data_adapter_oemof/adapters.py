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

    inputs = []
    outputs = ["bus"]

    @classmethod
    def parametrize_dataclass(
        cls, data: dict, timeseries: pandas.DataFrame, struct: dict
    ):
        defaults = cls.get_default_parameters(data, timeseries, struct)
        defaults.update({"type": "Dispatchable"})
        for key, value in {"type": "Dispatchable", "carrier": "carrier", "tech": "tech"}:
            if key not in defaults:
                defaults.update({key: value})
                warnings.warn(f"Facade requires {key} as input. None provided or found within "
                              f"data, proceeding with {value} as {key}")
        #
        return cls(**defaults)



@facade_adapter
class HeatPumpAdapter(facades.HeatPump, Adapter):
    """
    HeatPump Adapter
    """

    inputs = ["electricity_bus", "low_temperature_bus"]
    outputs = ["high_temperature_bus"]

    @classmethod
    def parametrize_dataclass(
        cls, data: dict, timeseries: pandas.DataFrame, struct: dict
    ):
        defaults = cls.get_default_parameters(data, timeseries, struct)
        for key, value in {"type": "HeatPump", "carrier": "carrier", "tech":"tech", "cop":1}:
            if key not in defaults:
                defaults.update({key:value})
                warnings.warn(f"Facade requires {key} as input. None provided or found within "
                              f"data, proceeding with {value} as {key}")
        #
        return cls(**defaults)


@facade_adapter
class Link(facades.Link, Adapter):
    """
    Link Adapter
    """

    inputs = ["from_bus"]
    outputs = ["to_bus"]

    @classmethod
    def parametrize_dataclass(
        cls, data: dict, timeseries: pandas.DataFrame, struct: dict
    ):
        defaults = cls.get_default_parameters(data, timeseries, struct)
        defaults.update({"type": "Link"})
        return cls(**defaults)


@facade_adapter
class ReservoirAdapter(facades.Reservoir, Adapter):
    """
    Reservoir Adapter
    """

    inputs = []
    outputs = ["bus"]

    @classmethod
    def parametrize_dataclass(
        cls, data: dict, timeseries: pandas.DataFrame, struct: dict
    ):
        defaults = cls.get_default_parameters(data, timeseries, struct)
        defaults.update({"type": "Reservoir"})
        for key, value in {"type": "Reservoir", "carrier": 1, "tech":"tech", "efficiency":1, "profile": "profile"}:
            if key not in defaults:
                defaults.update({key:value})
                warnings.warn(f"Facade requires {key} as input. None provided or found within "
                              f"data, proceeding with {value} as {key}")
        return cls(**defaults)


@facade_adapter
class ExcessAdapter(facades.Excess, Adapter):
    """
    Excess Adapter
    """

    inputs = ["Excess"]
    outputs = []

    @classmethod
    def parametrize_dataclass(
        cls, data: dict, timeseries: pandas.DataFrame, struct: dict
    ):
        defaults = cls.get_default_parameters(data, timeseries, struct)
        for key, value in {"type": "Excess", "marginal_cost": 1}:
            if key not in defaults:
                defaults.update({key:value})
                warnings.warn(f"Facade requires {key} as input. None provided or found within "
                              f"data, proceeding with {value} as {key}")
        return cls(**defaults)


@facade_adapter
class BackpressureTurbineAdapter(facades.BackpressureTurbine, Adapter):
    """
    BackpressureTurbine Adapter
    """

    inputs = ["fuel_bus"]
    outputs = ["heat_bus", "electricity_bus"]

    @classmethod
    def parametrize_dataclass(
        cls, data: dict, timeseries: pandas.DataFrame, struct: dict
    ):
        defaults = cls.get_default_parameters(data, timeseries, struct)
        for key, value in {"type": "BackpressureTurbine",
                         "carrier": "carrier",
                         "tech": "tech",
                         "electricity_bus": "electricity_bus",
                         "heat_bus": "heat_bus",
                         "fuel_bus": "fuel_bus",
                         "electric_efficiency": 1,
                         "thermal_efficiency": 1,
                         }:
            if key not in defaults:
                defaults.update({key:value})
                warnings.warn(f"Facade requires {key} as input. None provided or found within "
                              f"data, proceeding with {value} as {key}")
        return cls(**defaults)


@facade_adapter
class CommodityAdapter(facades.Commodity, Adapter):
    """
    CommodityAdapter
    """

    inputs = []
    outputs = ["bus"]

    @classmethod
    def parametrize_dataclass(
        cls, data: dict, timeseries: pandas.DataFrame, struct: dict
    ):
        defaults = cls.get_default_parameters(data, timeseries, struct)
        for key, value in {"type": "Commodity", "carrier": "carrier", "amount": 1}:
            if key not in defaults:
                defaults.update({key:value})
                warnings.warn(f"Facade requires {key} as input. None provided or found within "
                              f"data, proceeding with {value} as {key}")
        return cls(**defaults)


@facade_adapter
class ConversionAdapter(facades.Conversion, Adapter):
    """
    ConversionAdapter
    To use Conversion, map the inputs and outputs within the structure to avoid deduction failure.
    """

    inputs = ["from_bus"]
    outputs = ["to_bus"]

    @classmethod
    def parametrize_dataclass(
        cls, data: dict, timeseries: pandas.DataFrame, struct: dict
    ):
        defaults = cls.get_default_parameters(data, timeseries, struct)
        for key, value in {"type": "Conversion", "carrier": "carrier", "tech": "tech"}:
            if key not in defaults:
                defaults.update({key:value})
                warnings.warn(f"Facade requires {key} as input. None provided or found within "
                              f"data, proceeding with {value} as {key}")
        return cls(**defaults)


@facade_adapter
class LoadAdapter(facades.Load, Adapter):
    """
    LoadAdapter
    """

    inputs = ["bus"]
    outputs = []

    @classmethod
    def parametrize_dataclass(
        cls, data: dict, timeseries: pandas.DataFrame, struct: dict
    ):
        defaults = cls.get_default_parameters(data, timeseries, struct)
        for key, value in {"type": "Load", "bus": "electricity_bus", "amount": 1,"profile": "profile"}:
            if key not in defaults:
                defaults.update({key:value})
                warnings.warn(f"Facade requires {key} as input. None provided or found within "
                              f"data, proceeding with {value} as {key}")
        return cls(**defaults)


@facade_adapter
class StorageAdapter(facades.Storage, Adapter):
    """
    StorageAdapter
    """

    inputs = ["bus"]
    outputs = []

    @classmethod
    def parametrize_dataclass(
        cls, data: dict, timeseries: pandas.DataFrame, struct: dict
    ):
        defaults = cls.get_default_parameters(data, timeseries, struct)
        for key, value in {"type": "Storage", "carrier": "carrier", "tech": "tech"}:
            if key not in defaults:
                defaults.update({key:value})
                warnings.warn(f"Facade requires {key} as input. None provided or found within "
                              f"data, proceeding with {value} as {key}")
        return cls(**defaults)


@facade_adapter
class ExtractionTurbineAdapter(facades.ExtractionTurbine, Adapter):
    """
    ExtractionTurbineAdapter
    """

    inputs = ["fuel_bus"]
    outputs = ["electricity_bus", "heat_bus"]

    @classmethod
    def parametrize_dataclass(
        cls, data: dict, timeseries: pandas.DataFrame, struct: dict
    ):
        defaults = cls.get_default_parameters(data, timeseries, struct)
        for key, value in {"type": "ExtractionTurbine",
                         "carrier": "carrier",
                         "tech": "tech",
                         "electricity_bus": "electricity_bus",
                         "heat_bus": "heat_bus",
                         "fuel_bus": "fuel_bus",
                         "condensing_efficiency": 1,
                         "electric_efficiency": 1,
                         "thermal_efficiency": 1,
                         }:
            if key not in defaults:
                defaults.update({key:value})
                warnings.warn(f"Facade requires {key} as input. None provided or found within "
                              f"data, proceeding with {value} as {key}")

        return cls(**defaults)


@facade_adapter
class VolatileAdapter(facades.Volatile, Adapter):
    inputs = []
    outputs = ["electricity"]

    @classmethod
    def parametrize_dataclass(
        cls, data: dict, timeseries: pandas.DataFrame, struct: dict
    ):
        defaults = cls.get_default_parameters(data, timeseries, struct)
        defaults.update()
        for key, value in {"type": "Volatile", "carrier": "carrier", "tech": "tech", "profile": "profile"}:
            if key not in defaults:
                defaults.update({key:value})
                warnings.warn(f"Facade requires {key} as input. None provided or found within "
                              f"data, proceeding with {value} as {key}")
        return cls(**defaults)


TYPE_MAP = {
    "commodity": CommodityAdapter,
    "conversion": ConversionAdapter,
    "load": LoadAdapter,
    "storage": StorageAdapter,
    "volatile": VolatileAdapter,
    "dispatchable": DispatchableAdapter,
    "battery_storage": StorageAdapter,
}
