import dataclasses
import logging

from oemof.tabular import facades
from oemof.solph import Bus

from data_adapter_oemof import calculations
from data_adapter_oemof.mappings import Mapper

logger = logging.getLogger()


# Todo: Build all dataadapters, build
# Todo: Add Timeseries adapter


class AdapterToDataFrameMixin:
    extra_attributes = ("name", "type")
    """
    Adds function to return DataFrame from adapter.

    This mixin is necessary as `pd.DataFrame(dataclass_instance)` will only create columns for attributes already present in dataclass.
    But we add custom_attributes (i.e. "name") which would be neglected.
    """

    def as_dict(self):
        fields = dataclasses.fields(self)
        data = {field.name: getattr(self, field.name) for field in fields}
        for attr in self.extra_attributes:
            data[attr] = getattr(self, attr)
        return data

    @classmethod
    def default_parametrize_dataclass(cls, data: dict, struct, process_type):
        mapper = Mapper(data)
        defaults = get_default_mappings(cls, mapper)
        busses = mapper.get_busses(cls, struct)
        defaults.update(busses)
        attributes = {
            "name": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
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


def get_default_mappings(cls, mapper):
    """
    :param cls: Data-adapter which is inheriting from oemof.tabular facade
    :param mapper: Mapper to map oemof.tabular data names to Project naming
    :return: Dictionary for all fields that the facade can take and matching data
    """
    mapped_all_class_fields = {
        field.name: mapper.get(field.name) for field in dataclasses.fields(cls)
    }
    return mapped_all_class_fields


@facade_adapter
class CommodityAdapter(facades.Commodity, AdapterToDataFrameMixin):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@facade_adapter
class ConversionAdapter(facades.Conversion, AdapterToDataFrameMixin):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@facade_adapter
class LoadAdapter(facades.Load, AdapterToDataFrameMixin):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@facade_adapter
class StorageAdapter(facades.Storage, AdapterToDataFrameMixin):
    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@facade_adapter
class VolatileAdapter(facades.Volatile, AdapterToDataFrameMixin):
    @classmethod
    def parametrize_dataclass(cls, data: dict, struct, process_type):
        defaults = super(VolatileAdapter, cls).default_parametrize_dataclass(
            data, struct, process_type
        )
        defaults.update({"type": "volatile"})
        return cls(**defaults)


TYPE_MAP = {
    "commodity": VolatileAdapter,
    "conversion": VolatileAdapter,
    "load": VolatileAdapter,
    "storage": VolatileAdapter,
    "volatile": VolatileAdapter,
    "dispatchable": VolatileAdapter,
    "battery_storage": VolatileAdapter,
}
