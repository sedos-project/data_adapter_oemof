import dataclasses
import logging

from oemof.tabular import facades
from oemof.solph import Bus

from data_adapter_oemof import calculations
from data_adapter_oemof.mappings import Mapper

# TODO: Build all dataadapters

logger = logging.getLogger()


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
    """
    :param cls: Data-adapter which is inheriting from oemof.tabular facade
    :param mapper: Mapper to map oemof.tabular data names to Project naming
    :return: Dictionary for all fields that the facade can take and matching data
    """
    mapped_all_class_fields = {
        field.name: mapper.get(field.name) for field in dataclasses.fields(cls)
    }
    return mapped_all_class_fields


def get_busses(cls, struct, one_bus_from_struct: str = "outputs"):
    """
    Get the busses a facade can take and which are found in es structure Processes can either have:
        - one bus (from_bus and to_bus bus are the same OR only one from_bus/to_bus bus is there)
        - two busses (from_bus bus is not the same as to_bus bus)
    Special cases where multiple busses are occurring will be caught in their facades, such as:
        - n:1
        - 1:n
        - n:j
    :param cls: Data-adapter which is inheriting from oemof.tabular facade
    :param struct: struct from data_adapter.get_struct
    :return: Dict: facade specific correct bus names as keys and connected busses as value
    """
    # TODO: decide what should happen with multiple i/o
    bus_occurrences_in_fields = {
        field.name for field in dataclasses.fields(cls) if "bus" in field.name
    }
    bus_dict = {}
    if "bus" in bus_occurrences_in_fields and len(bus_occurrences_in_fields) == 1:
        bus_dict["bus"] = struct["default"][one_bus_from_struct][0]
    elif (
        len(bus_occurrences_in_fields) == 2
        and "from_bus" in bus_occurrences_in_fields
        and "to_bus" in bus_occurrences_in_fields
    ):
        bus_dict["from_bus"] = struct["default"]["inputs"][0]
        bus_dict["to_bus"] = struct["default"]["outputs"][0]
    else:
        logger.warning(
            f"There is no valid number of from/to busses for {dataclasses.fields(cls)}'"
        )
        return None
    return bus_dict


@facade_adapter
class CommodityAdapter(facades.Commodity):
    @classmethod
    def parametrize_dataclass(cls, data: dict, struct):
        mapper = Mapper(data)
        defaults = get_default_mappings(cls, mapper)
        busses = get_busses(cls, struct)
        defaults.update(busses)
        attributes = {
            "name": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            "type": "something"
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
        busses = get_busses(cls, struct)
        defaults.update(busses)

        # TODO: n:j conversion facades in allen anderen facades wird nur "default" benutzt?
        #  Bw. kann n:j facade nicht auch 1:1 sein
        #  (wenn wir sowieso eine facade bauen müssen die das kann..?)
        attributes = {
            "name": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            # TODO: capacity costs berechnen wie? -> erstmal nicht machen?
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
        defaults = get_default_mappings(cls, mapper=mapper)
        busses = get_busses(cls, struct)
        defaults.update(busses)
        attributes = {
            "name": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            "bus": struct["default"]["inputs"][0]
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
        busses = get_busses(cls, struct)
        defaults.update(busses)
        attributes = {
            "name": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            # TODO: decide where such calculations shall be made -> mapper?
            #  Essentially when there are multiple ways to obtain "capacity_cost"
            #  (either by calculation or if it is already existing in dataset)
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
        busses = get_busses(cls, struct)
        defaults.update(busses)
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
