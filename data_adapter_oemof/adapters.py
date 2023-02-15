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
            "name": calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            "capacity_cost": calculations.get_capacity_cost(**{"mapper":mapper}),
        }
        defaults.update(attributes)
        print(defaults)
        return cls(**defaults)

        # Hier fallen diejenigen attribute wieder aus heraus, die nich teil der facade sind -> das sind vorallem 'name' und 'type'

        # Der nutzen der klasse hier ist (wie ich das verstanden habe) ein check ob die daten types stimmen und ob die
        # minimalen parameter übergeben wurden um die facade nachher zu bauen.

        # Lösungsvorschlag: Statt der klasse geben wir ein dictionary (oder Dataframe) zurück,
        # der die Facade csv enthält und prüfen die intefrität der facade bevor die attributes hinzugefügt werden.

        # Außerdem:
        #   Douplicate code: Jede Adapterklasse besteht im moment aus den gleichen aufrufen -> TypeMap umschreiben und
        #   dort auf die tabular.facades mappen. Die kann dann an eine allgemeine funktion (oder klasse, was ist hier
        #   sinnvoller?) übergeben werden.

        #Todo:
        # def adapter(data, type_map_call, type_map):
        #     cls = type_map[type_map_call]
        #     mapper = Mapper(data)
        #     defaults = get_default_mappings(cls, mapper)
        #     attributes = {
        #         "name": calculations.get_name(
        #             mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
        #         ),
        #         "capacity_cost": calculations.get_capacity_cost(**{"mapper": mapper}),
        #         "type": type_map_call
        #     }
        #     defaults.update(attributes)
        #     def check_integrity():
        #         cls_attrs = defaults.update({"build_solph_components":False})
        #         return cls(**cls_attrs)
        #     return defaults


TYPE_MAP = {
    "commodity": CommodityAdapter,
    "conversion": ConversionAdapter,
    "load": LoadAdapter,
    "storage": VolatileAdapter,
    "volatile": VolatileAdapter,
    "dispatchable": VolatileAdapter,
    "battery_storage": StorageAdapter
}
