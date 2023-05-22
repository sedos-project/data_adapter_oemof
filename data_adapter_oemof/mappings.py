import dataclasses
import logging
import warnings
from pathlib import Path
import difflib
import yaml

logger = logging.getLogger()


class Mapper:
    def __init__(self, data: dict, mapping=None, bus_map=None):
        if mapping is None:
            mapping = GLOBAL_PARAMETER_MAP
        if bus_map is None:
            bus_map = BUS_NAME_MAP
        self.data = data
        self.mapping = mapping
        self.bus_map = bus_map

    def get(self, key):
        if key in self.mapping:
            mapped_key = self.mapping[key]
            logger.info(f"Mapped '{key}' to '{mapped_key}'")
        else:
            mapped_key = key
            logger.info(f"Key not found. Did not map '{key}'")

        if mapped_key not in self.data:
            logger.warning(f"Could not get data for mapped key '{mapped_key}'")
            return None
        return self.data[mapped_key]

    @staticmethod
    def __get_default_busses(bus, struct, cls):
        """
        This Function checks the FacadeAdapters entries for inputs/outputs

        The Dataclass `bus category` variables `inputs` and `outputs` are searched for `bus`
        If there's only one "bus category":
         the "bus" will be assigned to the "category" entry from the "structure."

        :param bus:str
        :param struct:dict
        :param cls:adapters.Adapter
        :return:dict | None
        """

        bus_category = (
            "inputs"
            if bus in cls.inputs
            else ("outputs" if bus in cls.outputs else None)
        )
        if bus_category is None:
            warnings.warn(
                f"Bus {bus} not found within the Adapter {cls.__name__} inputs or output variables"
            )
            return None

        if len(getattr(cls, bus_category)) == 1:
            if struct[bus_category]:
                return {bus: struct[bus_category][0]}
            else:
                try:
                    return {bus: struct[bus_category][0]}
                except ValueError or IndexError:
                    warnings.warn(
                        f"Bus Category {bus_category} not found in struct. Check {cls.__name__}"
                        f"and structure.csv"
                    )
                    return None

        else:
            return None

    def get_busses(self, cls, struct):
        """
        Identify mentioned buses in the facade.
        Determine if each bus in the facade is classified as an "input"/"output".
        If there is only one entry in the adapter for the facade's bus category:
         Select the first entry from the structure CSV __get_default_busses(bus, struct, cls).
        If there are multiple entries:
         first check if there is a corresponding name in the BUS_NAME_MAP.
            If found, search for similarities in the structure CSV.
            If not, search for name similarities:
                Between the structure CSV and the adapter's buses take name from the structure.

        :param cls: Child from Adapter class
        :param struct: dict
        :return: dictionary with tabular like Busses
        """

        busses_in_Adapter = cls.inputs + cls.outputs

        if len(busses_in_Adapter) == 0:
            logger.warning(f"No busses found in facades Adapter {cls.__name__}")

        bus_dict = {}
        for bus in busses_in_Adapter:
            match = self.__get_default_busses(bus, struct, cls)
            if match is not None:
                bus_dict.update(match)
                continue
            else:
                bus_category = "inputs" if bus in cls.inputs else "outputs"
                # If there are is more than one bus and DEFAULTS dont match lookup in BUS_NAME_MAP
                # Check if this bus is mentioned in BUS_NAME_MAP:
                if bus in self.bus_map[cls.__name__]:
                    name = self.bus_map[cls.__name__][bus]
                else:
                    name = bus

                match = difflib.get_close_matches(
                    name, struct[bus_category], n=1, cutoff=0.2
                )[0]

                if not match:
                    logger.warning(
                        f"No Matching bus found for bus with `bus facade name` {bus}"
                        f" please adjust BUS_NAME_MAP or structure"
                    )
                    continue
                bus_dict.update({bus: match})

        return bus_dict

    def get_default_mappings(self, cls, struct):
        """
        :param struct: dict
        :param cls: Data-adapter which is inheriting from oemof.tabular facade
        :param mapper: Mapper to map oemof.tabular data names to Project naming
        :return: Dictionary for all fields that the facade can take and matching data
        """
        mapped_all_class_fields = {
            field.name: value
            for field in dataclasses.fields(cls)
            if (value := self.get(field.name)) is not None
        }
        mapped_all_class_fields.update(self.get_busses(cls, struct))
        return mapped_all_class_fields


def load_yaml(file_path):
    with open(file_path, "r", encoding="UTF-8") as file:
        dictionary = yaml.load(file, Loader=yaml.FullLoader)

    return dictionary


TYPE_MAP = {
    "volatile": "",
}

CARRIER_MAP = {
    "electricity": "",
}

TECH_MAP = {
    "onshore": "",
}

# Maps from oemof.tabular parameter names
# to ontological terms or to sedos nomenclature as fallback option
GLOBAL_PARAMETER_MAP = load_yaml(
    Path(__file__).parent / "mappings" / "GLOBAL_PARAMETER_MAP.yaml"
)

PROCESS_TYPE_MAP = load_yaml(
    Path(__file__).parent / "mappings" / "PROCESS_TYPE_MAP.yaml"
)

BUS_NAME_MAP = load_yaml(Path(__file__).parent / "mappings" / "BUS_NAME_MAP.yaml")
