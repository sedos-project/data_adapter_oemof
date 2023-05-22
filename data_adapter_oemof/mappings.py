import dataclasses
import logging
import warnings
from pathlib import Path
from difflib import SequenceMatcher
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
    def __get_default_busses(bus_occurrences_in_fields, bus_dict, bus, struct, cls):
       """
       This Function checks the FacadeAdapters entries for inputs/outputs

       The Dataclass `bus category` variables `inputs` and `outputs` are searched for `bus`
       If there is only one "category bus," the "bus" will be assigned to the "category" entry from the "structure."

       :param bus_occurrences_in_fields:
       :param bus_dict:
       :param bus:
       :param struct:
       :param cls:
       :return:
       """

       if len(bus_occurrences_in_fields) == 1:
            if struct["inputs"]:
                match = struct["inputs"][0]
                bus_dict[bus] = match
                return match
            elif struct["outputs"]:
                match = struct["outputs"][0]
                bus_dict[bus] = match
                return match
            else:
                warnings.warn(
                    f"Please provide explicit bus for {cls.__name__} in structure"
                )
       elif len(bus_occurrences_in_fields) == 2 and all(
                [i in "from_bus", "to_bus"] for i in bus_occurrences_in_fields
        ):
            return {
                "from_bus": struct["inputs"][0],
                "to_bus": struct["outputs"][0],
            }

        # Category for busses is either input or output bus.
       category = (
            "inputs"
            if (bus in cls.inputs or bus in ["from_bus", "bus"])
            else "outputs"
        )

        # Category busses are busses that are found within the category where the bus originates from
        category_busses = cls.__dict__[category]

        if len(category_busses) == 0:
            logger.warning(
                f"The bus {bus} in facade's field is not in Adapter {cls.__name__}"
            )
        elif len(category_busses) == 1:
            match = struct[category][0]

        return match


    def get_busses(self, cls, struct):
        """
        Getting Busses after following rules
            - Searching for all busses mentioned in facade
            - for all Busses in the Facade check if they are input or output -> should be Attribute of Adapter
            - If for the facades bus category ( input or output) there is only one Entry in the Adapter:
                - Take (first) Entry from structure csv
            - Else (if there are more than one entries)
                - First check if there is a Name in BUS_NAME_MAP given for the Bus, if yes search for similarities in
                strucutre csv
                - If not: Search for similarities in names in Structure csv and Adapters busses -> take name from struct

        :param cls: Child from Adapter class
        :param struct: dict
        :return: dictionary with tabular like Busses
        """




        bus_occurrences_in_fields = [
            field.name for field in dataclasses.fields(cls) if "bus" in field.name
        ]
        if len(bus_occurrences_in_fields) == 0:
            logger.warning(
                f"No busses found in facades fields for Dataadapter {cls.__name__}"
            )

        bus_dict = {}
        for bus in bus_occurrences_in_fields:
            if self.__get_default_busses()
            # If there are is more than one bus and DEFAULTS dont match lookup in BUS_NAME_MAP
            else:
                # Check if this bus is mentioned in BUS_NAME_MAP:
                if bus in self.bus_map[cls.__name__]:
                    name = self.bus_map[cls.__name__][bus]
                else:
                    name = bus
                match = difflib.get_close_matches(
                    name, struct[category], n=1, cutoff=0.2
                )[0]
            if not match:
                logger.warning(
                    f"No Matching bus found for bus {bus}"
                    f"which is a the facade name for the bus, please try to adjust BUS_NAME_MAP or structure"
                )
                continue
            bus_dict[bus] = match

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
