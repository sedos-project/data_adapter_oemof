import dataclasses
import logging
import warnings
from pathlib import Path
from difflib import SequenceMatcher
import difflib
import yaml

logger = logging.getLogger()


class Mapper:
    def __init__(self, data: dict, mapping=None, busses=None):
        if mapping is None:
            mapping = GLOBAL_PARAMETER_MAP
        if busses is None:
            busses = BUS_NAME_MAP
        self.data = data
        self.mapping = mapping
        self.bus_map = busses

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

    def get_busses(self, cls, struct):
        """
        Getting the busses for process from structure
        :param cls: DataAdapter (children) class
        :param struct: dict
        :return: dictionary with tabular like Busses
        """
        bus_occurrences_in_fields = [
            field.name for field in dataclasses.fields(cls) if "bus" in field.name
        ]
        if len(bus_occurrences_in_fields) == 0:
            logger.warning(f"No busses found in fields for Dataadapter {cls.__name__}")
        bus_dict = {}
        for bus in bus_occurrences_in_fields:
            name = self.bus_map[cls.__name__][bus]["name"]
            category = self.bus_map[cls.__name__][bus]["category"]

            # If default is in busmap rule is to take teh first entry in mentioned category
            if name == "default":
                match = struct["default"][category][0]

            else:
                match = difflib.get_close_matches(
                    name, struct["default"][category], n=1, cutoff=0.2
                )[0]

            if not match:
                logger.warning(f"No Matching bsu found for ")
                continue
            bus_dict[bus] = match

        return bus_dict


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
