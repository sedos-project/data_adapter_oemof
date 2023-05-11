import dataclasses
import logging
from pathlib import Path
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
        Get busses by:
            - Finding all busses in facade
            - Checking if they're input/output in Adapter
            - If only one entry for bus category:
              - Use first entry from structure csv
            - Else (if multiple entries):
              - Check BUS_NAME_MAP for name similarity, else
              - Search for name similarities in csv and adapter, use name from csv.
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
            name = self.bus_map[cls.__name__][bus]
            category = "inputs" if bus in cls.inputs else "outputs"
            category_busses = cls.__dict__[category]

            if len(category_busses) == 0:
                logger.warning(
                    f"The bus {bus} in facade's field is not in Adapter {cls.__name__}"
                )
            elif len(category_busses) == 1:
                match = struct[list(struct.keys())[0]][category][0]
            else:
                match = difflib.get_close_matches(
                    name, struct[list(struct.keys())[0]][category], n=1, cutoff=0.2
                )[0]
            if not match:
                logger.warning(
                    f"No Matching bus found for bus {bus}"
                    f"Please adjust BUS_NAME_MAP or structure to match the facade name for the bus."
                )
                continue
            bus_dict[bus] = match

        return bus_dict

    def get_default_mappings(self, cls, struct):
        """
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
