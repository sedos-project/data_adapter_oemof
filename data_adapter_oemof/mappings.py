import logging
from pathlib import Path

import yaml

logger = logging.getLogger()


class Mapper:
    def __init__(self, data: dict, mapping=None, busses = None):
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

    def get_bus(self, struct):
        return list(filter(lambda x: "bus" in x, self.mapping))


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

BUS_NAME_MAP = load_yaml(
    Path(__file__).parent / "mappings" / "BUS_NAME_MAP.yaml"
)
