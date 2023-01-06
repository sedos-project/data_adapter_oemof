import logging
from pathlib import Path
import yaml

logger = logging.getLogger()


class Mapper:
    def __init__(self, data, mapping=None):
        if mapping is None:
            mapping = GLOBAL_PARAMETER_MAP
        self.data = data
        self.mapping = mapping

    def get(self, key):
        if key in self.mapping:
            mapped_key = self.mapping[key]
            logger.info(f"Mapped '{key}' to '{mapped_key}'")
        else:
            mapped_key = key
            logger.info(f"Key not found. Did not map '{key}'")

        if mapped_key not in self.data:
            logger.warning("Could not get key")
            return None

        return self.data[mapped_key]


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
filepath = Path(__file__).parent / "mappings" / "GLOBAL_PARAMETER_MAP.yaml"
GLOBAL_PARAMETER_MAP = load_yaml(filepath)

print(GLOBAL_PARAMETER_MAP)
