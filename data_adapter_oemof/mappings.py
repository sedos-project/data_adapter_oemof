import dataclasses
import difflib
import logging
from pathlib import Path
from typing import Optional, Type

import pandas
import yaml

logger = logging.getLogger()

DEFAULT_MAPPING = {
    "carrier": "carrier",
    "tech": "tech",
}


class MappingError(Exception):
    """Raised if mapping fails"""


class Mapper:
    def __init__(
        self,
        adapter,
        data: dict,
        timeseries: pandas.DataFrame,
        mapping=None,
        bus_map=None,
    ):
        if mapping is None:
            mapping = GLOBAL_PARAMETER_MAP
        if bus_map is None:
            bus_map = BUS_NAME_MAP
        self.adapter = adapter
        self.data = data
        self.timeseries = timeseries
        self.mapping = mapping
        self.bus_map = bus_map

    def get(self, key, field_type: Optional[Type] = None):
        # Map keys
        # Check facade-specific mappings first
        if (
            self.adapter.__name__ in self.mapping
            and key in self.mapping[self.adapter.__name__]
        ):
            mapped_key = self.mapping[self.adapter.__name__][key]
            logger.info(f"Mapped '{key}' to '{mapped_key}'")

        # Check default mappings second
        elif key in self.mapping.get("DEFAULT", []):
            mapped_key = self.mapping["DEFAULT"][key]
            logger.info(f"Mapped '{key}' to '{mapped_key}'")
        # Use key if no mapping available
        else:
            mapped_key = key
            logger.info(f"Key not found. Did not map '{key}'")

        # Look for data
        # Check if mapped key is scalar
        if mapped_key in self.data:
            return self.data[mapped_key]

        # Check if mapped key is timeseries
        if self.is_sequence(field_type):
            mapped_key = f"{mapped_key}_{self.get('region')}"
            if mapped_key in self.timeseries.columns:
                return mapped_key
            if len(self.timeseries) == 1:
                timeseries_key = self.timeseries.columns[0]
                logger.info(
                    "Key not found in timeseries. "
                    f"Using existing timeseries column '{timeseries_key}'."
                )
                return timeseries_key
            logger.warning(
                f"Could not find timeseries entry for mapped key '{mapped_key}'"
            )
            return None

        if key in DEFAULT_MAPPING:
            return DEFAULT_MAPPING[key]

        logger.warning(f"Could not get data for mapped key '{mapped_key}'")
        mapped_key = key
        logger.info(f"Key not found. Did not map '{key}'")
        # return None
        return mapped_key

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
        bus_occurrences_in_fields = [
            field.name for field in dataclasses.fields(cls) if "bus" in field.name
        ]
        if len(bus_occurrences_in_fields) == 0:
            logger.warning(
                f"No busses found in facades fields for Dataadapter {cls.__name__}"
            )

        bus_dict = {}
        for bus in bus_occurrences_in_fields:
            # 1. Check for existing mappings
            try:
                bus_dict[bus] = self.bus_map[cls.__name__][bus]
                continue
            except KeyError:
                pass

            # 2. Check for default busses
            if bus in ("bus", "from_bus", "to_bus"):
                if bus == "bus":
                    busses = struct["inputs"] + struct["outputs"]
                if bus == "from_bus":
                    busses = struct["inputs"]
                if bus == "to_bus":
                    busses = struct["outputs"]
                if len(busses) != 1:
                    raise MappingError(
                        f"Could not map {bus=} to default bus - too many options"
                    )
                bus_dict[bus] = busses[0]
                continue

            # 3. Try to find closes match
            match = difflib.get_close_matches(
                bus, struct["inputs"] + struct["outputs"], n=1, cutoff=0.2
            )[0]
            if match:
                bus_dict[bus] = match
                continue

            # 4. No mapping found
            raise MappingError(
                f"No Matching bus found for bus with `bus facade name` {bus}"
                f" please adjust BUS_NAME_MAP or structure"
            )

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
            if (value := self.get(field.name, field.type)) is not None
        }
        mapped_all_class_fields.update(self.get_busses(cls, struct))
        return mapped_all_class_fields

    @staticmethod
    def is_sequence(field_type: Type):
        # TODO: Implement it using typing hints
        return "Sequence" in str(field_type)


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
