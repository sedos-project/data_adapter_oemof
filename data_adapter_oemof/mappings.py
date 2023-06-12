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

    def map_key(self, key):
        """Use adapter specific mapping if available, otherwise use default
        mapping or return key if no mapping is available.

        :param key: str
            key to be mapped
        :return: str
            mapped key
        """
        # 1.Check facade-specific mappings first
        if (
            self.adapter.__name__ in self.mapping
            and key in self.mapping[self.adapter.__name__]
        ):
            mapped_key = self.mapping[self.adapter.__name__][key]
            logger.info(f"Mapped '{key}' to '{mapped_key}'")

        # 2 Check default mappings second
        elif key in self.mapping.get("DEFAULT", []):
            mapped_key = self.mapping["DEFAULT"][key]
            logger.info(f"Mapped '{key}' to '{mapped_key}'")
        # 3 Use key if no mapping available
        else:
            mapped_key = key
            logger.warning(f"Key not found. Did not map '{key}'")
        return mapped_key

    def get_data(self, key, field_type: Optional[Type] = None):
        """
        Get data for key either from scalar data or timeseries data. Return
        None if no data is available.

        :param key: str
        :param field_type: Type
            Type of data field. Used to determine if key is a timeseries.
        :return: str, numerical or None
            Data for key or column name of timeseries
        """

        # 1.1 Check if mapped key is in scalar data
        if key in self.data:
            return self.data[key]

        # 1.2 Check if mapped key is in timeseries data
        if self.is_sequence(field_type):
            # 1.2.1 Take key_region if exists
            key = f"{key}_{self.get_data('region')}"
            if key in self.timeseries.columns:
                return key
            # 1.2.2 Take column name if only one time series is available
            if len(self.timeseries) == 1:
                timeseries_key = self.timeseries.columns[0]
                logger.info(
                    "Key not found in timeseries. "
                    f"Using existing timeseries column '{timeseries_key}'."
                )
                return timeseries_key
            logger.warning(f"Could not find timeseries entry for mapped key '{key}'")
            return None

        # 2 Use defaults
        if key in DEFAULT_MAPPING:
            return DEFAULT_MAPPING[key]

        # 3 Return None if no data is available
        logger.warning(f"Could not get data for mapped key '{key}'")
        return None

    def get(self, key, field_type: Optional[Type] = None):
        """
        Map key with adapter specific mapping and return data for key if
        available.

        :param key: str
            Name of data field
        :param field_type: Type
            Type of data field. Used to determine if key is a timeseries.
        :return: str, numerical or None
        """
        mapped_key = self.map_key(key)
        return self.get_data(mapped_key, field_type)

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
