import collections
import dataclasses
import difflib
import logging
import warnings
from typing import Optional, Type, Union

import pandas as pd
from oemof.tabular import facades
from oemof.tabular._facade import Facade

from data_adapter_oemof import calculations

logger = logging.getLogger()


DEFAULT_MAPPING = {
    "carrier": "carrier",
    "tech": "tech",
}

Field = collections.namedtuple(typename="Field", field_names=["name", "type"])


class MappingError(Exception):
    """Raised if mapping fails"""


class Adapter:
    type: str = "adapter"
    facade: Union[Facade, dataclasses.dataclass] = None
    extra_fields = (
        Field(name="name", type=str),
        Field(name="region", type=str),
        Field(name="year", type=int),
    )
    counter: int = collections.Counter()

    def __init__(
        self,
        process_name: str,
        data: dict,
        timeseries: pd.DataFrame,
        structure: dict,
        parameter_map: dict,
        bus_map: dict,
    ):
        self.process_name = process_name
        self.data = data
        self.timeseries = timeseries
        self.structure = structure
        self.parameter_map = parameter_map
        self.bus_map = bus_map
        self.facade_dict = self.get_default_parameters()

    def get_default_parameters(self) -> dict:
        defaults = {"type": self.type}
        # Add mapped attributes
        mapped_values = self.get_default_mappings()
        defaults.update(mapped_values)

        # add name if found in data, else use calculation for name:
        if (name := self.get("name")) is not None:
            defaults.update({"name": name})
        else:
            defaults.update(
                {
                    "name": calculations.get_name(
                        self.get("region"),
                        self.get("carrier"),
                        self.get("tech"),
                        self.counter,
                    )
                }
            )

        return defaults

    def get_fields(self) -> list[Field]:
        return [
            Field(name=field.name, type=field.type)
            for field in dataclasses.fields(self.facade)
        ] + list(self.extra_fields)

    def map_key(self, key):
        """Use adapter specific mapping if available, otherwise use default
        mapping or return key if no mapping is available.

        :param key: str
            key to be mapped
        :return: str
            mapped key
        """
        # 1.Check process-specific mappings first
        if (
            self.process_name in self.parameter_map
            and key in self.parameter_map[self.process_name]
        ):
            return self.parameter_map[self.process_name][key]

        # 2. Check adapter-specific mappings second
        if (
            self.__class__.__name__ in self.parameter_map
            and key in self.parameter_map[self.__class__.__name__]
        ):
            return self.parameter_map[self.__class__.__name__][key]

        # 3. Check facade-specific mappings third
        if (
            self.facade.__name__ in self.parameter_map
            and key in self.parameter_map[self.facade.__name__]
        ):
            return self.parameter_map[self.facade.__name__][key]

        # 4. Check default mappings fourth
        if key in self.parameter_map.get("DEFAULT", []):
            return self.parameter_map["DEFAULT"][key]

        # 5. Use key if no mapping available
        logger.debug(f"Key not found. Did not map '{key}'")
        return key

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
            if len(self.timeseries.columns) == 1:
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
        logger.debug(
            f"No {key} data in {self.process_name} as a {self.__class__.__name__}"
        )
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

    def get_busses(self):
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
        Note: If passed class has more than two busses or different names for busses fields it
        is highly recommended to provide BUS_NAME_MAP entry for this class.
        If multiple instances of the same facade shall be having different inputs/outputs
        a facade Adapter has to be added for each.
        :return: dictionary with tabular like Busses
        """
        bus_occurrences_in_fields = [
            field.name for field in self.get_fields() if "bus" in field.name
        ]
        if len(bus_occurrences_in_fields) == 0:
            logger.warning(
                f"No busses found in facades fields for Dataadapter {self.__class__.__name__}"
            )

        bus_dict = {}
        for bus in bus_occurrences_in_fields:  # emission_bus
            # 1. Check for existing mappings
            try:
                bus_dict[bus] = self.bus_map[self.__class__.__name__][bus]
                continue
            except KeyError:
                pass

            # TODO: Make use of Parameter [stuct.csv]?
            # Do we need parameter specific Bus structure? Maybe for multiple in/output?
            if len(self.structure.keys()) == 1:
                struct = list(self.structure.values())[0]
            elif "default" in self.structure.keys():
                struct = self.structure["default"]
            else:
                struct = self.structure
                warnings.warn(
                    "Please check structure and provide either one set of inputs/outputs "
                    "or specify as default Parameter specific busses not implemented yet. "
                    f"No Bus found for Process {self.process_name} in Adapter {self}"
                )

            # 2. Check for default busses
            if bus in ("bus", "from_bus", "to_bus", "fuel_bus"):
                if bus == "bus":
                    busses = struct["inputs"] + struct["outputs"]
                if bus in ("from_bus", "fuel_bus"):
                    busses = struct["inputs"]
                if bus == "to_bus":
                    busses = struct["outputs"]
                if len(busses) != 1:
                    raise MappingError(
                        f"Could not map {bus} to default bus - too many options"
                    )
                bus_dict[bus] = busses[0]
                continue

            # 3. Try to find close matches
            match = difflib.get_close_matches(
                bus,
                struct["inputs"] + struct["outputs"],
                n=1,
                cutoff=0.2,
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

    def get_default_mappings(self):
        """
        :return: Dictionary for all fields that the facade can take and matching data
        """

        mapped_all_class_fields = {
            field.name: value
            for field in self.get_fields()
            if (value := self.get(field.name, field.type)) is not None
        }
        mapped_all_class_fields.update(self.get_busses())
        return mapped_all_class_fields

    @staticmethod
    def is_sequence(field_type: Type):
        # TODO: Implement it using typing hints
        return "Sequence" in str(field_type)


class DispatchableAdapter(Adapter):
    """
    Dispatchable Adapter
    """

    type = "dispatchable"
    facade = facades.Dispatchable


class HeatPumpAdapter(Adapter):
    """
    HeatPump Adapter
    """

    type = "heat_pump"
    facade = facades.HeatPump


class LinkAdapter(Adapter):
    """
    Link Adapter
    """

    type = "link"
    facade = facades.Link


class ReservoirAdapter(Adapter):
    """
    Reservoir Adapter
    """

    type = "reservoir"
    facade = facades.Reservoir


class ExcessAdapter(Adapter):
    """
    Excess Adapter
    """

    type = "excess"
    facade = facades.Excess


class BackpressureTurbineAdapter(Adapter):
    """
    BackpressureTurbine Adapter
    """

    type = "backpressure_turbine"
    facade = facades.BackpressureTurbine


class CommodityAdapter(Adapter):
    """
    CommodityAdapter
    """

    type = "commodity"
    facade = facades.Commodity

    def get_default_parameters(
        self,
    ) -> dict:
        defaults = super().get_default_parameters()
        if self.get("carrier") == "carrier":
            defaults["carrier"] = self.get_busses()["bus"]

        return defaults


class ConversionAdapter(Adapter):
    """
    ConversionAdapter
    To use Conversion, map the inputs and outputs within the structure to avoid deduction failure.
    """

    type = "conversion"
    facade = facades.Conversion


class LoadAdapter(Adapter):
    """
    LoadAdapter
    """

    type = "load"
    facade = facades.Load


class StorageAdapter(Adapter):
    """
    StorageAdapter
    """

    type = "storage"
    facade = facades.Storage
    extra_fields = Adapter.extra_fields + (
        Field(name="invest_relation_output_capacity", type=float),
        Field(name="inflow_conversion_factor", type=float),
        Field(name="outflow_conversion_factor", type=float),
    )


class ExtractionTurbineAdapter(Adapter):
    """
    ExtractionTurbineAdapter
    """

    type = "extraction_turbine"
    facade = facades.ExtractionTurbine


class VolatileAdapter(Adapter):
    """
    VolatileAdapter
    """

    type = "volatile"
    facade = facades.Volatile


# Create a dictionary of all adapter classes defined in this module
FACADE_ADAPTERS = {
    name: adapter for name, adapter in globals().items() if name.endswith("Adapter")
}
