import logging

from oemof.tabular import facades
from oemof.tabular._facade import Facade

from data_adapter_oemof import calculations
from data_adapter_oemof.mappings import Field, Mapper

logger = logging.getLogger()


class FacadeAdapter:
    type: str = "adapter"
    facade: Facade = None
    extra_fields = (
        Field(name="name", type=str),
        Field(name="region", type=str),
        Field(name="year", type=int),
    )

    def __init__(self, struct: dict, mapper: Mapper):
        self.facade_dict = self.get_default_parameters(struct, mapper)

    def get_default_parameters(self, struct: dict, mapper: Mapper) -> dict:
        defaults = {"type": self.type}
        # Add mapped attributes
        mapped_values = mapper.get_default_mappings(struct)
        defaults.update(mapped_values)

        # add name if found in data, else use calculation for name:
        if (name := mapper.get("name")) is not None:
            defaults.update({"name": name})
        else:
            defaults.update(
                {
                    "name": calculations.get_name(
                        mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
                    )
                }
            )

        return defaults


class DispatchableAdapter(FacadeAdapter):
    """
    Dispatchable Adapter
    """

    type = "dispatchable"
    facade = facades.Dispatchable


class HeatPumpAdapter(FacadeAdapter):
    """
    HeatPump Adapter
    """

    type = "heat_pump"
    facade = facades.HeatPump


class LinkAdapter(FacadeAdapter):
    """
    Link Adapter
    """

    type = "link"
    facade = facades.Link


class ReservoirAdapter(FacadeAdapter):
    """
    Reservoir Adapter
    """

    type = "reservoir"
    facade = facades.Reservoir


class ExcessAdapter(FacadeAdapter):
    """
    Excess Adapter
    """

    type = "excess"
    facade = facades.Excess


class BackpressureTurbineAdapter(FacadeAdapter):
    """
    BackpressureTurbine Adapter
    """

    type = "backpressure_turbine"
    facade = facades.BackpressureTurbine


class CommodityAdapter(FacadeAdapter):
    """
    CommodityAdapter
    """

    type = "commodity"
    facade = facades.Commodity

    def get_default_parameters(self, struct: dict, mapper: Mapper) -> dict:
        defaults = super().get_default_parameters(struct, mapper)
        if mapper.get("carrier") == "carrier":
            defaults["carrier"] = mapper.get_busses(struct)["bus"]

        return defaults


class ConversionAdapter(FacadeAdapter):
    """
    ConversionAdapter
    To use Conversion, map the inputs and outputs within the structure to avoid deduction failure.
    """

    type = "conversion"
    facade = facades.Conversion


class LoadAdapter(FacadeAdapter):
    """
    LoadAdapter
    """

    type = "load"
    facade = facades.Load


class StorageAdapter(FacadeAdapter):
    """
    StorageAdapter
    """

    type = "storage"
    facade = facades.Storage
    extra_fields = FacadeAdapter.extra_fields + (
        Field(name="invest_relation_output_capacity", type=float),
        Field(name="inflow_conversion_factor", type=float),
        Field(name="outflow_conversion_factor", type=float),
    )


class ExtractionTurbineAdapter(FacadeAdapter):
    """
    ExtractionTurbineAdapter
    """

    type = "extraction_turbine"
    facade = facades.ExtractionTurbine


class VolatileAdapter(FacadeAdapter):
    """
    VolatileAdapter
    """

    type = "volatile"
    facade = facades.Volatile


# Create a dictionary of all adapter classes defined in this module
FACADE_ADAPTERS = {
    name: adapter for name, adapter in globals().items() if name.endswith("Adapter")
}
