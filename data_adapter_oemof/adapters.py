import logging

from oemof.tabular import facades
from oemof.tabular._facade import Facade

from data_adapter_oemof import calculations
from data_adapter_oemof.mappings import Field, Mapper

logger = logging.getLogger()


class Adapter:
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

    def get_default_parameters(self, struct: dict, mapper: Mapper) -> dict:
        defaults = super().get_default_parameters(struct, mapper)
        if mapper.get("carrier") == "carrier":
            defaults["carrier"] = mapper.get_busses(struct)["bus"]

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


class BevFleetAdapter(Adapter):
    """
    BevFleetAdapter
    """

    type = "Bev Fleet"
    facade = facades.BevFleet  # .Bev

    def get_default_parameters(self, struct: dict, mapper: Mapper) -> dict:
        defaults = super().get_default_parameters(struct, mapper)
        #defaults.update({"name": mapper.get("process_name", default="fake_name")})
        defaults.update({"name": mapper.process_name})
        return defaults


class TransportConversionAdapter(Adapter):
    """
    TransportConversionAdapter
    To use Conversion, map the inputs and outputs within the structure to avoid deduction failure.
    """

    type = "conversion"
    facade = facades.Conversion

    def get_default_parameters(self, struct: dict, mapper: Mapper) -> dict:
        defaults = super().get_default_parameters(struct, mapper)
        defaults.update({"name": mapper.process_name})
        return defaults


# Create a dictionary of all adapter classes defined in this module
FACADE_ADAPTERS = {
    name: adapter for name, adapter in globals().items() if name.endswith("Adapter")
}
