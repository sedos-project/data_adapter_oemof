from typing import Sequence, Union
from dataclasses import dataclass, field

from data_adapter_oemof.mappings import Mapper
from data_adapter_oemof import calculations


@dataclass
class Commodity:
    name: str
    type: str
    carrier: str
    tech: str
    bus: str
    amount: float
    marginal_cost: float = 0
    output_parameters: dict = field(default_factory=dict)

    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@dataclass
class Conversion:
    name: str
    type: str
    from_bus: str
    to_bus: str
    carrier: str
    tech: str
    capacity: float = None
    efficiency: float = 1
    marginal_cost: float = 0
    carrier_cost: float = 0
    capacity_cost: float = None
    expandable: bool = False
    capacity_potential: float = float("+inf")
    capacity_minimum: float = None
    input_parameters: dict = field(default_factory=dict)
    output_parameters: dict = field(default_factory=dict)

    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@dataclass
class Load:
    name: str
    type: str
    carrier: str
    tech: str
    bus: str
    amount: float
    profile: Union[float, Sequence[float]]
    marginal_utility: float = 0
    input_parameters: dict = field(default_factory=dict)

    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@dataclass
class Storage:
    name: str
    type: str
    carrier: str
    tech: str
    bus: str
    storage_capacity: float = 0
    capacity: float = 0
    capacity_cost: float = 0
    storage_capacity_cost: float = None
    storage_capacity_potential: float = float("+inf")
    capacity_potential: float = float("+inf")
    expandable: bool = False
    marginal_cost: float = 0
    efficiency: float = 1
    input_parameters: dict = field(default_factory=dict)
    output_parameters: dict = field(default_factory=dict)

    def parametrize_dataclass(self, data):
        instance = self.datacls()
        return instance


@dataclass
class Volatile:
    name: str
    type: str
    carrier: str
    tech: str
    capacity: float
    capacity_cost: float
    bus: str
    marginal_cost: float
    profile: Union[float, Sequence[float]]
    output_parameters: dict

    @classmethod
    def parametrize_dataclass(cls, data):
        mapper = Mapper(data)

        instance = cls(
            name=calculations.get_name(
                mapper.get("region"), mapper.get("carrier"), mapper.get("tech")
            ),
            type=mapper.get("type"),
            carrier=mapper.get("carrier"),
            tech=mapper.get("tech"),
            capacity=mapper.get("capacity"),
            capacity_cost=calculations.get_capacity_cost(
                mapper.get("overnight_cost"),
                mapper.get("fixed_cost"),
                mapper.get("lifetime"),
                mapper.get("wacc"),
            ),
            bus=mapper.get("bus"),  # get_param("bus"),
            marginal_cost=mapper.get("marginal_cost"),
            profile=8,  # None,
            output_parameters=8,  # None,
        )
        return instance


TYPE_MAP = {
    "commodity": Commodity,
    "conversion": Conversion,
    "load": Load,
    "storage": Storage,
    "volatile": Volatile,
}
