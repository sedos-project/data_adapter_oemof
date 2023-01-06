from typing import Sequence, Union
from dataclasses import dataclass, field


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


TYPE_MAP = {
    "commodity": Commodity,
    "conversion": Conversion,
    "load": Load,
    "storage": Storage,
    "volatile": Volatile,
}
