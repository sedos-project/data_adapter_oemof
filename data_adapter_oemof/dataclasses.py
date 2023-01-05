from typing import Sequence, Union
from dataclasses import dataclass


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


TYPE_MAP = {"volatile": Volatile}
