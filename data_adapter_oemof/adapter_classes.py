from typing import Sequence, Union
from dataclasses import dataclass


@dataclass
class VolatileAdapter:
    name: str
    type: str
    carrier: str
    tech: str
    capacity: float
    capacity_cost: float
    bus: str
    marginal_cost: float
    profile: Union[float, Sequence[float]]
    output_parameters : dict

    @classmethod
    def parametrize(cls):
        kwargs = dict(
            name = "get_name(region, carrier, tech)",
            type = "TYPE_MAP[volatile]",
            carrier = "carrier",
            tech = "tech",
            capacity = "get_capacity()",
            capacity_cost = "get_capacity_cost()",
            bus = None,
            marginal_cost = None,
            profile = None,
            output_parameters = None,
        )
        return cls(**kwargs)


ADAPTER_MAP = {
    "volatile": VolatileAdapter
}
