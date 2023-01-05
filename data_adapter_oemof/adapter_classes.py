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
    output_parameters: dict

    @classmethod
    def parametrize(cls):
        instance = cls(
            name=get_name(region, carrier, tech),
            type=type,
            carrier=carrier,
            tech="tech",
            capacity=get_param("capacity"),
            capacity_cost=get_capacity_cost("capacity_cost"),
            bus=get_param("bus"),
            marginal_cost=get_param("marginal_cost"),
            profile=None,
            output_parameters=None,
        )

        return instance


ADAPTER_MAP = {"volatile": VolatileAdapter}
