from data_adapter_oemof import dataclasses


class VolatileAdapter(dataclasses.Volatile):
    @classmethod
    def parametrize(cls):
        instance = cls(
            name="calculations.get_name(region, carrier, tech)",
            type="type",
            carrier="carrier",
            tech="tech",
            capacity=8,  # get_param("capacity"),
            capacity_cost=8,  # get_capacity_cost("capacity_cost"),
            bus=8,  # get_param("bus"),
            marginal_cost=8,  # get_param("marginal_cost"),
            profile=8,  # None,
            output_parameters=8,  # None,
        )

        return instance


ADAPTER_MAP = {"volatile": VolatileAdapter}
