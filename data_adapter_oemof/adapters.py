class Adapter:
    def __init__(self, datacls, data=None):
        self.datacls = datacls
        self.data = data

    def parametrize_dataclass(self):
        instance = self.datacls(
            name="calculations.get_name(data.region, data.carrier, data.tech)",
            type="self.data.type",
            carrier="carrier",
            tech="",
            capacity=8,  # get_param("capacity"),
            capacity_cost=8,  # get_capacity_cost("capacity_cost"),
            bus=8,  # get_param("bus"),
            marginal_cost=8,  # get_param("marginal_cost"),
            profile=8,  # None,
            output_parameters=8,  # None,
        )

        return instance
