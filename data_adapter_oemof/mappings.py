import logging

logger = logging.getLogger()


class Mapper:
    def __init__(self, data, mapping=None):
        if mapping is None:
            mapping = GLOBAL_PARAMETER_MAP
        self.data = data
        self.mapping = mapping

    def get(self, key):
        if key in self.mapping:
            mapped_key = self.mapping[key]
            logger.info(f"Mapped '{key}' to '{mapped_key}'")
        else:
            mapped_key = key
            logger.info(f"Key not found. Did not map '{key}'")

        if mapped_key not in self.data:
            logger.warning("Could not get key")
            return None

        return self.data[mapped_key]


TYPE_MAP = {
    "volatile": "",
}

CARRIER_MAP = {
    "electricity": "",
}

TECH_MAP = {
    "onshore": "",
}

# Maps from oemof.tabular parameter names
# to ontological terms or to sedos nomenclature as fallback option
GLOBAL_PARAMETER_MAP = {
    "capacity": "sedos-capacity",
    "marginal_cost": "sedos-marginal_cost",
    "overnight_cost": "sedos-overnight_cost",
    "fixed_cost": "sedos-fixed_cost",
    "lifetime": "sedos-lifetime",
    "wacc": "sedos-wacc",
}
