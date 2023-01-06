from oemof.tools.economics import annuity


def get_name(region, carrier, tech):
    return f"{region}-{carrier}-{tech}"


def get_capacity_cost(overnight_cost, fixed_cost, lifetime, wacc):
    return annuity(overnight_cost, lifetime, wacc) + fixed_cost
