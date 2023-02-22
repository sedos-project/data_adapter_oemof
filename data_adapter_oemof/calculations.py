import warnings

from oemof.tools.economics import annuity


def calculation(func):
    """
    This is a decorator that allows calculations to fail
    """

    def decorated_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            warnings.warn(
                f"Calculation function '{func.__name__}' \n"
                f"called with {args, kwargs} \n"
                f"failed because of: \n" + str(e)
            )
            return e

    return decorated_func


@calculation
def get_name(region, carrier, tech):
    return f"{region}-{carrier}-{tech}"


@calculation
def get_capacity_cost(**kwargs):
    """
    Takes kwargs with either specified overnight cost, fixed cost, lifetime and wacc
     OR mapper that might contain those elements to calculate capacity cost
    :param kwargs:
    :return:
    """
    if {"capacity_cost", "fixed_cost", "lifetime", "wacc"}.issubset(kwargs.keys()):
        return annuity(capex=kwargs["overnight_cost"], n=kwargs["lifetime"], wacc=kwargs["wacc"]) + kwargs["fixed_cost"]
    elif {"mapper"}.issubset(kwargs.keys()):
        return annuity(kwargs["mapper"].get("capacity_cost"), kwargs["mapper"].get("lifetime"),
                       kwargs["mapper"].get("wacc"))+kwargs["mapper"].get("fixed_cost")
